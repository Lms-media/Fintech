"""Regression checks for information timing, label separation and OHLC fills."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from Contracts import EWMAClosePredictor, RidgeClosePredictor, OpenCloseStrategy, CorridorAssessor, HistoricalExecutor
from Contracts.DataSource.SeriesDataSource import SeriesDataSource
from Entities import CandleSeries, TrimmedCandleSeries
from Entities.CandleSeries.OpenCandleSeries import OpenCandleSeries, CandleOpen
from Entities.Action.CandleCorridorAction import CandleCorridorAction
from Interfaces import ActionStatus, IntervalType
from Services import RuntimePortfolio, PortfolioSyncMarket
from Contracts import DatasetContextProvider, LoggedPredictor
from UseCases import PredictorTrainingUseCase, PredictorTestingUseCase, StrategyTestingUseCase
from ValueObjects import Asset, AssetPair, Candle
from Factories import CausalBacktestFactory


class MemoryLogger:
    def log(self, message):
        pass


class CausalTradingTests(unittest.TestCase):
    def setUp(self):
        self.pair = AssetPair(Asset("RUB", 1), Asset("TEST", 10))
        self.interval = IntervalType.OneMinute
        self.logger = MemoryLogger()

    def series(self, count=30, current=None):
        series = CandleSeries(self.pair)
        for i in range(count):
            prices = current if current is not None and i == count - 1 else (100, 101, 102, 98)
            series.appendRight(Candle(self.pair, i * 60, self.interval, *prices, 100))
        return series

    def trained(self, series=None, split=20):
        series = series or self.series()
        predictor = RidgeClosePredictor(3)
        source = SeriesDataSource(TrimmedCandleSeries(series, 0, split))
        PredictorTrainingUseCase(source, predictor, self.logger).execute()
        return predictor

    def test_input_has_no_target_or_future_access(self):
        series = self.series()
        view = OpenCandleSeries.atIndex(series, 10, 3)
        self.assertIsNone(view.getByIndex(3))
        self.assertIsNone(view.getByTimestamp(600))
        self.assertEqual(view.getIndexOf(series.getByIndex(11)), -1)
        self.assertFalse(hasattr(view.getCurrentOpen(), "getClosePrice"))
        self.assertFalse(hasattr(view, "_base"))
        series.popRight()
        self.assertEqual(view.getCount(), 3)

    def test_prediction_independent_of_unknown_target_prices(self):
        first, second = self.series(), self.series(current=(100, 300, 400, 1))
        for predictor in [EWMAClosePredictor(3), self.trained(first)]:
            a = predictor.predict(OpenCandleSeries.atIndex(first, 29, 3)).getNextCandle()
            b = predictor.predict(OpenCandleSeries.atIndex(second, 29, 3)).getNextCandle()
            self.assertEqual(a, b)

    def test_future_mutation_does_not_change_decision(self):
        first, second = self.series(), self.series(current=(100, 300, 400, 1))
        predictor = self.trained(first)
        a = predictor.predict(OpenCandleSeries.atIndex(first, 25, 3)).getNextCandle()
        b = predictor.predict(OpenCandleSeries.atIndex(second, 25, 3)).getNextCandle()
        self.assertEqual(a, b)

    def test_known_open_anchors_forecast_and_timestamp(self):
        series = self.series(current=(200, 201, 202, 198))
        predictor = EWMAClosePredictor(3)
        prediction = predictor.predict(OpenCandleSeries.atIndex(series, 29, 3))
        candle = prediction.getNextCandle()
        self.assertEqual(candle.getOpenPrice(), 200)
        self.assertAlmostEqual(candle.getClosePrice(), 202)
        self.assertEqual(candle.getOpenTimestamp(), 29 * 60)

    def test_ridge_learns_training_return(self):
        series = self.series()
        predictor = self.trained(series)
        result = predictor.predict(OpenCandleSeries.atIndex(series, 20, 3)).getNextCandle()
        self.assertAlmostEqual(result.getClosePrice(), 101)
        self.assertAlmostEqual(result.getHighPrice(), 102)
        self.assertAlmostEqual(result.getLowPrice(), 98)

    def test_rejects_untrained_model_and_training_overlap(self):
        opening = OpenCandleSeries.atIndex(self.series(), 10, 3)
        with self.assertRaisesRegex(ValueError, "Train"):
            RidgeClosePredictor(3).predict(opening)
        with self.assertRaisesRegex(ValueError, "overlaps"):
            self.trained().predict(opening)

    def test_normalization_unchanged_during_test(self):
        predictor = self.trained()
        mean, scale = predictor._algo._mean.copy(), predictor._algo._scale.copy()
        PredictorTestingUseCase(SeriesDataSource(self.series()), predictor, self.logger, startIndex=20).execute()
        self.assertTrue((mean == predictor._algo._mean).all())
        self.assertTrue((scale == predictor._algo._scale).all())

    def test_requires_explicit_opening_input(self):
        with self.assertRaisesRegex(ValueError, "OpenCandleSeries"):
            EWMAClosePredictor(3).predict(TrimmedCandleSeries(self.series(), 0, 3))

    def test_rejects_unclosed_history(self):
        with self.assertRaisesRegex(ValueError, "not closed"):
            OpenCandleSeries(TrimmedCandleSeries(self.series(), 0, 3),
                             CandleOpen(self.pair, 120, self.interval, 100))

    def test_slice_cannot_read_excluded_candle_by_timestamp(self):
        series = self.series()
        trimmed = TrimmedCandleSeries(series, 2, 5)
        self.assertIsNone(trimmed.getByTimestamp(300))
        self.assertEqual(trimmed.getByTimestamp(299), series.getByIndex(4))
        self.assertEqual(trimmed.getIndexOf(series.getByIndex(5)), -1)
        self.assertEqual(TrimmedCandleSeries(series, 29, 30).getByTimestamp(1740), series.getByIndex(29))
        with self.assertRaises(ValueError):
            TrimmedCandleSeries(series, -1, 3)

    def settle(self, prices, first_buy, commission=0, slippage=0):
        series = self.series(count=4, current=prices)
        source = SeriesDataSource(series)
        context = DatasetContextProvider(self.pair, source)
        portfolio = RuntimePortfolio(self.pair.getBaseAsset())
        portfolio.deposit(10000)
        market = PortfolioSyncMarket(portfolio, context, commission, slippage)
        opening = OpenCandleSeries.atIndex(series, 3, 3)
        predicted = EWMAClosePredictor(3).predict(opening)
        signal = OpenCloseStrategy().getSignal(predicted)
        action = CandleCorridorAction(signal, self.pair, 1, first_buy, 95, 105)
        HistoricalExecutor(market).start(action)
        self.assertEqual(action.getStatus(), ActionStatus.Finished)
        self.assertEqual(portfolio.getAssetAmount(self.pair.getQuoteAsset()), 0)
        return portfolio.getBaseAmount(), market.getFills()

    def test_both_touched_is_stop_for_long_and_short(self):
        for buy in [True, False]:
            cash, fills = self.settle((100, 101, 110, 90), buy)
            self.assertEqual(cash, 9950)
            self.assertEqual(fills[-1]["reason"], "stop")

    def test_exact_touch_executes_for_both_directions(self):
        for prices, buy in [((100, 101, 105, 99), True), ((100, 99, 101, 95), False)]:
            cash, fills = self.settle(prices, buy)
            self.assertEqual(cash, 10050)
            self.assertEqual(fills[-1]["reason"], "take_profit")

    def test_exact_stop_touch_executes(self):
        for prices, buy in [((100, 99, 101, 95), True), ((100, 101, 105, 99), False)]:
            cash, fills = self.settle(prices, buy)
            self.assertEqual(cash, 9950)
            self.assertEqual(fills[-1]["reason"], "stop")

    def test_no_touch_closes_current_candle_including_last_bar(self):
        cash, fills = self.settle((100, 102, 103, 99), True)
        self.assertEqual(cash, 10020)
        self.assertEqual(fills[-1]["price"], 102)
        self.assertEqual(fills[-1]["timestamp"], 180)
        self.assertEqual(fills[-1]["reason"], "close")

    def test_short_no_touch_and_costs(self):
        cash, fills = self.settle((100, 98, 103, 97), False, 0.001, 0.001)
        entry, exit = 100 * 0.999, 98 * 1.001
        self.assertAlmostEqual(cash, 10000 + 10 * (entry - exit) - 10 * (entry + exit) * 0.001)

    def test_risk_sizing_uses_lot_size_and_current_open(self):
        portfolio = RuntimePortfolio(self.pair.getBaseAsset())
        portfolio.deposit(100000)
        series = self.series()
        forecast = EWMAClosePredictor(3).predict(OpenCandleSeries.atIndex(series, 29, 3))
        signal = OpenCloseStrategy().getSignal(forecast)
        action = CorridorAssessor(portfolio).getAction(signal)
        # ATR=4, risk <=1000, exposure <=40000, lot size=10.
        self.assertEqual(action.getTasks()[0].getLotCount(), 25)
        self.assertEqual(action.getTasks()[0].getTimestamp(), 29 * 60)
        self.assertEqual(action.getTasks()[1].getTimestamp(), 29 * 60)
        exposure_limited = CorridorAssessor(portfolio, maxExposure=0.01).getAction(signal)
        self.assertEqual(exposure_limited.getTasks()[0].getLotCount(), 1)

    def test_flat_prices_and_no_edge_skip_without_tasks(self):
        series = self.series(count=1, current=(100, 100, 100, 100))
        for i in range(1, 5):
            series.appendRight(Candle(self.pair, i * 60, self.interval, 100, 100, 100, 100, 0))
        forecast = EWMAClosePredictor(3).predict(OpenCandleSeries.atIndex(series, 4, 3))
        portfolio = RuntimePortfolio(self.pair.getBaseAsset())
        portfolio.deposit(100000)
        action = CorridorAssessor(portfolio).getAction(OpenCloseStrategy().getSignal(forecast))
        self.assertEqual(action.getTasks(), [])
        HistoricalExecutor(None).start(action)
        self.assertEqual(action.getStatus(), ActionStatus.Finished)
        falling = CandleSeries(self.pair)
        for i in range(5):
            falling.appendRight(Candle(self.pair, i * 60, self.interval, 100, 99, 102, 98, 0))
        forecast = EWMAClosePredictor(3).predict(OpenCandleSeries.atIndex(falling, 4, 3))
        signal = OpenCloseStrategy().getSignal(forecast)
        self.assertTrue(CorridorAssessor(portfolio).getAction(signal).getTasks())
        self.assertEqual(CorridorAssessor(portfolio, allowShort=False).getAction(signal).getTasks(), [])

    def test_testing_counts_exactly_and_handles_short_ranges(self):
        source, predictor = SeriesDataSource(self.series()), EWMAClosePredictor(3)
        metrics = PredictorTestingUseCase(source, predictor, self.logger, startIndex=29).execute()
        self.assertEqual(metrics["count"], 1)
        self.assertAlmostEqual(metrics["closeMAE"], 0)
        with self.assertRaises(ValueError):
            PredictorTestingUseCase(source, predictor, self.logger, startIndex=30).execute()
        for mode in ["algo", "ml"]:
            factory = CausalBacktestFactory(source, mode=mode, candlesCount=3)
            predictor = factory.getPredictor()
            if mode == "ml":
                training = SeriesDataSource(TrimmedCandleSeries(source.getSeries(), 0, 20))
                PredictorTrainingUseCase(training, predictor, self.logger).execute()
            metrics = PredictorTestingUseCase(source, predictor, self.logger, startIndex=20).execute()
            use_case = StrategyTestingUseCase(
                source, predictor, factory.getStrategy(), factory.getAssessor(),
                self.logger, factory.getExecutor(), 10, startIndex=20,
            )
            self.assertEqual(use_case.execute()["count"], 10)
            self.assertEqual(len(factory.getMarket().getFills()), 20)
            self.assertEqual(metrics["count"], 10)

    def test_iteration_limit_and_logged_predictor_timing(self):
        factory = CausalBacktestFactory(SeriesDataSource(self.series()), candlesCount=3)
        predictor = LoggedPredictor(factory.getPredictor(), self.logger)
        use_case = StrategyTestingUseCase(factory.getDataSource(), predictor, factory.getStrategy(),
                                         factory.getAssessor(), self.logger, factory.getExecutor(), 2, startIndex=20)
        self.assertEqual(use_case.execute()["count"], 2)
        self.assertEqual(len(factory.getMarket().getFills()), 4)

if __name__ == "__main__":
    unittest.main()
