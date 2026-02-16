from keras.models import Sequential, Model
from keras.layers import Dense, Dropout, Input
from keras.optimizers import Adam
import numpy as np
from typing import Tuple, Optional

# =============================================================================
# KERAS vs TENSORFLOW — В ЧЁМ РАЗНИЦА?
# =============================================================================
#
# ИСТОРИЯ:
# - Keras (2015) — изначально отдельная библиотека, высокоуровневый API для нейросетей.
#   Работал поверх разных "бэкендов": TensorFlow, Theano, CNTK.
#
# - TensorFlow 2.0 (2019) — Google интегрировал Keras как tf.keras.
#   Теперь tf.keras — это РЕКОМЕНДУЕМЫЙ способ работы с TensorFlow.
#   Устанавливая tensorflow, вы автоматически получаете tf.keras.
#
# - Keras 3.0 (2023) — снова стал отдельным пакетом, поддерживает
#   TensorFlow, PyTorch и JAX как бэкенды.
#
# ВЫВОД: Для проекта с tensorflow в requirements.txt используйте tf.keras —
# ничего дополнительно устанавливать не нужно!
#
# =============================================================================


# =============================================================================
# ПРОСТОЙ МНОГОСЛОЙНЫЙ ПЕРСЕПТРОН (MLP) ДЛЯ ПРЕДСКАЗАНИЯ ЦЕН
# =============================================================================

class SimpleMLP:
    """
    Простой многослойный персептрон для предсказания следующей цены свечи.

    Персептрон (MLP - Multi-Layer Perceptron) — это базовая архитектура
    нейронной сети, состоящая из полносвязных слоёв (Dense layers).
    """

    def __init__(self, input_size: int = 10, output_size: int = 1):
        """
        Параметры:
        ----------
        input_size : int
            Количество входных признаков (например, последние N цен закрытия)
        output_size : int
            Количество выходов (1 для предсказания одной цены)
        """
        self.input_size = input_size
        self.output_size = output_size
        self.model = self._build_model()

    def _build_model(self) -> Model:
        """
        Создаёт и возвращает модель персептрона.
        """

        # =====================================================================
        # SEQUENTIAL — контейнер для последовательного стека слоёв
        # =====================================================================
        # Модель Sequential позволяет строить сеть как "пирог" — слой за слоем.
        # Данные проходят через слои последовательно: вход → слой1 → слой2 → выход

        model = Sequential([

            # =================================================================
            # INPUT LAYER (Входной слой)
            # =================================================================
            # Определяет форму входных данных.
            # shape=(self.input_size,) означает одномерный вектор из input_size элементов.
            # Например, если input_size=10, то на вход подаётся вектор [x1, x2, ..., x10]

            Input(shape=(self.input_size,)),

            # =================================================================
            # HIDDEN LAYER 1 (Первый скрытый слой)
            # =================================================================
            # Dense(64) — полносвязный слой с 64 нейронами.
            # "Полносвязный" означает, что каждый нейрон этого слоя связан
            # со ВСЕМИ нейронами предыдущего слоя.
            #
            # activation='relu' — функция активации ReLU (Rectified Linear Unit)
            # ReLU(x) = max(0, x) — обнуляет отрицательные значения.
            # Зачем: добавляет нелинейность, позволяя сети обучаться сложным паттернам.
            # Без активации сеть была бы просто линейной комбинацией (бесполезно).

            Dense(64, activation='relu', name='hidden_layer_1'),

            # =================================================================
            # DROPOUT (Слой регуляризации)
            # =================================================================
            # Dropout(0.2) — случайно "выключает" 20% нейронов во время обучения.
            # Зачем: предотвращает переобучение (overfitting).
            # Переобучение — когда сеть запоминает обучающие данные вместо
            # того, чтобы находить общие закономерности.

            Dropout(0.2, name='dropout_1'),

            # =================================================================
            # HIDDEN LAYER 2 (Второй скрытый слой)
            # =================================================================
            # Ещё один полносвязный слой с 32 нейронами.
            # Обычно размер слоёв уменьшается к выходу (64 → 32 → 1),
            # это называется "сужающаяся" архитектура — сеть постепенно
            # "сжимает" информацию до нужного выхода.

            Dense(32, activation='relu', name='hidden_layer_2'),

            # =================================================================
            # OUTPUT LAYER (Выходной слой)
            # =================================================================
            # Dense(output_size) — выходной слой с количеством нейронов = количеству выходов.
            # Для предсказания одной цены output_size=1.
            #
            # activation='linear' (по умолчанию) — без активации.
            # Для задачи регрессии (предсказание числа) активация не нужна,
            # так как мы хотим получить любое число, а не ограниченный диапазон.

            Dense(self.output_size, name='output_layer')
        ])

        # =====================================================================
        # COMPILE — настройка процесса обучения
        # =====================================================================

        model.compile(
            # -----------------------------------------------------------------
            # OPTIMIZER (Оптимизатор)
            # -----------------------------------------------------------------
            # Adam — адаптивный оптимизатор, комбинирует лучшее от:
            # - Momentum (учитывает "инерцию" градиента)
            # - RMSprop (адаптирует learning rate для каждого параметра)
            #
            # learning_rate=0.001 — скорость обучения (шаг градиентного спуска).
            # Слишком большой → сеть "прыгает" и не сходится.
            # Слишком маленький → обучение очень медленное.
            # 0.001 — хорошее значение по умолчанию.

            optimizer=Adam(learning_rate=0.001),

            # -----------------------------------------------------------------
            # LOSS (Функция потерь)
            # -----------------------------------------------------------------
            # MSE (Mean Squared Error) — среднеквадратичная ошибка.
            # Формула: (1/n) * Σ(y_pred - y_true)²
            # Используется для задач регрессии (предсказание чисел).
            # Сеть минимизирует эту функцию во время обучения.

            loss='mse',

            # -----------------------------------------------------------------
            # METRICS (Метрики для мониторинга)
            # -----------------------------------------------------------------
            # MAE (Mean Absolute Error) — средняя абсолютная ошибка.
            # Формула: (1/n) * Σ|y_pred - y_true|
            # Более интерпретируемая метрика: показывает среднее отклонение
            # предсказания от реального значения в тех же единицах (например, рублях).

            metrics=['mae']
        )

        return model

    def summary(self):
        """Выводит структуру модели."""
        self.model.summary()

    def train(self, X_train, y_train, epochs: int = 100, batch_size: int = 32):
        """
        Обучает модель.

        Параметры:
        ----------
        X_train : array-like
            Обучающие данные (входы), shape: (n_samples, input_size)
        y_train : array-like
            Целевые значения (выходы), shape: (n_samples, output_size)
        epochs : int
            Количество эпох — сколько раз сеть "увидит" все данные.
            Больше эпох → лучше обучение, но риск переобучения.
        batch_size : int
            Размер батча — сколько примеров обрабатывается за один шаг.
            Меньше батч → более "шумное" обучение, но меньше памяти.
        """

        # validation_split=0.2 — 20% данных используется для валидации.
        # Валидация показывает, как модель работает на данных, которые не видела.
        # Если loss падает, а val_loss растёт — это переобучение.

        history = self.model.fit(
            X_train,
            y_train,
            epochs=epochs,
            batch_size=batch_size,
            validation_split=0.2,
            verbose='auto'  # Показывать прогресс обучения
        )
        return history

    def predict(self, X):
        """
        Делает предсказание.

        Параметры:
        ----------
        X : array-like
            Входные данные, shape: (n_samples, input_size)

        Возвращает:
        -----------
        Предсказанные значения, shape: (n_samples, output_size)
        """
        return self.model.predict(X)


# =============================================================================
# НОРМАЛИЗАЦИЯ ДАННЫХ — ПОЧЕМУ ЭТО ВАЖНО?
# =============================================================================
#
# ПРОБЛЕМА:
# Цены акций/валют имеют значения вроде 85.50, 150.25, 1200.00 и т.д.
# Нейросети работают лучше, когда данные находятся в небольшом диапазоне
# (обычно [0, 1] или [-1, 1]).
#
# ПОЧЕМУ:
# 1. Градиенты становятся более стабильными
# 2. Все признаки имеют одинаковый "вес" при обучении
# 3. Функции активации (ReLU, sigmoid) работают оптимально в этих диапазонах
#
# МЕТОДЫ НОРМАЛИЗАЦИИ:
#
# 1. MIN-MAX SCALING (масштабирование в [0, 1]):
#    x_norm = (x - min) / (max - min)
#    + Простой и понятный
#    - Чувствителен к выбросам
#
# 2. STANDARDIZATION (Z-score, стандартизация):
#    x_norm = (x - mean) / std
#    + Устойчив к выбросам
#    + Данные центрированы вокруг 0
#    - Не ограничен диапазоном
#
# 3. ПРОЦЕНТНОЕ ИЗМЕНЕНИЕ (для временных рядов цен) — РЕКОМЕНДУЕТСЯ:
#    x_norm = (x[t] - x[t-1]) / x[t-1] * 100
#    + Убирает тренд
#    + Данные уже в небольшом диапазоне (обычно -5% до +5%)
#    + Модель учится предсказывать ИЗМЕНЕНИЕ, а не абсолютную цену
#
# =============================================================================


class CandleNormalizer:
    """
    Нормализатор данных свечей для нейросети.

    Поддерживает разные методы нормализации и автоматически
    сохраняет параметры для обратного преобразования.
    """

    def __init__(self, method: str = 'minmax'):
        """
        Параметры:
        ----------
        method : str
            Метод нормализации:
            - 'minmax': масштабирование в [0, 1]
            - 'zscore': стандартизация (среднее=0, std=1)
            - 'pct_change': процентное изменение
        """
        self.method = method
        self._params: dict = {}
        self._is_fitted = False

    def fit(self, data: np.ndarray) -> 'CandleNormalizer':
        """
        Вычисляет параметры нормализации на обучающих данных.

        ВАЖНО: fit() вызывается ТОЛЬКО на обучающих данных!
        Тестовые данные нормализуются с теми же параметрами.

        Параметры:
        ----------
        data : np.ndarray
            Данные для вычисления параметров, shape: (n_samples, n_features)
        """
        if self.method == 'minmax':
            # Сохраняем min и max для каждого признака
            self._params['min'] = np.min(data, axis=0)
            self._params['max'] = np.max(data, axis=0)
            # Защита от деления на 0 (если min == max)
            self._params['range'] = self._params['max'] - self._params['min']
            self._params['range'][self._params['range'] == 0] = 1

        elif self.method == 'zscore':
            # Сохраняем среднее и стандартное отклонение
            self._params['mean'] = np.mean(data, axis=0)
            self._params['std'] = np.std(data, axis=0)
            # Защита от деления на 0
            self._params['std'][self._params['std'] == 0] = 1

        elif self.method == 'pct_change':
            # Для процентного изменения параметры не нужны
            pass

        self._is_fitted = True
        return self

    def transform(self, data: np.ndarray) -> np.ndarray:
        """
        Применяет нормализацию к данным.

        Параметры:
        ----------
        data : np.ndarray
            Данные для нормализации

        Возвращает:
        -----------
        Нормализованные данные
        """
        if not self._is_fitted and self.method != 'pct_change':
            raise ValueError("Сначала вызовите fit() на обучающих данных!")

        if self.method == 'minmax':
            # x_norm = (x - min) / (max - min)
            return (data - self._params['min']) / self._params['range']

        elif self.method == 'zscore':
            # x_norm = (x - mean) / std
            return (data - self._params['mean']) / self._params['std']

        elif self.method == 'pct_change':
            # Процентное изменение относительно предыдущего значения
            # Первый элемент будет 0 (нет предыдущего)
            pct = np.zeros_like(data)
            pct[1:] = (data[1:] - data[:-1]) / data[:-1] * 100
            return pct

        raise ValueError(f"Неизвестный метод нормализации: {self.method}")

    def fit_transform(self, data: np.ndarray) -> np.ndarray:
        """Вычисляет параметры и сразу применяет нормализацию."""
        return self.fit(data).transform(data)

    def inverse_transform(self, data_norm: np.ndarray) -> np.ndarray:
        """
        Обратное преобразование — из нормализованных данных в исходные.

        ВАЖНО: Нужно для интерпретации предсказаний модели!
        Модель выдаёт нормализованные значения, их нужно преобразовать
        обратно в реальные цены.

        Параметры:
        ----------
        data_norm : np.ndarray
            Нормализованные данные

        Возвращает:
        -----------
        Данные в исходном масштабе
        """
        if self.method == 'minmax':
            # x = x_norm * (max - min) + min
            return data_norm * self._params['range'] + self._params['min']

        elif self.method == 'zscore':
            # x = x_norm * std + mean
            return data_norm * self._params['std'] + self._params['mean']

        elif self.method == 'pct_change':
            raise ValueError(
                "Обратное преобразование для pct_change требует базовую цену. "
                "Используйте inverse_pct_change(pct, base_price)"
            )

        raise ValueError(f"Неизвестный метод нормализации: {self.method}")

    def inverse_pct_change(self, pct: float, base_price: float) -> float:
        """
        Обратное преобразование для процентного изменения.

        Параметры:
        ----------
        pct : float
            Предсказанное процентное изменение
        base_price : float
            Базовая цена (последняя известная цена)

        Возвращает:
        -----------
        Предсказанная цена
        """
        return base_price * (1 + pct / 100)


# =============================================================================
# MLP ДЛЯ ПРЕДСКАЗАНИЯ СВЕЧЕЙ С НОРМАЛИЗАЦИЕЙ
# =============================================================================

class CandlePredictorMLP:
    """
    Персептрон для предсказания следующей свечи (OHLC).

    Вход: 15 последних свечей × 4 параметра = 60 признаков
    Выход: 1 свеча × 4 параметра = 4 значения (open, high, low, close)

    Включает встроенную нормализацию данных.
    """

    # Константы для индексов OHLC
    OPEN = 0
    HIGH = 1
    LOW = 2
    CLOSE = 3

    def __init__(
        self,
        n_candles: int = 15,
        normalization: str = 'minmax',
        hidden_layers: Tuple[int, ...] = (128, 64, 32)
    ):
        """
        Параметры:
        ----------
        n_candles : int
            Количество входных свечей (по умолчанию 15)
        normalization : str
            Метод нормализации: 'minmax', 'zscore', 'pct_change'
        hidden_layers : tuple
            Размеры скрытых слоёв (по умолчанию 128 → 64 → 32)
        """
        self.n_candles = n_candles
        self.n_features = 4  # OHLC
        self.input_size = n_candles * self.n_features  # 15 * 4 = 60
        self.output_size = self.n_features  # 4 (предсказываем одну свечу)
        self.hidden_layers = hidden_layers

        # Нормализаторы для входа и выхода
        self.input_normalizer = CandleNormalizer(method=normalization)
        self.output_normalizer = CandleNormalizer(method=normalization)

        self.model = self._build_model()

    def _build_model(self) -> Model:
        """Создаёт модель персептрона."""

        layers_list = [Input(shape=(self.input_size,))]

        # Добавляем скрытые слои
        for i, units in enumerate(self.hidden_layers):
            layers_list.append(
                Dense(
                    units,
                    activation='relu',
                    name=f'hidden_{i+1}'
                )
            )
            layers_list.append(
                Dropout(0.2, name=f'dropout_{i+1}')
            )

        # Выходной слой: 4 нейрона для OHLC
        layers_list.append(
            Dense(self.output_size, name='output')
        )

        model = Sequential(layers_list)

        model.compile(
            optimizer=Adam(learning_rate=0.001),
            loss='mse',
            metrics=['mae']
        )

        return model

    def prepare_data(
        self,
        candles: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Подготавливает данные для обучения.

        Параметры:
        ----------
        candles : np.ndarray
            Массив свечей, shape: (n_total_candles, 4)
            Каждая строка: [open, high, low, close]

        Возвращает:
        -----------
        X : np.ndarray
            Входные данные, shape: (n_samples, 60)
        y : np.ndarray
            Целевые значения, shape: (n_samples, 4)
        """
        n_samples = len(candles) - self.n_candles

        X = np.zeros((n_samples, self.input_size))
        y = np.zeros((n_samples, self.output_size))

        for i in range(n_samples):
            # Берём n_candles свечей и "разворачиваем" в вектор
            X[i] = candles[i:i + self.n_candles].flatten()
            # Целевая свеча — следующая после окна
            y[i] = candles[i + self.n_candles]

        return X, y

    def fit(
        self,
        candles: np.ndarray,
        epochs: int = 100,
        batch_size: int = 32
    ):
        """
        Обучает модель на данных свечей.

        Параметры:
        ----------
        candles : np.ndarray
            Массив свечей, shape: (n_total_candles, 4)
        epochs : int
            Количество эпох обучения
        batch_size : int
            Размер батча
        """
        # Подготавливаем данные
        X, y = self.prepare_data(candles)

        # Нормализуем
        # ВАЖНО: fit() только на обучающих данных!
        X_norm = self.input_normalizer.fit_transform(X)
        y_norm = self.output_normalizer.fit_transform(y)

        # Обучаем
        history = self.model.fit(
            X_norm,
            y_norm,
            epochs=epochs,
            batch_size=batch_size,
            validation_split=0.2,
            verbose='auto'
        )

        return history

    def predict(self, last_candles: np.ndarray) -> np.ndarray:
        """
        Предсказывает следующую свечу.

        Параметры:
        ----------
        last_candles : np.ndarray
            Последние n_candles свечей, shape: (n_candles, 4)

        Возвращает:
        -----------
        np.ndarray
            Предсказанная свеча [open, high, low, close]
        """
        # Разворачиваем в вектор
        X = last_candles.flatten().reshape(1, -1)

        # Нормализуем (используем параметры, вычисленные при обучении)
        X_norm = self.input_normalizer.transform(X)

        # Предсказываем
        y_norm = self.model.predict(X_norm, verbose='0')

        # Обратное преобразование в реальные цены
        y = self.output_normalizer.inverse_transform(y_norm)

        return y[0]  # Возвращаем [open, high, low, close]

    def summary(self):
        """Выводит структуру модели."""
        self.model.summary()


# =============================================================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ
# =============================================================================

if __name__ == "__main__":

    print("=" * 60)
    print("ПРИМЕР 1: Простой MLP")
    print("=" * 60)

    # Создаём модель: 10 входов, 1 выход
    mlp = SimpleMLP(input_size=10, output_size=1)
    mlp.summary()

    # Тестовые данные
    X_train = np.random.randn(1000, 10)
    y_train = np.random.randn(1000, 1)

    # Обучаем
    mlp.train(X_train, y_train, epochs=5, batch_size=32)

    print("\n" + "=" * 60)
    print("ПРИМЕР 2: Нормализация данных")
    print("=" * 60)

    # Симулируем цены акций (например, от 80 до 120)
    prices = np.random.uniform(80, 120, size=(100, 4))  # 100 свечей OHLC

    print(f"\nИсходные данные (первые 3 свечи):")
    print(prices[:3])

    # MinMax нормализация
    normalizer = CandleNormalizer(method='minmax')
    prices_norm = normalizer.fit_transform(prices)

    print(f"\nПосле MinMax нормализации:")
    print(prices_norm[:3])
    print(f"Диапазон: [{prices_norm.min():.2f}, {prices_norm.max():.2f}]")

    # Обратное преобразование
    prices_restored = normalizer.inverse_transform(prices_norm)
    print(f"\nПосле обратного преобразования:")
    print(prices_restored[:3])

    print("\n" + "=" * 60)
    print("ПРИМЕР 3: CandlePredictorMLP")
    print("=" * 60)

    # Генерируем синтетические данные свечей
    # В реальности это были бы исторические данные
    n_candles = 500
    base_price = 100.0

    candles = np.zeros((n_candles, 4))
    for i in range(n_candles):
        # Симулируем случайное блуждание
        change = np.random.randn() * 0.5
        open_price = base_price + change
        close_price = open_price + np.random.randn() * 0.3
        high_price = max(open_price, close_price) + abs(np.random.randn() * 0.2)
        low_price = min(open_price, close_price) - abs(np.random.randn() * 0.2)

        candles[i] = [open_price, high_price, low_price, close_price]
        base_price = close_price

    print(f"\nСгенерировано {n_candles} свечей")
    print(f"Диапазон цен: [{candles.min():.2f}, {candles.max():.2f}]")

    # Создаём и обучаем модель
    predictor = CandlePredictorMLP(
        n_candles=15,           # Используем 15 последних свечей
        normalization='minmax', # MinMax нормализация
        hidden_layers=(128, 64, 32)
    )

    predictor.summary()

    print("\nОбучение модели...")
    predictor.fit(candles, epochs=10, batch_size=32)

    # Предсказываем следующую свечу
    last_15_candles = candles[-15:]
    predicted_candle = predictor.predict(last_15_candles)

    print(f"\nПоследняя известная свеча:")
    print(f"  Open:  {candles[-1, 0]:.2f}")
    print(f"  High:  {candles[-1, 1]:.2f}")
    print(f"  Low:   {candles[-1, 2]:.2f}")
    print(f"  Close: {candles[-1, 3]:.2f}")

    print(f"\nПредсказанная следующая свеча:")
    print(f"  Open:  {predicted_candle[0]:.2f}")
    print(f"  High:  {predicted_candle[1]:.2f}")
    print(f"  Low:   {predicted_candle[2]:.2f}")
    print(f"  Close: {predicted_candle[3]:.2f}")
