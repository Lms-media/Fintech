import threading
import time
from Interfaces import IExecutor, IAction, IMarket, IContextProvider, TaskStatus

class BackgroundPollingExecutor(IExecutor):
    _market: IMarket
    _contextProvider: IContextProvider

    def __init__(self, market: IMarket, contextProvider: IContextProvider):
        self._market = market
        self._contextProvider = contextProvider

    def start(self, action: IAction) -> None:
        thread = threading.Thread(target=self._polling, args=(action,))
        thread.start()

    def getMarket(self):
        return self._market

    def _polling(self, action: IAction) -> None:
        done = False
        while(not done):
            done = True
            context = self._contextProvider.getContext()
            action.update(context)

            for task in action.getTasks():
                if task.getStatus() == TaskStatus.Locked:
                    done = False

                if not task.getStatus() == TaskStatus.Executing:
                    continue

                self._market.execute(task)
                task.finish()

            time.sleep(0.01)
