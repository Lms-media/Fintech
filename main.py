from src.Factories import DummyFactory
from src.UseCases import SingleActionUseCase

factory = DummyFactory()
useCase = SingleActionUseCase(factory)

useCase.execute()
