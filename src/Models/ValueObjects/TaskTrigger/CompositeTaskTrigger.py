from src.Interfaces import ITaskTrigger

class CompositeTaskTrigger(ITaskTrigger):
    _dependencies: list[ITaskTrigger]

    def __init__(self, dependencies: list[ITaskTrigger]):
        self._dependencies = list(dependencies)

    def getDependencies(self) -> list[ITaskTrigger]:
        return self._dependencies

    def withDependency(self, dependency: ITaskTrigger) -> ITaskTrigger:
        newDependencies = list(self.getDependencies())
        newDependencies.append(dependency)

        return CompositeTaskTrigger(newDependencies)

    def withoutDependency(self, dependency: ITaskTrigger) -> ITaskTrigger:
        newDependencies = list(self.getDependencies())
        newDependencies.remove(dependency)

        return CompositeTaskTrigger(newDependencies)

    def isTriggered(self, context) -> bool:
        blocked = False

        for dep in self._dependencies:
            if not dep.isTriggered(context):
                blocked = True
                break

        return not blocked

    def __eq__(self, other) -> bool:
        if not isinstance(other, CompositeTaskTrigger):
            return False

        return self._dependencies == other.getDependencies()

    def __hash__(self) -> int:
        return hash(self._dependencies)

    def __copy__(self) -> ITaskTrigger:
        return CompositeTaskTrigger(self._dependencies)

    def __str__(self) -> str:
        result = [f"🚩 Depends on:"]

        for dep in self._dependencies:
            result.append(f"\t{dep}")

        return "\n".join(result)
