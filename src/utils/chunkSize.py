def getChunkSize(interval: int) -> int:
        if interval == 1:
            return 7 * 24 * 3600
        elif interval == 10:
            return 30 * 24 * 3600
        elif interval == 60:
            return 90 * 24 * 3600
        else:
            return 365 * 24 * 3600