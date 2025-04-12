from datetime import timedelta

def getChunkSize(interval):
        if interval == 1:
            return timedelta(days=7)
        elif interval == 10:
            return timedelta(days=30)
        elif interval == 60:
            return timedelta(days=90)
        else:
            return timedelta(days=365)