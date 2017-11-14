from applications.ingest_og_process_time.state import State


class RecordingState(object):
    @staticmethod
    def update(oldValue, newValues=None):
        if newValues is None:
            newValues = []

        newValues = newValues + oldValue

        started = [item for item in newValues if item[2] == 'started']
        finished = [item for item in newValues if item[2] == 'finished']

        started.sort(key=lambda i: i[0])
        finished.sort(key=lambda i: i[0], reverse=True)

        started_value = [started[0]] if started else []
        finished_value = [finished[0]] if finished else []

        return started_value + finished_value


    @staticmethod
    def isValidState(state):
        return state is not None and len(state.values()) == 2

    @staticmethod
    def updateStateIfNotExpired(value, state):
        if not state:
            return State(RecordingState.update(value), expired_time=3*3600)
        elif state.is_expired():
            return None
        elif len(state.values()) == 2:
            return None
        else:
            return state.update(value, RecordingState.update)
