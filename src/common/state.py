import time


class State(object):
    def __init__(self, value, expired_time=10 * 60):
        self.__timestamp = time.time()
        self.__value = value
        self.__expired_time = expired_time
        self.__expired = False
        self.__worked_out = False

    def __str__(self):
        return "State {" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.__timestamp)) + \
               ': ' + self.__value.__str__() + "}"

    def __repr__(self):
        return self.__str__()

    def update(self, value, update_function=None):
        if update_function:
            self.__value = update_function(value, self.__value)
        else:
            self.__timestamp = time.time()
            self.__value += value
        return self

    def values(self):
        return self.__value

    def is_expired(self):
        if time.time() - self.__timestamp > self.__expired_time or self.__expired:
            self.__worked_out = True
            return True
        else:
            return False

    def is_worked_out(self):
        return self.__worked_out

    def expire(self):
        self.__expired = True
