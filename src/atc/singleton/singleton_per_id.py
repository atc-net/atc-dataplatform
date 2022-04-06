"""
A type of singleton metaclass where each id:str is initialized only once
"""


class SingletonPerId(type):
    _instances = {}

    def __call__(cls, id: str):
        if id not in cls._instances:
            cls._instances[id] = super(SingletonPerId, cls).__call__(id)
        return cls._instances[id]
