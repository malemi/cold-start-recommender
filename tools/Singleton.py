# Singleton

class  Singleton (object):
    instances = {}
    def __new__(cls, *args, **kwargs):
        if Singleton.instances.get(cls) is None:
            cls.__original_init__ = cls.__init__
            Singleton.instances[cls] = object.__new__(cls, *args, **kwargs)
        elif cls.__init__ == cls.__original_init__:
            def nothing(*args, **kwargs):
                pass
            cls.__init__ = nothing
        return Singleton.instances[cls]