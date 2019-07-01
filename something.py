import operator as op
from serializer import CloudPickleSerializer, PickleSerializer
from io import BytesIO as StringIO
import pickle
from pickle import _Pickler as Pickle
from multiprocessing import Process, Pipe


class Something(object):
    def __init__(self, value, serializer=None):
        self._val = value

    def add(self, to_add): 
        # def func(a, b):
        #     return op.add(a, b)
        # return PipelinedSomething(self, func)
        return PipelinedSomething(self, SomethingAdder(to_add).func)
    
    def sub(self, to_sub):
        return PipelinedSomething(self, SomethingSuber(to_sub).func)
    
    def collect(self):
        return self._val


class SomethingAdder(object):
    def __init__(self, to_add):
        self.to_add = to_add
    
    def func(self, initial):
        return op.add(initial, self.to_add)


class SomethingSuber(object):
    def __init__(self, to_sub):
        self.to_sub = to_sub
    
    def func(self, initial):
        return op.sub(initial, self.to_sub)


def _pickle_command(command):
    ser = CloudPickleSerializer()
    pickled_command = ser.dumps(command)
    return pickled_command

# seems deserializer, serializer is for initial data
# def _wrap_function(func, deserializer, serializer):
#     assert deserializer, "deserializer should not be empty"
#     assert serializer, "serializer should not be empty"
#     command = (func, deserializer, serializer)
#     pickled_command = _pickle_command(command)
#     return bytearray(pickled_command)


def read_command(serializer, file):
    command = serializer.loads(file)
    return command


def worker(conn, pickled_command, initial):
    # deserialization
    f = read_command(PickleSerializer(), pickled_command)
    res = f(initial)
    conn.send(res)
    conn.close()


class PipelinedSomething(Something):
    def __init__(self, prev, func):
        if not isinstance(prev, PipelinedSomething):
            self.func = func
            self._prev_val = prev._val
        else:
            prev_func = prev.func
            def pipeline_func(initial):
                return func(prev_func(initial))
            self.func = pipeline_func
            self._prev_val = prev._prev_val  # maintain the initial value
        self.prev = prev
        self._ret_val = None

    @property
    def _val(self):
        if self._ret_val:
            return self._ret_val
        pickled_command = _pickle_command(self.func)
        p_conn, c_conn = Pipe()
        p = Process(target=worker, args=(c_conn, pickled_command, self._prev_val,))
        p.start()
        self._ret_val = p_conn.recv()
        p.join()
        return self._ret_val
        

if __name__ == "__main__":
    something = Something(10)
    something_1 = something.add(1)
    something_2 = something_1.sub(5)
    res = something_2.collect()
    print(res)