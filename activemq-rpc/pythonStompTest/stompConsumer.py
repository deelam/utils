#!/usr/bin/env python

import stomp
import time

# https://stackoverflow.com/questions/7936572/python-call-a-function-from-string-name#7936588
def meth1():
    print("meth1")
def meth2():
    print("meth2")

class MyClass(object):
    def meth1(self):
          print("MyClass.meth1")
my_cls = MyClass()
 
class SampleListener(object):
    def on_message(self, headers, msg):
        print(msg, headers)
        #print("\t",headers['param1'],headers['param2'])
        if('methodName' in headers):
            try:
                method_name = headers['methodName'] # 'install' # set by the command line options
                method = getattr(my_cls, method_name)
            except AttributeError:
                raise NotImplementedError("Class `{}` does not implement `{}`".format(my_cls.__class__.__name__, method_name))
        else:
            function_name = headers['functionName'] # 'install' # set by the command line options
            possibles = globals().copy()
            possibles.update(locals())
            method = possibles.get(function_name)
        if not method:
              raise NotImplementedError("Method %s not implemented" % method_name)
        method()
 
#conn = stomp.Connection10()
conn = stomp.Connection11()
 
conn.set_listener('SampleListener', SampleListener())
 
conn.start()
 
conn.connect()
 
conn.subscribe('test',123)
 
time.sleep(10000) # secs
 
conn.disconnect()


