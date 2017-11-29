#!/usr/bin/env python

def startStompListener(msgQueue, subscriptionInt,  msgListener,  
    hosts=[('localhost', 61613)], 
    msgLstnerName='WorkerCreateLoadDataSql'):
    import stomp
    conn = stomp.Connection11(host_and_ports=hosts)
    conn.set_listener(msgLstnerName, msgListener)
    conn.start()
    conn.connect(wait=True)
    print("Subscribing msgListener={} to queue={} id={}"
        .format( msgLstnerName, msgQueue,  subscriptionInt))
    # id should uniquely identify the subscription
    # ack = auto, client, or client-individual
    # See https://stomp.github.io/stomp-specification-1.1.html#SUBSCRIBE_ack_Header
    conn.subscribe(msgQueue, subscriptionInt)

    def signal_handler(signal, frame):
        print('Disconnecting')
        try:
            conn.unsubscribe(subscriptionInt)
        except Exception as e:
            print("When unsubscribing:",  e)
        conn.disconnect()

    import signal
    signal.signal(signal.SIGINT, signal_handler)
    print('Press Ctrl+C to stop listening')
    signal.pause()

#https://stackoverflow.com/questions/1305532/convert-python-dict-to-object
class HeaderStruct:
    def __init__(self, entries):
        self.__dict__.update(entries)
        
class AbstractListener:
    def _runFunction(self,  func,  headers, msg):
        print("Running function with msg={} headers={}".format(msg, headers))
        try:
            func(HeaderStruct(headers))
        except Exception as e:
            print("When running function:",  e)

class RunFunctionListener(AbstractListener):
    def __init__(self,  func):
        self.func=func
    def on_message(self, headers, msg):
        print("RunFunctionListener: got msg: {} with {}".format(msg, headers))
        AbstractListener._runFunction(self,  self.func,  headers, msg)

class CallMethodListener(AbstractListener):
    def __init__(self, classInstance,  defMethodName=None,  methodNameKey='methodName'):
        self.classInstance=classInstance
        self.methodNameKey=methodNameKey
        self.defMethodName=defMethodName
    def on_message(self, headers, msg):
        print("CallMethodListener: got msg: {} with {}".format(msg, headers))
        method_name = headers[self.methodNameKey] if self.methodNameKey in headers else self.defMethodName
        try:
            method = getattr(self.classInstance, method_name)
        except AttributeError:
            raise NotImplementedError("Class `{}` does not implement `{}`"
                .format(self.classInstance.__class__.__name__, method_name))
        AbstractListener._runFunction(self, method,  headers, msg)
            
class CallFunctionListener(AbstractListener):
    def __init__(self, locals,  defMethodName=None, methodNameKey='methodName'):
        self.defMethodName=defMethodName
        self.methodNameKey=methodNameKey
        self.possibles = globals().copy()
        self.possibles.update(locals)
    def on_message(self, headers, msg):
        print("CallFunctionListener: got msg: {} with {}".format(msg, headers))
        method_name = headers[self.methodNameKey] if self.methodNameKey in headers else self.defMethodName
        method = self.possibles.get(method_name)
        if not method:
            raise NotImplementedError("Method {} not implemented: {}"
                .format(method_name,  self.possibles))
        AbstractListener._runFunction(self, method,  headers, msg)
