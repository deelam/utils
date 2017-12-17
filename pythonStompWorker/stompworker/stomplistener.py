#!/usr/bin/env python

import stomp
import threading

class StompListener:
    counter=0
    def __init__(self, hosts=[('localhost', 61613)]):
        self.jobDoneLock = threading.Condition()
        self.listenerDoneLock = threading.Condition()
        self.conn = stomp.Connection11(host_and_ports=hosts)
        self.conn.start()
        self.conn.connect(wait=True)
        self.subscriptionIds=[]
        
    #calling this within message handler will hang since disconnect waits for receipt
    def shutdown(self, signal=None, frame=None):
        print('Shutting down')
        try:
            print('Unsubscribing ',  self.subscriptionIds)
            for id in self.subscriptionIds:
                self.conn.unsubscribe(id)
        except Exception as e:
            print("When unsubscribing:", e)
        print('Disconnecting')
        self.conn.disconnect()
        print('Disconnected')
        
    def onJobCompletion(self):
        with self.jobDoneLock:
            self.jobDoneLock.notifyAll()
    
    def handle(self, msgQueue, msgListener, subscriptionInt=None, 
        msgLstnerName=None):
        if subscriptionInt is None:
            StompListener.counter+=1
            subscriptionInt=StompListener.counter
        if msgLstnerName is None:
            msgLstnerName='pythonProcess'+str(subscriptionInt)
        self.subscriptionIds.append(subscriptionInt)
        self.conn.set_listener(msgLstnerName, msgListener)
        print("Subscribing msgListener={} to queue={} id={}\n"
            .format( msgLstnerName, msgQueue, subscriptionInt))
        import sys
        sys.stdout.flush()
        # id should uniquely identify the subscription
        # ack = auto, client, or client-individual
        # See https://stomp.github.io/stomp-specification-1.1.html#SUBSCRIBE_ack_Header
        self.conn.subscribe(msgQueue, subscriptionInt, ack='auto')

#https://stackoverflow.com/questions/1305532/convert-python-dict-to-object
class HeaderStruct:
    def __init__(self, entries):
        self.__dict__.update(entries)
        
class AbstractListener:
    def __init__(self, stompListener):
        self.stompListener=stompListener
        self.conn=stompListener.conn
    def on_receipt(self,  msg,  third):
        print('listener receipt',  msg,  third)
    def on_disconnected(self):
        print('listener disconnected')
    def _runFunction(self, func, headers, msg):
        print("Running function with msg={} headers={}".format(msg, headers))
        self.percent=10
        self._status(headers, 'Starting on job')
        try:
            func(msg,  HeaderStruct(headers))
            self.percent=20
#            self._failed(headers['reply-to'], 'Failed job')
            self._done(headers, 'Done with job')
        except Exception as e:
            import traceback
            traceback.print_tb(e.__traceback__)
            print("When running function:", e)
            self._failed(headers, 'Failed job')
        except:
            import sys
            print ("Unexpected error:", sys.exc_info()[0])
            raise
    def _status(self, request, msg):
        print("Sending: job status for",  request['id'],  "with",  self.percent)
        headers={'id': request['id'], 'percent': self.percent, 'message': msg}
        if 'reply-to' in request:
            self.conn.send(body='', destination=request['reply-to'], headers=headers)
    def _done(self, request, msg):
        print("Sending: Done job",  request['id'])
        headers={'id': request['id'], 'percent': 100, 'message': msg}
        if 'reply-to' in request:
            self.conn.send(body='', destination=request['reply-to'], headers=headers)
        self.stompListener.onJobCompletion()
    def _failed(self, request, msg):
        print("Sending: Failed job",  request['id'])
        headers={'id': request['id'], 'percent': -self.percent, 'message': msg}
        if 'reply-to' in request:
            self.conn.send(body='', destination=request['reply-to'], headers=headers)
        self.stompListener.onJobCompletion()

class RunFunctionListener(AbstractListener):
    def __init__(self, stompListener, func):
        super(RunFunctionListener, self).__init__(stompListener)
        self.func=func
    def on_message(self, headers, msg):
        print("RunFunctionListener: got msg: {}".format(msg))
        self._runFunction(self.func, headers, msg)

class CallMethodListener(AbstractListener):
    def __init__(self, stompListener, classInstance, defMethodName=None, methodNameKey='methodName'):
        super(CallMethodListener, self).__init__(stompListener)
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
        AbstractListener._runFunction(self, method, headers, msg)
            
class CallFunctionListener(AbstractListener):
    def __init__(self, stompListener, locals, defMethodName=None, methodNameKey='methodName'):
        super(CallFunctionListener, self).__init__(stompListener)
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
                .format(method_name, self.possibles))
        AbstractListener._runFunction(self, method, headers, msg)
