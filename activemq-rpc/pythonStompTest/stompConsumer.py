#!/usr/bin/python3

import stomp
import time
 
class SampleListener(object):
  def on_message(self, headers, msg):
    print(msg, headers)
    print("\t",headers['param1'],headers['param2'])
 
#conn = stomp.Connection10()
conn = stomp.Connection11()
 
conn.set_listener('SampleListener', SampleListener())
 
conn.start()
 
conn.connect()
 
conn.subscribe('test',123)
 
time.sleep(10000) # secs
 
conn.disconnect()

