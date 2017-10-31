import stomp
import time
 
class SampleListener(object):
  def on_message(self, headers, msg):
    print(msg)
 
conn = stomp.Connection10()
 
conn.set_listener('SampleListener', SampleListener())
 
conn.start()
 
conn.connect()
 
conn.subscribe('test')
 
time.sleep(10000) # secs
 
conn.disconnect()

