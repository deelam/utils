#!/usr/bin/env python

from stompworker.settingsreader import GenSql

class MsgListener:
    def meth1(self, msg,  s):
        if s.command=='QUERY':
            self.query(msg, s)
        elif s.command=='SELECT':
            self.select(msg, s)
        elif s.command=='END':
            with st.listenerDoneLock:
                st.listenerDoneLock.notifyAll()
    def query(self, msg,  s):
        config = {
          'domainfieldsFile': s.domainfieldsFile,
          'fieldmapFile': s.fieldmapFile,
          'dbname': s.dbname,
          'tablename': s.tablename,
          'csvFile': s.csvFile
        }
        GenSql.genPopulateSql(**config)

    def select(self, msg, s):
        # use https://www.sqlalchemy.org/ instead?
        import MySQLdb
        config = {
          'user': s.user, #'root',
          'password': s.pw, #'my-secret-pw',
          'host': s.host, #'127.0.0.1',
          'database': s.db  #'thegeekstuff'
        }
        try:
            cnx = MySQLdb.connect(**config)
            cursor = cnx.cursor()

            query = (GenSql.genSelectAll(s.tablename))
            print("select=",  query)
            cursor.execute(query)
            #https://stackoverflow.com/questions/9942594/unicodeencodeerror-ascii-codec-cant-encode-character-u-xa0-in-position-20/9942822
            #for r in cursor:
              #print("row: {}".format(str(r).encode('utf-8')))
        finally:
            cursor.close()
            cnx.close()
            
def meth1(msg,  s):
    GenSql.genPopulateSql(**s)

import sys
def main():
    domainfieldsFile=sys.argv[1]
    fieldmapFile=sys.argv[2]
    dbname="thegeekstuff"
    tablename="a17"
    #csvfile="/tmp/data/LE_datasets/Austin_PD_Annual_Crime_Dataset_2015.csv"
    #csvfile="/tmp/data/INTEL_datasets/I-94_sample_data.csv"
    csvfile="/tmp/data/INTEL_datasets/TIDE_sample_data.csv"
    GenSql.genPopulateSql(domainfieldsFile, fieldmapFile, dbname, tablename, csvfile)

    
#import os
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#print(sys.path)

import stompworker.stomplistener as sl
if __name__ == "__main__":
    if(len(sys.argv)>2):
        main()
    else:
        st=sl.StompListener()
        inboxQueue=sys.argv[1]
        st.handle(inboxQueue, sl.RunFunctionListener(st, MsgListener().meth1))
        #sl.startStompListener('test', 123,  sl.RunFunctionListener(meth1))
        #sl.startStompListener('test', 123,  sl.CallFunctionListener(locals(),  defMethodName="meth1"))
        #sl.startStompListener('test', 123,  sl.CallMethodListener(MsgListener()))
        
#        import signal
#        signal.signal(signal.SIGINT, st.shutdown)

        with st.listenerDoneLock:
            st.listenerDoneLock.wait()
#        import time
#        time.sleep(10)
        st.shutdown()
        print('Exiting')


