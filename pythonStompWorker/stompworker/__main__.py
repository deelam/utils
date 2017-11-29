#!/usr/bin/env python

import sys
import csv
import collections
from io import StringIO

ColSpec = collections.namedtuple('ColSpec', ['datatype', 'params'])
def readDomainFields(domainfieldsFile):
    cols={}
    settings={}
    columnsFlag=False
    with open(domainfieldsFile) as f:
        for line in f:
            line=line.strip()
            if(line == 'COLUMNS'):
                columnsFlag=True;
            elif (columnsFlag):
                for row in csv.reader(StringIO(line), delimiter=","):
                    cols[row[0].strip()]=ColSpec(row[1].strip(), row[2].strip() if(len(row)>2) else None);
            elif (line):
                cmdArgs=line.split("=",1)
                #print ("Parse variable:",cmdArgs)
                settings[cmdArgs[0]]=cmdArgs[1]
                print ("Parse variable:",line.split("=",1))

    #for col,colspec in cols.items():
    #    print("col:", col, colspec)
    return (cols, settings)

ColSpecTransform = collections.namedtuple('ColSpecTransform', ['createColName','colName','datatype', 'transform'])
def readFieldMap(fieldmapFile, domainCols):
    cols=[]
    loadDataCmdArgs={}
    fieldsFlag=False
    with open(fieldmapFile) as f:
        for line in f:
            line=line.strip()
            if(line == 'FIELDS'):
                fieldsFlag=True;
            elif (fieldsFlag):
                for row in csv.reader(StringIO(line), delimiter=",", skipinitialspace=True):
                    colName=row[1].strip()
                    #print ("row: ", row, domainCols[colName].datatype)
                    if(colName == 'IGNORE'):
                        cols.append(ColSpecTransform(None, "@"+colName, None, None))
                    elif(len(row)>2):
                        if("%" in row[2]):
                            transform=colName+"=STR_TO_DATE("+"@"+colName+", '"+row[2]+"')"
                        else:
                            transform=colName+"='"+row[2]+"'"
                        cols.append(ColSpecTransform(colName, "@"+colName, domainCols[colName].datatype, transform))
                    else:
                        cols.append(ColSpecTransform(colName, colName, domainCols[colName].datatype, None))
            elif (line):
                cmdArgs=line.split("=",1)
                #print ("Parse variable:",cmdArgs)
                loadDataCmdArgs[cmdArgs[0]]=cmdArgs[1]
    return (cols, loadDataCmdArgs)

def printSqlCommands(domainfieldsFile, fieldmapFile, dbname, tablename, csvFile):
    print("printSqlCommands:",domainfieldsFile, fieldmapFile, dbname, tablename, csvFile)
    (domainCols, _)=readDomainFields(domainfieldsFile)
    print("Parsed", domainfieldsFile)
    (cols, loadDataCmdArgs)=readFieldMap(fieldmapFile, domainCols)
    print("Parsed", fieldmapFile)

    print("")
    print("use "+dbname+";")

    print("")
    print("create table '"+tablename+"'")
    print("(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY", end='')
    for col in cols:
        print (",", col.colName, col.datatype, end='')
    print(");")

    print("")
    print("LOAD DATA LOCAL INFILE '"+csvFile+"' INTO TABLE '"+tablename+"'")
    if('loadData.startingClause' in loadDataCmdArgs):
        print("  ", loadDataCmdArgs['loadData.startingClause'])
    print("  ", loadDataCmdArgs['loadData.fieldClause'])
    print("  ", loadDataCmdArgs['loadData.linesClause'])
    print("  ", loadDataCmdArgs['loadData.endingClause'])
    print("  (", ','.join(map(lambda c: c.colName, cols)),")")
    transforms=list(filter(None, map(lambda c: c.transform, cols)))
    if(len(transforms)>0):
        print("SET",', '.join(transforms), end='')
    print(";")


class MsgListener:
    def meth1(self, s):
        printSqlCommands(s.domainfieldsFile, s.fieldmapFile, s.dbname, s.tablename,s.csvFile)

def meth1(s):
    printSqlCommands(s.domainfieldsFile, s.fieldmapFile, s.dbname, s.tablename,s.csvFile)

def main():
    domainfieldsFile=sys.argv[1]
    fieldmapFile=sys.argv[2]
    dbname="thegeekstuff"
    tablename="a16"
    #csvfile="/tmp/data/LE_datasets/Austin_PD_Annual_Crime_Dataset_2015.csv"
    #csvfile="/tmp/data/INTEL_datasets/I-94_sample_data.csv"
    csvfile="/tmp/data/INTEL_datasets/TIDE_sample_data.csv"
    printSqlCommands(domainfieldsFile, fieldmapFile, dbname, tablename, csvfile)

#import os
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#print(sys.path)

import stompworker.stomplistener as sl
if __name__ == "__main__":
    if(len(sys.argv)>1):
        main()
    else:
        sl.startStompListener('test', 123,  sl.RunFunctionListener(MsgListener().meth1))
        #sl.startStompListener('test', 123,  sl.RunFunctionListener(meth1))
        #sl.startStompListener('test', 123,  sl.CallFunctionListener(locals(),  defMethodName="meth1"))
        #sl.startStompListener('test', 123,  sl.CallMethodListener(MsgListener()))

