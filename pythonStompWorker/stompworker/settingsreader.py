#!/usr/bin/env python

import csv
import collections
from io import StringIO

class SettingsReader:
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
                        cols[row[0].strip()]=SettingsReader.ColSpec(row[1].strip(), row[2].strip() if(len(row)>2) else None);
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
                            cols.append(SettingsReader.ColSpecTransform(None, "@"+colName, None, None))
                        elif(len(row)>2):
                            if("%" in row[2]):
                                transform=colName+"=STR_TO_DATE("+"@"+colName+", '"+row[2]+"')"
                            else:
                                transform=colName+"='"+row[2]+"'"
                            cols.append(SettingsReader.ColSpecTransform(colName, "@"+colName, domainCols[colName].datatype, transform))
                        else:
                            cols.append(SettingsReader.ColSpecTransform(colName, colName, domainCols[colName].datatype, None))
                elif (line):
                    cmdArgs=line.split("=",1)
                    #print ("Parse variable:",cmdArgs)
                    loadDataCmdArgs[cmdArgs[0]]=cmdArgs[1]
        return (cols, loadDataCmdArgs)
        
class GenSql:
    def genPopulateSql(domainfieldsFile, fieldmapFile, dbname, tablename, csvFile):
        print("genPopulateSql:",domainfieldsFile, fieldmapFile, dbname, tablename, csvFile)
        (domainCols, _)=SettingsReader.readDomainFields(domainfieldsFile)
        print("Parsed", domainfieldsFile)
        (cols, loadDataCmdArgs)=SettingsReader.readFieldMap(fieldmapFile, domainCols)
        print("Parsed", fieldmapFile)

        cmds=["use {};".format(dbname), 
            "create table {}".format(tablename), 
            " (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY"
            ]
        for col in cols:
            cmds.append(" ,{} {}".format(col.createColName, col.datatype))
        cmds.append(");")

        cmds.append("")
        cmds.append("LOAD DATA LOCAL INFILE '"+csvFile+"' INTO TABLE "+tablename+" ")
        if('loadData.startingClause' in loadDataCmdArgs):
            cmds.append("  "+loadDataCmdArgs['loadData.startingClause'])
        cmds.append("  "+loadDataCmdArgs['loadData.fieldClause'])
        cmds.append("  "+loadDataCmdArgs['loadData.linesClause'])
        cmds.append("  "+loadDataCmdArgs['loadData.endingClause'])
        cmds.append("  ("+','.join(map(lambda c: c.colName, cols))+")")
        transforms=list(filter(None, map(lambda c: c.transform, cols)))
        if(len(transforms)>0):
            cmds.append("SET "+', '.join(transforms))
        cmds.append(";")
        ret='\n'.join(cmds)
        #print(ret)
        return ret
        
    def genSelectAll(tablename):
        ret="select * from "+tablename
        return ret;
