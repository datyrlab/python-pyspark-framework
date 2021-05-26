#!/usr/bin/python

# tag::import[]

import json, os, re, sys
from typing import Any, Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
#from pyspark import SparkContext

# end::import[]


class Sparkclass:     
    """ handles files and spark tasks """

    # tag::init[]
    def __init__(self, config:dict):
        self.config = config
        self.debug_dir = "/tmp/spark"

    # end::init[]


    # tag::sparkStart[]
    def sparkStart(self, kwargs:dict) -> SparkSession:
        #print(kwargs)
        """ spark session from configuraton """
        
        try:
            MASTER = kwargs['spark_conf']['master']
            APP_NAME = kwargs['spark_conf']['app_name']
            LOG_LEVEL = kwargs['log']['level']

            def createSession(master:Optional[str]="local[*]", app_name:Optional[str]="myapp") -> SparkSession:
                """ create a spark session """
                spark = SparkSession\
                    .builder\
                    .appName(app_name)\
                    .master(master)\
                    .getOrCreate()
                return spark

            def setLogging(spark:SparkSession, log_level:Optional[str]=None) -> None:
                """ set log level - ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN 
                    this function will overide the configuration also set in log4j.properties
                        for example, log4j.rootCategory=ERROR, console
                """
                spark.sparkContext.setLogLevel(log_level) if log_level else None 

            def getSettings(spark:SparkSession) -> None:
                """ print spark settings to terminal"""
                #print(f"\033[1;33m{spark}\033[0m")
                #print(f"\033[96m{spark.sparkContext.getConf().getAll()}\033[0m")
                pass

            spark = createSession(MASTER, APP_NAME)
            setLogging(spark, LOG_LEVEL)
            getSettings(spark)
            return spark

        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e) 

    # end::sparkStart[]


    # tag::openJson[]
    def openJson(self, filepath:str) -> dict:
        """ open a json file and return a dict """
        if isinstance(filepath, str) and os.path.exists(filepath):
            with open(filepath, "r") as f:
                data = json.load(f)
            return data
        return openJson(filepath)
    
    # end::openJson[]


    # tag::importData[]
    def importData(self, spark:SparkSession, datapath:str, pattern=None) -> list:
        """ will return a list of files inside directory or single file """

        try:
            def fileOrDirectory(spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> str:
                """ check if path is a directory or file """
                if isinstance(datapath, str) and os.path.exists(datapath):
                    if os.path.isdir(datapath):
                        return openDirectory(spark, datapath, pattern)
                    elif os.path.isfile(datapath):
                        return openFile(spark, datapath)

            def openDirectory(spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> DataFrame:
                """ if datapath is a directory
                    add more logic to catch files not csv or json
                    multiple files vary, multiline, different fields, etc
                """
                if isinstance(datapath, str) and os.path.exists(datapath):
                    filelist = Sparkclass(self.config).listDirectory(datapath, pattern)  
                    filetype = getUniqueFileExtentions(filelist)
                    if filetype == None: 
                        raise ValueError('Cannot create a single dataframe from varying file types or no files found') 
                    return Sparkclass(self.config).createDataFrame(spark, filelist, filetype)

            def openFile(spark:SparkSession, datapath:str) -> DataFrame:
                """ if datapath is a file 
                    add more logic to catch files not csv or json
                """
                if isinstance(datapath, str) and os.path.exists(datapath):
                    filelist = [datapath]
                    filetype = Sparkclass(self.config).getFileExtension(datapath)
                    return Sparkclass(self.config).createDataFrame(spark, filelist, filetype)
            
            def getUniqueFileExtentions(filelist:list) -> list:
                """ required if no search pattern is given and could return any file type """
                if isinstance(filelist, list) and len(filelist) > 0:
                    exts = set(os.path.splitext(f)[1] for f in filelist)
                    filetype = list(exts)
                    return filetype[0][1:] if len(filetype) == 1 else None

            return fileOrDirectory(spark, datapath, pattern)
    
        except Exception as e:
            #print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e) 
            raise

    # end::importData[]

    
    # tag::getFileExtension[]
    def getFileExtension(self, filepath:str) -> str:
        """ get extension from a single file """
        if isinstance(filepath, str):
            filename, file_extension = os.path.splitext(filepath)
            return file_extension[1:] if file_extension else None
    
    # end::getFileExtension[]
    
    
    # tag::listDirectory[]
    def listDirectory(self, directory:str, pattern:Optional[str]=None) -> list:
        """ recursively list the files of a directory  """
        def recursiveFilelist(directory):
            if os.path.exists(directory): 
                filelist = []
                for dirpath, dirnames, filenames in os.walk(directory):
                    for filename in filenames:
                        filelist.append(os.path.join(dirpath, filename))
                return filelist
        
        def filterFiles(filelist:list, pattern:str) -> list:
            """ if pattern is included then filter files """
            if isinstance(pattern, str):
                return [x for x in filelist if re.search(rf"{pattern}", x)]
            else:
                return filelist

        filelist = recursiveFilelist(directory)
        return filterFiles(filelist, pattern)

    # end::listDirectory[]

    
    # tag::createDataFrame[]
    def createDataFrame(self, spark:SparkSession, filelist:list, filetype:str) -> DataFrame:
        """ create dataframe from list of files 
            assuming filetypes are json or csv
            add more functions for other filetypes for example, plain text files to create an RDD
            factor in text files without an extension
        """
        def dFfromCSV(filelist:list) -> DataFrame:
            if isinstance(filelist, list) and len(filelist) > 0:
                df = spark.read.format("csv") \
                    .option("header", "true")  \
                    .option("mode", "DROPMALFORMED") \
                    .load(filelist)
                return df
        
        def dFfromJSON(filelist:list) -> DataFrame:
            if isinstance(filelist, list) and len(filelist) > 0:
                df = spark.read.format("json") \
                    .option("mode", "PERMISSIVE") \
                    .option("primitivesAsString", "true") \
                    .load(filelist)
                return df
        
        def makeDF(filelist, filetype):
            return dFfromCSV(filelist) if filetype == "csv" else dFfromJSON(filelist) if filetype == "json" else None
        
        return makeDF(filelist, filetype)
            
    # end::createDataFrame[]


    # tag::createFile[]
    def createFile(self, spark, df:DataFrame, filepath:str) -> str:
        """ writes a dataframe to a file  """ 
        def writeFile(df:DataFrame, filepath:str, filetype:str) -> None:
            if isinstance(df, DataFrame):
                getattr(df.coalesce(1).write,filetype)(filepath, mode="overwrite", header="true") # csv, 1 file, with header

        def getFileType(filepath:str) -> str:
            """ gets the file extension for getattr filetype """
            return Sparkclass(self.config).getFileExtension(filepath)

        filetype = getFileType(filepath)
        writeFile(df, filepath, filetype) if isinstance(filetype, str) else None

    # end::createFile[]
    

    # tag::createTempTables[]
    def createTempTables(self, tupleDf:tuple) -> None:
        if isinstance(tupleDf, tuple) and len(tupleDf) == 2:
            tupleDf[0].createOrReplaceTempView(tupleDf[1])
            
    # end::createTempTables[]

    
    # tag::debugCreateFile[]
    def debugCreateFile(self, paths:tuple, content:dict) -> None:
        """ creates a json file with info from a dictonary 
            modify self.debug_dir for directory path, default /tmp/spark
        """

        def makeDirectory(directory:str) -> None:
            if isinstance(directory, str) and not os.path.exists(directory):
                os.makedirs(directory)
        
        def removeFile(filepath:str) -> None:
            if os.path.exists(filepath):
                os.remove(filepath)
        
        def createFile(filepath:str, content:Any) -> None:
            with open(filepath, 'a') as out: 
                out.write(content)
            out.close()
        
        directory = paths[0]
        filepath = paths[1]

        makeDirectory(directory)
        removeFile(filepath)
        createFile(filepath, content)

    # end::debugCreateFile[]


    # tag::debugDf[]
    def debugDf(self, df:DataFrame, filename:str) -> None:
        
        def dfToString(df:DataFrame) -> str:
            return df._jdf.schema().treeString()
        
        def createFilepath(directory:str, filename:str) -> str:
            d = f"{directory}/dataframes"
            return (d, f"{d}/{filename}.json")

        def createContent(df:DataFrame) -> dict:
            content = {}
            content['count'] = df.count() 
            content['schema'] = json.loads(df.schema.json())
            return json.dumps(content, sort_keys=False, indent=4, default=str)
        
        paths = createFilepath(self.debug_dir, filename)
        Sparkclass(self.config).debugCreateFile(paths, createContent(df)) 
        
    # end::debugDf[]


    # tag::debugTables[]
    def debugTables(self, table) -> None:
        
        def createFilepath(directory:str, filename:str) -> str:
            d = f"{directory}/tables"
            return (d, f"{d}/{filename}.json")
        
        def createContent(table) -> dict:
            content = {}
            content['table'] = table._asdict()
            content['dir.table'] = dir(table)
            return json.dumps(content, sort_keys=False, indent=4, default=str)
        
        paths = createFilepath(self.debug_dir, table.name)
        Sparkclass(self.config).debugCreateFile(paths, createContent(table)) 
    
    # end::debugTables[]


    
