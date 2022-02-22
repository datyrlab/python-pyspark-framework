#!/usr/bin/python

# tag::import[]

import json, os, re, sys, time
from typing import Any, Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

# end::import[]


class Sparkclass:     
    """ handles files and spark tasks """

    def __init__(self, config:dict):
        self.config = config
        self.debug_dir = "/tmp/spark"
        self.config_paths = (f"{self.debug_dir}/config", f"{self.debug_dir}/config/sparkSession.json")

    # tag::sparkStart[]
    def sparkStart(self, kwargs:dict) -> SparkSession:
        """ spark session from dict configuraton """
        
        try:
            def createBuilder(master:str, appname:str, config:dict) -> SparkSession.Builder:
                """ create a spark session """
                builder = SparkSession\
                    .builder\
                    .appName(appname)\
                    .master(master)
                return configDeltalake(builder, config)
            
            def configDeltalake(builder:SparkSession.Builder, config:dict) -> SparkSession.Builder:
                """ add delta lake to your session """
                if isinstance(builder, SparkSession.Builder) and config.get('deltalake') == True:
                    from delta import configure_spark_with_delta_pip
                    builder \
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    return configure_spark_with_delta_pip(builder)
                return builder
            
            def createSession(builder:SparkSession.Builder) -> SparkSession:
                if isinstance(builder, SparkSession.Builder):
                    return builder.getOrCreate()           

            def setLogging(spark:SparkSession, log_level:str) -> None:
                """ set log level - ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN 
                    this function will overide the configuration also set in log4j.properties
                        for example, log4j.rootCategory=ERROR, console
                """
                if isinstance(spark, SparkSession):
                    spark.sparkContext.setLogLevel(log_level) if isinstance(log_level, str) else None 

            def getSettings(spark:SparkSession, config_paths:tuple) -> None:
                """ write a json file with the session configuration  """
                if isinstance(spark, SparkSession):
                    c = {}
                    c['spark.version'] = spark.version
                    c['spark.sparkContext'] = spark.sparkContext.getConf().getAll()
                    content = json.dumps(c, sort_keys=False, indent=4, default=str)
                    Sparkclass(self.config).debugCreateFile((config_paths[0], config_paths[1]), content) 
                    
            
            MASTER = kwargs.get('spark_conf', {}).get('master', 'local[*]')
            APPNAME = kwargs.get('spark_conf', {}).get('appname', 'myapp')
            CONFIG = kwargs.get('config')
            LOG_LEVEL = kwargs.get('log', {}).get('level')
            
            builder = createBuilder(MASTER, APPNAME, CONFIG)
            spark = createSession(builder)
            setLogging(spark, LOG_LEVEL)
            getSettings(spark, self.config_paths)
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
    
    # end::openJson[]


    # tag::importData[]
    def importData(self, spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> list:
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
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e) 

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
            return filelist

        filelist = recursiveFilelist(directory)
        return filterFiles(filelist, pattern)

    # end::listDirectory[]

    
    # tag::createDataFrame[]
    def createDataFrame(self, spark:SparkSession, filelist:list, filetype:str) -> DataFrame:
        """ create dataframe from list of files 
            add more functions for other filetypes for example, plain text files to create an RDD
            factor in text files without an extension
        """
        def dfFromCSV(spark:SparkSession, filelist:list, filetype:str) -> DataFrame:
            if filetype == "csv":
                df = spark.read.format("csv") \
                    .option("header", "true")  \
                    .option("mode", "DROPMALFORMED") \
                    .load(filelist)
                return df
        
        def dfFromJSON(spark:SparkSession, filelist:list, filetype:str) -> DataFrame:
            if filetype == "json" or filetype == "har":
                df = spark.read.format("json") \
                    .option("mode", "PERMISSIVE") \
                    .load(filelist)
                return df
        
        def loopFunctions(spark:SparkSession, filelist:list, filetype:str) -> list:
            """ loop through each function - add more functions when needed """
            if isinstance(spark, SparkSession) and isinstance(filelist, list) and len(filelist) > 0:
                functionlist = [dfFromCSV, dfFromJSON]
                result = list(filter(None, [f(spark, filelist, filetype) for f in functionlist] ))
                return result[0] if len(result) > 0 else None
        
        return loopFunctions(spark, filelist, filetype)
            
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
        """ create temp tables for sql operations """
        if isinstance(tupleDf, tuple) and len(tupleDf) == 2 and isinstance(tupleDf[0], DataFrame):
            tupleDf[0].createOrReplaceTempView(tupleDf[1])

    # end::createTempTables[]
    
    
    # tag::loadTables[]
    def loadTables(self, spark:SparkSession, path:str, fmt:str) -> DataFrame:
        """ load parquet or delta tables  """
        if os.path.exists(path):
            df = spark.read.format(fmt) \
                .option("mergeSchema", "true") \
                .load(path)
            return df
        
    # end::loadTables[]


    # tag::exportDf[]
    def exportDf(self, tupleDf:tuple) -> None:
        """ export dataframe to a file format"""
        def openSession(spark:SparkSession) -> dict:
            """ returns a list of what's available in your spark session """
            return spark.sparkContext.getConf().getAll()

        def matchPattern(item:str, pattern:str) -> str:
            """ returns item from a list if matches with a pattern """
            match = re.search(pattern, item)
            return match[0] if match else None

        def loopSession(sessionlist:list, pattern:str) -> list:
            """ loop through content of the debug sparkSession.json file to find matching dependancy"""
            if isinstance(sessionlist, list):
                result = list(filter(any, [[(lambda x: matchPattern(x, pattern))(x) for x in linelist] for linelist in sessionlist]))
                return result[0] if len(result) > 0 else None
        
        def validateDependency(sessionlist:list, settings:dict) -> bool:
            """ returns true or false if dependencies are met """
            formatfound = loopSession(sessionlist, settings.get('format'))
            if settings.get('format') == "delta" and formatfound is None:
                return False
            return True
        
        def writeExport(tupleDf:tuple) -> None:
            """ exports dataframe to a chosen file format """
            if isinstance(tupleDf, tuple) and len(tupleDf) == 3:
                spark = tupleDf[0]
                df = tupleDf[1]
                settings = tupleDf[2]
                
                if validateDependency(openSession(spark), settings) == True and isinstance(df, DataFrame):
                    if settings.get('format') == "delta":
                        Sparkclass(self.config).exportDelta(spark, df, settings)                 
                    else:
                        df.write.format(settings.get('format')).mode(settings.get('mode')).save(settings.get('path'))
                    
        writeExport(tupleDf)

    # end::exportDf[]
    

    # tag::exportDelta[]
    def exportDelta(self, spark:SparkSession, df:DataFrame, settings:dict) -> None:
        """ export dataframe to a delta lake table """
        from delta import DeltaTable
        
        def debugSession(spark:SparkSession):
            c = {}
            c['spark.version'] = spark.version
            c['spark.sparkContext'] = spark.sparkContext.getConf().getAll()
            content = json.dumps(c, sort_keys=False, indent=4, default=str)
            Sparkclass(self.config).debugCreateFile((self.config_paths[0], f"{self.config_paths[0]}/exportDelta.json"), content) 

        def tableHistory(spark:SparkSession, df:DataFrame, settings:dict) -> None:
            """ return information on table version """
            if DeltaTable.isDeltaTable(spark, settings.get('path')) == True:
                dt = DeltaTable.forPath(spark, settings.get('path'))
                dt.history().show()

        def tableVacuum(spark:SparkSession, df:DataFrame, settings:dict) -> None:
            """ remove previous table versions """
            if DeltaTable.isDeltaTable(spark, settings.get('path')) == True:
                dt = DeltaTable.forPath(spark, settings.get('path'))
                dt.vacuum(168) 
        
        def tableNew(spark:SparkSession, df:DataFrame, settings:dict) -> None:
            """ create a new table """
            #print(f"\033[1;33mtableNew - {settings.get('path')}\033[0m") # debug terminal
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(settings.get('path'))
        
        def tableMerge(spark:SparkSession, df:DataFrame, settings:dict) -> None:
            """ merges data with existing table - avoid duplicates rows """
            #print(f"\033[1;33mtableMerge - {settings.get('path')}\033[0m") # debug terminal
            if settings.get('key') == None:
                raise ValueError('Provide a key in your settings dict to merge tables')  

            if DeltaTable.isDeltaTable(spark, settings.get('path')) == True:
                spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
                spark.sql("SET spark.databricks.delta.resolveMergeUpdateStructsByName.enabled = false")
                debugSession(spark)

                dt = DeltaTable.forPath(spark, settings.get('path'))
                dt \
                    .alias("t") \
                    .merge( \
                        df.alias("s"), \
                        f"t.{settings.get('key')} = s.{settings.get('key')}" \
                    ) \
                    .whenNotMatchedInsertAll() \
                    .execute()
        
        def tableAppend(spark:SparkSession, df:DataFrame, settings:dict) -> None:
            """ appends data to the table, will add duplicate rows """
            print(f"\033[1;33mtableAppend - {settings.get('path')}\033[0m") # debug terminal
            df.write.format("delta") \
                .option("mergeSchema","true") \
                .mode("append") \
                .save(settings.get('path'))

        def tableExist(spark:SparkSession, df:DataFrame, settings:dict) -> None:
            """ check if a path exists ...if a table should be newly created or data merged """
            if DeltaTable.isDeltaTable(spark, settings.get('path')) == False:
                tableNew(spark, df, settings)
            else:
                tableMerge(spark, df, settings)
            
        tableExist(spark, df, settings)
        tableHistory(spark, df, settings)
        #tableVacuum(spark, df, settings)

    # end::exportDelta[]
    

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
        """ create a json file with dataframe information """
        def dfToString(df:DataFrame) -> str:
            return df._jdf.schema().treeString()
        
        def createFilepath(directory:str, filename:str) -> str:
            timestamp = int(time.time() * 1000.0)
            d = f"{directory}/dataframes"
            return (d, f"{d}/{filename}-{timestamp}.json")

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
        """ create a json file with (sql temp) table information """
        def createFilepath(directory:str, filename:str) -> str:
            timestamp = int(time.time() * 1000.0)
            d = f"{directory}/tables"
            return (d, f"{d}/{filename}-{timestamp}.json")
        
        def createContent(table) -> dict:
            content = {}
            content['table'] = table._asdict()
            content['dir.table'] = dir(table)
            return json.dumps(content, sort_keys=False, indent=4, default=str)
        
        paths = createFilepath(self.debug_dir, table.name)
        Sparkclass(self.config).debugCreateFile(paths, createContent(table)) 
    
    # end::debugTables[]


    
