#!/usr/bin/python3

import logging
import json, os, re, sys
from typing import Callable, Optional

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, explode

sys.path.insert(1, project_dir)
from classes import class_pyspark

def main():
    """ purpose of this application is to,
        1. import raw data (csv, json, etc) into a spark dataframe
        2. transform the dataframe
        3. export the dataframe result (could be any file format but here we choose to save to a delta table)
    """

    # start session from json config file
    conf = openConfig(f"{project_dir}/json/sales.json")
    spark = sparkStart(conf)
    
    # 1. import the raw data into a spark dataframe
    # 3rd argument 'pattern'... importData(spark, path, pattern) ...adds a regex pattern to filter file names in a directory
    # transactionsDf = importData(spark, f"{project_dir}/test-data/sales/transactions") # example with no pattern will import all fiiles
    transactionsDf = importData(spark, f"{project_dir}/test-data/sales/transactions", ".json$") # will filter json files only
    customersDf = importData(spark, f"{project_dir}/test-data/sales/customers.csv")
    productsDf = importData(spark, f"{project_dir}/test-data/sales/products.csv")

    # 2. transform the dataframe and 3. export the dataframe result
    delta_path = f"{project_dir}/test-delta/sales"
    finalDf = transformData(spark, transactionsDf, customersDf, productsDf, delta_path) 
    
    # close and quit the spark session
    stopSpark(spark)

def openConfig(filepath:str) -> dict:
    """ opens the json configuration file  """
    if isinstance(filepath, str) and os.path.exists(filepath):
        return class_pyspark.Sparkclass(config={}).openJson(filepath)

def sparkStart(conf:dict) -> SparkSession:
    """ start a spark session """
    if isinstance(conf, dict) and isinstance(conf, dict):
        return class_pyspark.Sparkclass(config={}).sparkStart(conf)

def stopSpark(spark) -> None:
    """ ends the spark session """
    spark.stop() if isinstance(spark, SparkSession) else None

def importData(spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> list:
    """ get data from directories or files """
    if isinstance(datapath, str) and os.path.exists(datapath):
        return class_pyspark.Sparkclass(config={}).importData(spark, datapath, pattern)

def showMySchema(df:DataFrame, filename:str) -> None:
    """ saves a file of the schema  """
    if isinstance(df, DataFrame):
        class_pyspark.Sparkclass(config={}).debugDf(df, filename)

def transformData(spark:SparkSession, transactionsDf:DataFrame, customersDf:DataFrame, productsDf:DataFrame, path:str) -> DataFrame:
    """ call your custom functions to tranform your data """
    # wrap the exportResult function around the transform function, which saves the transform result to a delta table 
    # after exporting the result we're now done with the ETL
    exportResult([ \
        (spark, cleanTransactions(transactionsDf), {"format":"delta", "path":f"{path}/transactions", "key":"customer_id"}), \
        (spark, cleanCustomers(customersDf), {"format":"delta", "path":f"{path}/customers", "key":"customer_id"}), \
        (spark, cleanProducts(productsDf), {"format":"delta", "path":f"{path}/products", "key":"product_id"}) \
    ])
    
    # this final step needn't be in jobs/sales.py and would be ideally placed in a new jobs application
    # i'm including it as indication that we could perform further queries on the delta table 
    # because we might load historic data from our existing tables or other data from a different table
    # here I'll use an example of the delta tables we've already just saved
    l = loadDeltaTables([ \
        (spark, f"{path}/transactions", "delta"), \
        (spark, f"{path}/customers", "delta"), \
        (spark, f"{path}/products", "delta") \
    ])
    # if you prefer sql to directly quering dataframes then zip the list of 'dataframes' with a list of 'table names' 
    listOfDf = list(zip(l, ["transactions", "customers", "products"]))
    # then create temp tables that we can perform sql queries on
    createTempTables(spark, listOfDf)
    # from here, include functions to do more stuff
    
    # print delta tables to terminal 
    df = spark.sql("SELECT * FROM transactions")
    df.show()

def cleanTransactions(df:DataFrame) -> DataFrame:
    """ custom function - flatten nested columns and cast column types"""
    if isinstance(df, DataFrame):
        df1 = df.withColumn("basket_explode", explode(col("basket"))).drop("basket")
        df2 = df1.select(col("customer_id"), \
            col("date_of_purchase"), \
            col("basket_explode.*") \
        ) \
        .withColumn("date", col("date_of_purchase").cast("Date")) \
        .withColumn("price", col("price").cast("Integer"))
        showMySchema(df2, "transactions") 
        return df2
     
def cleanCustomers(df:DataFrame) -> DataFrame:
    """ custom function - cast column types """
    if isinstance(df, DataFrame):
        df1 = df \
            .withColumn("loyalty_score", col("loyalty_score").cast("Integer"))
        showMySchema(df1, "customers")
        return df1

def cleanProducts(df:DataFrame) -> DataFrame:
    """ custom function - cast column types """
    if isinstance(df, DataFrame):
        showMySchema(df, "products")
        return df

def createTempTables(spark:SparkSession, listOfDf:list) -> None:
    """ input is a list of tuples (dataframe, "tablename"), create temporary SQL tables in memory"""
    c = [(lambda x: class_pyspark.Sparkclass(config={}).createTempTables(x)) (x) for x in listOfDf]
    d = [(lambda x: class_pyspark.Sparkclass(config={}).debugTables(x)) (x) for x in spark.catalog.listTables()]
    
def exportResult(listOfDf:list) -> None:
    """ input is a list of tuples (dataframe, "tablename"), write to various file formats including delta lake tables """
    c = [(lambda x: class_pyspark.Sparkclass(config={}).exportDf(x)) (x) for x in listOfDf]

def loadDeltaTables(listOfPaths:list) -> list:
    """ load data from delta tables for various reasons like historic or additional data sources 
        you could also include this function in our main function above rather than from our transformData function
    """
    return [(lambda x: class_pyspark.Sparkclass(config={}).loadTables(x[0], x[1], x[2])) (x) for x in listOfPaths]

if __name__ == '__main__':
    main()



