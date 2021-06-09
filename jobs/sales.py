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

def main(project_dir:str):
    """ starts a spark job
        second argument in importData(path, pattern) adds a regex pattern to filter file names in a directory
    """

    # start session from json config file
    conf = openConfig(f"{project_dir}/json/sales.json")
    spark = sparkStart(conf)
    
    # dataframes
    #transactionsDf = importData(spark, f"{project_dir}/test-data/sales/transactions") # all fiiles
    transactionsDf = importData(spark, f"{project_dir}/test-data/sales/transactions", ".json$") # filter json only
    customersDf = importData(spark, f"{project_dir}/test-data/sales/customers.csv")
    productsDf = importData(spark, f"{project_dir}/test-data/sales/products.csv")
    
    # start transformations
    finalDf = transformData(spark, transactionsDf, customersDf, productsDf) 
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

def transformData(spark:SparkSession, transactionsDf:DataFrame, customersDf:DataFrame, productsDf:DataFrame) -> DataFrame:
    """ call your custom functions to tranform your data """
    #df = createTempTables(spark, [ (cleanTransactions(transactionsDf), "transactions"), (cleanCustomers(customersDf), "customers"), (cleanProducts(productsDf), "products") ])
    exportResult(spark, [ (cleanTransactions(transactionsDf), "transactions"), (cleanCustomers(customersDf), "customers"), (cleanProducts(productsDf), "products") ])
    
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
    
def exportResult(spark:SparkSession, listOfDf:list) -> None:
    """ input is a list of tuples (dataframe, "tablename"), write to various file formats including delta lake tables """
    c = [(lambda x: class_pyspark.Sparkclass(config={"export":"/tmp/delta"}).exportDf(x)) (x) for x in listOfDf]

if __name__ == '__main__':
    main(project_dir)



