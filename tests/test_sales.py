#!/usr/bin/python3

import json, os, sys, unittest

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.session import SparkSession 
from pyspark.sql.functions import col, explode


project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(1, project_dir)
from jobs import sales


class TestSpark(unittest.TestCase):
    
    def setUp(self):
        conf = sales.openConfig(f"{project_dir}/json/sales.json")
        self.spark = sales.sparkStart(conf)
        self.assertIsInstance(self.spark, SparkSession)
    
    def tearDown(self):
        self.spark.stop()
    
    def test_dataFrameFromDirectory(self):
        df = sales.importData(self.spark, f"{project_dir}/test-data/sales/transactions")
        self.assertIsInstance(df, DataFrame)

    def test_dataFrameFromDirectoryFiltered(self):
        df = sales.importData(self.spark, f"{project_dir}/test-data/sales/transactions", ".json$")
        self.assertIsInstance(df, DataFrame)

    #def test_dataFrameFromDirectoryFilteredNoFilesFound(self):
    #    df = sales.importData(self.spark, "{project_dir}/test-data/sales/transactions", ".tab$")
    #    self.assertIsInstance(df, DataFrame)

    def test_transformTransactions(self):
        df = sales.importData(self.spark, f"{project_dir}/test-data/sales/transactions", ".json$")
        tdf = sales.transformData(self.spark, transactionsDf=df, customersDf=None, productsDf=None)
        self.assertIsInstance(tdf, DataFrame)

if __name__ == '__main__':
    unittest.main()


