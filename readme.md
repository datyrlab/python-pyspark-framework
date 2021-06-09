# python-pyspark-framework

To quickly get started run this in your terminal

python3 python-pyspark-framework/jobs/sales.py

## Delta Lake

first install delta-spark via pip3 then in json/sales.json, change 

```Json
{"config":{"deltalake":false}}
```
to

```Json
{"config":{"deltalake":true}}
```

I've set this to false to allow you to install Delta Lake first should you want to use it

### function to export a dataframe to Delta Lake format 

See jobs/sales.py for an example of how I call the function exportResult() from inside transformData()

```Python
def transformData(spark:SparkSession, df:DataFrame) -> DataFrame:
    exportResult(spark, [ (dataframe, "table-name") ])
```

you don't need to call the function as I do. You can call the class method directly with this code


```Python
class_pyspark.Sparkclass(config={"export":"/tmp/delta"}).exportDf((dataframe, "table-name"))
```

## Dependencies

Apache Spark (https://spark.apache.org)

Python

```Bash
sudo pip3 install pyspark

sudo pip3 install delta-spark
```

## Contact me directly

Any questions, feedback, requests or suggestions then email me at datyrlab@gmail.com

https://twitter.com/datyrlab

