from pyspark.sql import SparkSession, DataFrame
from typing import Optional
from pyspark.sql import functions as F
import sys

def crearSesion()->Optional[SparkSession]:

	try:

		return SparkSession.builder\
						.appName("SparkTaxis")\
						.getOrCreate()

	except Exception:

		print("Error al crear la sesion de Spark")
		sys.exit()


if __name__ == "__main__":

	spark=crearSesion()

	df=spark.read.parquet("./data/*.parquet")

	df=df.withColumn("hour", F.hour(F.col("tpep_pickup_datetime")))

	df=df.withColumn("date", F.to_date(F.col("tpep_pickup_datetime")))

	df=df.groupBy("date","hour").count().orderBy("date", "hour", ascending=True)

	df.write.mode("overwrite").partitionBy("date").parquet("./taxis_trips")