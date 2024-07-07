from pyspark.sql import SparkSession, DataFrame
from typing import Optional
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import sys

from configazure import CUENTA, APLICACION_ID, TENANT_ID, CLIENTE_SECRETO, CONTENEDOR

def crearSesion()->Optional[SparkSession]:

	try:

		return SparkSession.builder\
							.appName("SparkTaxis")\
							.config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6")\
							.config(f"spark.hadoop.fs.azure.account.auth.type.{CUENTA}.dfs.core.windows.net", "OAuth")\
						    .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{CUENTA}.dfs.core.windows.net",
						            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")\
	    					.config(f"fs.azure.account.oauth2.client.id.{CUENTA}.dfs.core.windows.net", APLICACION_ID)\
						    .config(f"fs.azure.account.oauth2.client.secret.{CUENTA}.dfs.core.windows.net", CLIENTE_SECRETO)\
						    .config(f"fs.azure.account.oauth2.client.endpoint.{CUENTA}.dfs.core.windows.net",
						    		f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")\
							.getOrCreate()

	except Exception:

		print("Error al crear la sesion de Spark")
		sys.exit()

def preocesarData(df:DataFrame)->DataFrame:

	return df.withColumn("hour", F.hour(F.col("tpep_pickup_datetime")))\
			.withColumn("date", F.to_date(F.col("tpep_pickup_datetime")))\
			.groupBy("date","hour").count().orderBy("date", "hour", ascending=True)


if __name__ == "__main__":

	spark=crearSesion()

	df=spark.read.parquet("./data/*.parquet")

	df_procesado=preocesarData(df)

	ruta_parquet=f"abfss://{CONTENEDOR}@{CUENTA}.dfs.core.windows.net/data_taxis"

	df_procesado.write.mode("overwrite").partitionBy("date").parquet(ruta_parquet)

	df_data_lake=spark.read.parquet(ruta_parquet)

	df_data_lake.show()