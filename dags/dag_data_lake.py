from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import os
import time

from config import URL, CONTENEDOR, BRONZE, SILVER

from python.src.datalake.conexion_data_lake import ConexionDataLake

from python.src.utils import entorno_creado, crearEntornoDataLake, subirArchivosDataLake
from python.src.utils import descargarArchivo, obtenerFecha

from python.src.database.conexion import Conexion

def agregarFecha(fecha:str)->None:

	con=Conexion()

	con.insertarFecha(fecha)

	con.cerrarConexion()

def obtenerArchivoParquet()->str:

	fecha=obtenerFecha()

	nombre_archivo=f"yellow_tripdata_{fecha}"

	url_archivo=URL+f"{nombre_archivo}.parquet"

	ruta_data=os.path.join(os.getcwd(), "dags", "data")

	if not os.path.exists(ruta_data):

		os.mkdir(ruta_data)

	try:

		descargarArchivo(url_archivo, ruta_data, nombre_archivo)

		time.sleep(3)

		agregarFecha(fecha)

		return "data_lake_disponible"

	except Exception:

		return "log_descarga_archivo"

def data_lake_disponible()->str:

	try:

		con=ConexionDataLake()

		con.cerrarConexion()

		return "datalake.entorno_data_lake_creado"

	except Exception:

		return "log_data_lake"

def entorno_data_lake_creado():

	if not entorno_creado(CONTENEDOR):

		return "datalake.crear_entorno_data_lake"

	return "datalake.no_crear_entorno_data_lake"

def creacion_entorno_data_lake()->None:

	crearEntornoDataLake(CONTENEDOR, [BRONZE, SILVER])

	print("Entorno Data Lake creado")

def vaciarCarpeta(ruta:str)->None:

	if os.path.exists(ruta):

		for archivo in os.listdir(ruta):

			os.remove(os.path.join(ruta, archivo))

def subirParquetDataLake()->None:

	ruta_data=os.path.join(os.getcwd(), "dags", "data")

	try:

		subirArchivosDataLake(CONTENEDOR, BRONZE, ruta_data)

	except Exception as e:

			print(f"Error al subir el parquet al data lake")

			print(e)

	vaciarCarpeta(ruta_data)


with DAG("dag_data_lake",
		start_date=days_ago(1),
		description="DAG para subir el parquet al datalake",
		schedule_interval="@monthly",
		catchup=False) as dag:


	with TaskGroup("datalake") as tareas_datalake:

		tarea_entorno_data_lake_creado=BranchPythonOperator(task_id="entorno_data_lake_creado", python_callable=entorno_data_lake_creado)

		tarea_crear_entorno_data_lake=PythonOperator(task_id="crear_entorno_data_lake", python_callable=creacion_entorno_data_lake)

		tarea_no_crear_entorno_data_lake=DummyOperator(task_id="no_crear_entorno_data_lake")


		tarea_entorno_data_lake_creado >> [tarea_crear_entorno_data_lake, tarea_no_crear_entorno_data_lake]

	tarea_obtener_archivo_parquet=BranchPythonOperator(task_id="obtener_archivo_parquet", python_callable=obtenerArchivoParquet)

	tarea_log_descarga_archivo=PythonOperator(task_id="log_descarga_archivo", python_callable=lambda: print("Descarga Archivo Error"))

	tarea_data_lake_disponible=BranchPythonOperator(task_id="data_lake_disponible", python_callable=data_lake_disponible, trigger_rule="none_failed_min_one_success")

	tarea_log_data_lake=PythonOperator(task_id="log_data_lake", python_callable=lambda: print("Data Lake no disponible"))

	tarea_subir_parquet_data_lake=PythonOperator(task_id="subir_parquet_data_lake", python_callable=subirParquetDataLake, trigger_rule="none_failed_min_one_success")

tarea_obtener_archivo_parquet >> [tarea_data_lake_disponible, tarea_log_descarga_archivo]

tarea_data_lake_disponible >> [tareas_datalake, tarea_log_data_lake]

tareas_datalake >> tarea_subir_parquet_data_lake 