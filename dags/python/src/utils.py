from typing import List
import os
import wget
import requests
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

from .datalake.conexion_data_lake import ConexionDataLake

from .database.conexion import Conexion

def entorno_creado(nombre_contenedor:str)->bool:

	datalake=ConexionDataLake()

	contenedores=datalake.contenedores_data_lake()

	datalake.cerrarConexion()

	contenedor_existe=[contenedor for contenedor in contenedores if nombre_contenedor in contenedor["name"]]

	return False if not contenedor_existe else True

def crearEntornoDataLake(nombre_contenedor:str, nombres_carpetas:List[str])->None:

	datalake=ConexionDataLake()

	datalake.crearContenedor(nombre_contenedor)

	for nombre_carpeta in nombres_carpetas:

		datalake.crearCarpeta(nombre_contenedor, nombre_carpeta)

	datalake.cerrarConexion()

def subirArchivosDataLake(nombre_contenedor:str, nombre_carpeta:str, ruta_local_carpeta:str)->None:

	datalake=ConexionDataLake()

	archivos_carpeta_contenedor=datalake.paths_carpeta_contenedor(nombre_contenedor, nombre_carpeta)

	archivos_carpeta_contenedor_limpios=[archivo.name.split(f"{nombre_carpeta}/")[1] for archivo in archivos_carpeta_contenedor]

	try:

		archivos_local=os.listdir(ruta_local_carpeta)

	except Exception:

		raise Exception("La ruta de la carpeta local de los archivos es incorrecta")

	for archivo_local in archivos_local:

		if archivo_local not in archivos_carpeta_contenedor_limpios:

			datalake.subirArchivo(nombre_contenedor, nombre_carpeta, ruta_local_carpeta, archivo_local)

def url_disponible(url:str)->bool:

	try:

		peticion=requests.get(url)

		return False if peticion.status_code!=200 else True

	except Exception:

		return False

def realizarDescarga(url_archivo:str, ruta_archivos:str, nombre_archivo:str, tipo_archivo:str="parquet")->None:

	try:
		
		wget.download(url_archivo, os.path.join(ruta_archivos, f"{nombre_archivo}.{tipo_archivo}"))

		time.sleep(2)
	
	except Exception as e:
	
		raise Exception(f"No se ha podido descargar el archivo de {nombre_archivo}")

def descargarArchivo(url_archivo:str, ruta_archivos:str, nombre_archivo:str)->None:

	if url_disponible(url_archivo):

		realizarDescarga(url_archivo, ruta_archivos, nombre_archivo)

	else: 

		raise Exception(f"No se ha podido descargar el archivo de {nombre_archivo}")

def obtenerFecha(fecha_inicio:str="2024-01")->str:

	con=Conexion()

	if con.tabla_vacia():

		con.cerrarConexion()

		return fecha_inicio

	fecha_maxima=con.fecha_maxima()

	con.cerrarConexion()

	fecha_datetime=datetime.strptime(fecha_maxima, "%Y-%m")

	fecha_mes_siguiente=fecha_datetime+relativedelta(months=1)

	return fecha_mes_siguiente.strftime("%Y-%m")