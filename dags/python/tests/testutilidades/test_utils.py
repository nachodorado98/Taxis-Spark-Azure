import pytest
import os
import time

from src.utils import entorno_creado, crearEntornoDataLake, subirArchivosDataLake, url_disponible
from src.utils import realizarDescarga, descargarArchivo

def test_entorno_creado_no_creado():

	assert not entorno_creado("contenedor3")

def test_entorno_creado(datalake):

	datalake.crearContenedor("contenedor3")

	time.sleep(2)

	assert entorno_creado("contenedor3")

	datalake.eliminarContenedor("contenedor3")

	datalake.cerrarConexion()

@pytest.mark.parametrize(["carpetas", "contenedor"],
	[
		(["carpeta4", "carpeta5"], 1),
		(["carpeta6"], 2),
		(["carpeta7", "carpeta8", "carpeta9"], 3),
		(["carpeta10", "carpeta11", "carpeta12", "carpeta13", "carpeta14"], 4)
	]
)
def test_crear_entorno_data_lake(datalake, carpetas, contenedor):

	crearEntornoDataLake(f"contenedornuevo{contenedor}", carpetas)

	time.sleep(2)

	assert entorno_creado(f"contenedornuevo{contenedor}")
	assert len(datalake.paths_contenedor(f"contenedornuevo{contenedor}"))==len(carpetas)

	for carpeta in carpetas:

		datalake.eliminarCarpeta(f"contenedornuevo{contenedor}", carpeta)

	datalake.eliminarContenedor(f"contenedornuevo{contenedor}")

	time.sleep(1)

	datalake.cerrarConexion()

def borrarCarpeta(ruta:str)->None:

	if os.path.exists(ruta):

		os.rmdir(ruta)

def crearCarpeta(ruta:str)->None:

	if not os.path.exists(ruta):

		os.mkdir(ruta)

def vaciarCarpeta(ruta:str)->None:

	if os.path.exists(ruta):

		for archivo in os.listdir(ruta):

			os.remove(os.path.join(ruta, archivo))

def test_subir_archivo_data_lake_contenedor_no_existe():

	with pytest.raises(Exception):

		subirArchivosDataLake("contenedornacho", "carpeta", "ruta_local")

def test_subir_archivo_data_lake_carpeta_no_existe():

	with pytest.raises(Exception):

		subirArchivosDataLake("contenedor4", "carpeta", "ruta_local")

def test_subir_archivo_data_lake_local_no_existe(datalake):

	crearEntornoDataLake("contenedor4", ["carpeta_creada"])

	with pytest.raises(Exception):

		subirArchivosDataLake("contenedor4", "carpeta_creada", "ruta_local")

	datalake.cerrarConexion()

def test_subir_archivo_data_lake_archivo_no_existen(datalake):

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	crearCarpeta(ruta_carpeta)

	subirArchivosDataLake("contenedor4", "carpeta_creada", ruta_carpeta)

	archivos_carpeta_contenedor=datalake.paths_carpeta_contenedor("contenedor4", "carpeta_creada")

	assert not archivos_carpeta_contenedor

	datalake.cerrarConexion()

def crearArchivoTXT(ruta:str, nombre:str)->None:

	ruta_archivo=os.path.join(ruta, nombre)

	with open(ruta_archivo, "w") as file:

		file.write("Nacho")

def test_subir_archivo_data_lake(datalake):

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	nombre_archivo="archivo.txt"

	crearArchivoTXT(ruta_carpeta, nombre_archivo)

	subirArchivosDataLake("contenedor4", "carpeta_creada", ruta_carpeta)

	archivos_carpeta_contenedor_nuevos=datalake.paths_carpeta_contenedor("contenedor4", "carpeta_creada")

	assert len(archivos_carpeta_contenedor_nuevos)==1

	datalake.eliminarContenedor("contenedor4")

	datalake.cerrarConexion()

	vaciarCarpeta(ruta_carpeta)

def test_subir_archivo_data_lake_archivo_existente(datalake):

	crearEntornoDataLake("contenedor5", ["carpeta"])

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	nombre_archivo="archivo.txt"

	crearArchivoTXT(ruta_carpeta, nombre_archivo)

	datalake.subirArchivo("contenedor5", "carpeta", ruta_carpeta, nombre_archivo)

	archivos_carpeta_contenedor=datalake.paths_carpeta_contenedor("contenedor5", "carpeta")

	assert len(archivos_carpeta_contenedor)==1

	subirArchivosDataLake("contenedor5", "carpeta", ruta_carpeta)

	archivos_carpeta_contenedor_nuevos=datalake.paths_carpeta_contenedor("contenedor5", "carpeta")

	assert len(archivos_carpeta_contenedor_nuevos)==1

	datalake.eliminarContenedor("contenedor5")

	datalake.cerrarConexion()

	vaciarCarpeta(ruta_carpeta)

def test_subir_archivo_data_lake_archivos_existentes_no_existentes(datalake):

	crearEntornoDataLake("contenedor6", ["carpeta"])

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	nombre_archivos_subir=[f"archivo{numero}_subir.txt" for numero in range(1,6)]

	for nombre_archivo in nombre_archivos_subir:

		crearArchivoTXT(ruta_carpeta, nombre_archivo)

		datalake.subirArchivo("contenedor6", "carpeta", ruta_carpeta, nombre_archivo)

	nombre_archivos_no_subir=[f"archivo{numero}_no_subir.txt" for numero in range(1,6)]

	for nombre_archivo in nombre_archivos_no_subir:

		crearArchivoTXT(ruta_carpeta, nombre_archivo)

	archivos_carpeta_contenedor=datalake.paths_carpeta_contenedor("contenedor6", "carpeta")

	assert len(archivos_carpeta_contenedor)==5
	assert len(os.listdir(ruta_carpeta))==10

	subirArchivosDataLake("contenedor6", "carpeta", ruta_carpeta)

	archivos_carpeta_contenedor_nuevos=datalake.paths_carpeta_contenedor("contenedor6", "carpeta")

	assert len(archivos_carpeta_contenedor_nuevos)==10

	datalake.eliminarContenedor("contenedor6")

	datalake.cerrarConexion()

	vaciarCarpeta(ruta_carpeta)

	borrarCarpeta(ruta_carpeta)

@pytest.mark.parametrize(["url"],
	[
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2099-01.parquet",),
		("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zona_lookup.csv",),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2000-01.parquet",),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2010-01.parquet",),
		("url",),
	]
)
def test_url_disponible_no_disponible(url):

	assert not url_disponible(url)

@pytest.mark.parametrize(["url"],
	[
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-06.parquet",),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-04.parquet",),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-04.parquet",),
		("https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_staten_island.jpg",),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2020-01.parquet",)
	]
)
def test_url_disponible(url):

	assert url_disponible(url)

@pytest.mark.parametrize(["url"],
	[(None,), ("url_antigua",), ("url_nueva",),("url",)]
)
def test_realizar_descarga_error(url):

	with pytest.raises(Exception):

		realizarDescarga(url, "ruta", "nombre")

@pytest.mark.parametrize(["url_archivo", "nombre_archivo"],
	[
		("https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2020-01.parquet", "trip"),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-06.parquet", "taxis_amarillos"),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-04.parquet", "yellow_tripdata_2019-04"),
	]
)
def test_realizar_descarga(url_archivo, nombre_archivo):

	ruta_carpeta=os.path.join(os.getcwd(), "testutilidades", "Archivos_Tests")

	crearCarpeta(ruta_carpeta)

	vaciarCarpeta(ruta_carpeta)

	realizarDescarga(url_archivo, ruta_carpeta, nombre_archivo)

	time.sleep(3)

	assert os.path.exists(os.path.join(ruta_carpeta, f"{nombre_archivo}.parquet"))

	vaciarCarpeta(ruta_carpeta)

	borrarCarpeta(ruta_carpeta)

@pytest.mark.parametrize(["url"],
	[
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2099-01.parquet",),
		("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zona_lookup.csv",),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2000-01.parquet",),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2010-01.parquet",),
		("url",)
	]
)
def test_descargar_archivo_url_no_disponible(url):

	with pytest.raises(Exception):

		descargarArchivo(url, "ruta", "archivo")

@pytest.mark.parametrize(["url_archivo", "nombre_archivo"],
	[
		("https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2020-01.parquet", "trip"),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-06.parquet", "taxis_amarillos"),
		("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-04.parquet", "yellow_tripdata_2019-04"),
	]
)
def test_descargar_archivo(url_archivo, nombre_archivo):

	ruta_carpeta=os.path.join(os.getcwd(), "testutilidades", "Archivos_Tests")

	crearCarpeta(ruta_carpeta)

	vaciarCarpeta(ruta_carpeta)

	descargarArchivo(url_archivo, ruta_carpeta, nombre_archivo)

	time.sleep(3)

	assert os.path.exists(os.path.join(ruta_carpeta, f"{nombre_archivo}.parquet"))

	vaciarCarpeta(ruta_carpeta)

	borrarCarpeta(ruta_carpeta)