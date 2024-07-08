import pytest
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient, DataLakeDirectoryClient, DataLakeFileClient
import os

from src.datalake.conexion_data_lake import ConexionDataLake

@pytest.mark.parametrize(["cuenta", "clave"],
	[("", ""),("cuenta", "clave")]
)
def test_crear_conexion_data_lake_error(cuenta, clave):

	with pytest.raises(Exception):

		ConexionDataLake(cuenta, clave)

def test_crear_conexion_data_lake():

	datalake=ConexionDataLake()

	assert isinstance(datalake.cliente_data_lake, DataLakeServiceClient)

	datalake.cerrarConexion()

def test_crear_contenedor_data_lake(datalake):

	datalake.crearContenedor("contenedor1")

	contenedores=list(datalake.cliente_data_lake.list_file_systems())

	assert contenedores[0]["name"]=="contenedor1"

	datalake.cerrarConexion()

def test_crear_contenedor_data_lake_existe_error(datalake):

	with pytest.raises(Exception):

		datalake.crearContenedor("contenedor1")

	datalake.cerrarConexion()

def test_contenedores_data_lake(datalake):

	contenedores=datalake.contenedores_data_lake()

	assert len(contenedores)==1

	datalake.cerrarConexion()

def test_eliminar_contenedor_data_lake(datalake):

	datalake.eliminarContenedor("contenedor1")

	contenedores=datalake.contenedores_data_lake()

	assert not contenedores

	datalake.cerrarConexion()

def test_eliminar_contenedor_data_lake_no_existe_error(datalake):

	with pytest.raises(Exception):

		datalake.eliminarContenedor("contenedor1")

	datalake.cerrarConexion()

def test_conexion_disponible(datalake):

	assert datalake.conexion_disponible()

	datalake.cerrarConexion()

@pytest.mark.parametrize(["nombre_contenedor"],
	[("contenedor1",),("contenedornacho",),("no_existo",)]
)
def test_existe_contenedor_no_existe(datalake, nombre_contenedor):

	assert not datalake.existe_contenedor(nombre_contenedor)

	datalake.cerrarConexion()

def test_existe_contenedor_existe(datalake):

	datalake.crearContenedor("contenedor2")

	assert datalake.existe_contenedor("contenedor2")

	datalake.cerrarConexion()

def test_obtener_contenedor_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.obtenerContenedor("contenedornacho")

	datalake.cerrarConexion()

def test_obtener_contenedor_existe(datalake):

	objeto_contenedor=datalake.obtenerContenedor("contenedor2")

	assert isinstance(objeto_contenedor, FileSystemClient)

	datalake.cerrarConexion()

def test_paths_contenedor_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.paths_contenedor("contenedornacho")

	datalake.cerrarConexion()

def test_paths_contenedor_no_paths(datalake):

	assert not datalake.paths_contenedor("contenedor2")

	datalake.cerrarConexion()

def test_paths_contenedor(datalake):

	datalake.crearCarpeta("contenedor2", "carpeta")

	paths=datalake.paths_contenedor("contenedor2")

	assert len(paths)==1

	datalake.cerrarConexion()

def test_existe_carpeta_contenedor_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.existe_carpeta("contenedornacho", "carpeta")

def test_existe_carpeta_no_existe(datalake):

	assert not datalake.existe_carpeta("contenedor2", "carpeta_no_existe")

def test_existe_carpeta(datalake):

	objeto_contenedor=datalake.obtenerContenedor("contenedor2")

	objeto_contenedor.create_directory("carpeta")

	assert datalake.existe_carpeta("contenedor2", "carpeta")

def test_crear_carpeta_contenedor_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.crearCarpeta("contenedornacho", "carpeta")

	datalake.cerrarConexion()

def test_crear_carpeta_contenedor_carpeta_existe(datalake):

	objeto_contenedor=datalake.obtenerContenedor("contenedor2")

	objeto_contenedor.create_directory("carpeta")

	with pytest.raises(Exception):

		datalake.crearCarpeta("contenedor2", "carpeta")

	datalake.cerrarConexion()

def test_crear_carpeta_contenedor(datalake):

	datalake.crearCarpeta("contenedor2", "carpeta_nueva")

	assert datalake.existe_carpeta("contenedor2", "carpeta_nueva")

	datalake.cerrarConexion()

def test_eliminar_carpeta_contenedor_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.eliminarCarpeta("contenedornacho", "carpeta")

	datalake.cerrarConexion()

def test_eliminar_carpeta_contenedor_carpeta_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.eliminarCarpeta("contenedor2", "carpeta_eliminar")

	datalake.cerrarConexion()

def test_eliminar_carpeta_contenedor(datalake):

	datalake.crearCarpeta("contenedor2", "carpeta_eliminar")

	datalake.eliminarCarpeta("contenedor2", "carpeta_eliminar")

	assert not datalake.existe_carpeta("contenedor2", "carpeta_eliminar")

	datalake.cerrarConexion()

def test_paths_carpeta_contenedor_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.paths_carpeta_contenedor("contenedornacho", "carpeta")

	datalake.cerrarConexion()

def test_paths_carpeta_contenedor_no_existe_carpeta(datalake):

	with pytest.raises(Exception):

		datalake.paths_carpeta_contenedor("contenedor2", "carpeta_no_existe")

	datalake.cerrarConexion()

def test_paths_carpeta_contenedor_no_paths(datalake):

	assert not datalake.paths_carpeta_contenedor("contenedor2", "carpeta_nueva")

	datalake.cerrarConexion()

def test_paths_carpeta_contenedor(datalake):

	datalake.crearCarpeta("contenedor2", "carpeta_nueva/carpeta_interna")

	paths_carpeta=datalake.paths_carpeta_contenedor("contenedor2", "carpeta_nueva")

	assert len(paths_carpeta)==1

	datalake.cerrarConexion()

def test_obtener_carpeta_contenedor_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.obtenerCarpeta("contenedornacho", "carpeta")

	datalake.cerrarConexion()

def test_obtener_carpeta_carpeta_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.obtenerCarpeta("contenedor2", "carpeta_no_existe")

	datalake.cerrarConexion()

def test_obtener_carpeta_contenedor(datalake):

	datalake.crearCarpeta("contenedor2", "carpeta_creada")

	objeto_carpeta=datalake.obtenerCarpeta("contenedor2", "carpeta_creada")

	assert isinstance(objeto_carpeta, DataLakeDirectoryClient)

	datalake.cerrarConexion()




def test_obtener_file_contenedor_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.obtenerFile("contenedornacho", "carpeta_file", "archivo.txt")

	datalake.cerrarConexion()

def test_obtener_file_carpeta_no_existe(datalake):

	with pytest.raises(Exception):

		datalake.obtenerFile("contenedor2", "carpeta_no_existe", "archivo.txt")

	datalake.cerrarConexion()

def test_obtener_file_contenedor(datalake):

	objeto_file=datalake.obtenerFile("contenedor2", "carpeta_creada", "archivo.txt")

	assert isinstance(objeto_file, DataLakeFileClient)

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

def crearArchivoTXT(ruta:str, nombre:str)->None:

	ruta_archivo=os.path.join(ruta, nombre)

	with open(ruta_archivo, "w") as file:

	    file.write("Nacho")

def test_subir_archivo_contenedor_no_existe(datalake):

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	nombre_archivo="archivo.txt"

	with pytest.raises(Exception):

		datalake.subirArchivo("contenedornacho", "carpeta_file", ruta_carpeta, nombre_archivo)

	datalake.cerrarConexion()

def test_subir_archivo_carpeta_no_existe(datalake):

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	nombre_archivo="archivo.txt"

	with pytest.raises(Exception):

		datalake.subirArchivo("contenedor2", "carpeta_no_existe", ruta_carpeta, nombre_archivo)

	datalake.cerrarConexion()

def test_subir_archivo_local_no_existe(datalake):

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake", "no_existe")

	nombre_archivo="archivo.txt"

	with pytest.raises(Exception):

		datalake.subirArchivo("contenedor2", "carpeta_creada", ruta_carpeta, nombre_archivo)

	datalake.cerrarConexion()

def test_subir_archivo(datalake):

	ruta_carpeta=os.path.join(os.getcwd(), "Archivos_Tests_Data_Lake")

	nombre_archivo="archivo.txt"

	crearCarpeta(ruta_carpeta)

	vaciarCarpeta(ruta_carpeta)

	crearArchivoTXT(ruta_carpeta, nombre_archivo)

	datalake.subirArchivo("contenedor2", "carpeta_creada", ruta_carpeta, nombre_archivo)

	archivos=datalake.paths_carpeta_contenedor("contenedor2", "carpeta_creada")

	assert len(archivos)==1
	assert archivos[0]["name"]==f"carpeta_creada/{nombre_archivo}"

	vaciarCarpeta(ruta_carpeta)

	borrarCarpeta(ruta_carpeta)

	datalake.eliminarContenedor("contenedor2")

	datalake.cerrarConexion()