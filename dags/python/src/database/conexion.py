import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional

from .confconexion import *

# Clase para la conexion a la BBDD
class Conexion:

	def __init__(self)->None:

		try:

			self.bbdd=psycopg2.connect(host=HOST, user=USUARIO, password=CONTRASENA, port=PUERTO, database=BBDD)
			self.c=self.bbdd.cursor(cursor_factory=RealDictCursor)

		except psycopg2.OperationalError as e:

			print("Error en la conexion a la BBDD")

	# Metodo para cerrar la conexion a la BBDD
	def cerrarConexion(self)->None:

		self.c.close()
		self.bbdd.close()

	# Metodo para confirmar una accion
	def confirmar(self)->None:

		self.bbdd.commit()

	# Metodo para saber si la tabla esta vacia
	def tabla_vacia(self)->bool:

		self.c.execute("SELECT * FROM taxis")

		return True if not self.c.fetchall() else False

	# Metodo para obtener la fecha maxima
	def fecha_maxima(self)->Optional[str]:

		self.c.execute("""SELECT to_char(MAX(to_date(fecha||'-01','YYYY-MM-DD')),'YYYY-MM') AS fecha_maxima
							FROM taxis""")

		return self.c.fetchone()["fecha_maxima"]

	# Metodo para insertar un registro
	def insertarFecha(self, fecha:str)->None:

		self.c.execute("""INSERT INTO taxis
							VALUES(%s)""",
							(fecha,))

		self.confirmar()