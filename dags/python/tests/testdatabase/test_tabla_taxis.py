import pytest

def test_tabla_vacia(conexion):

	assert conexion.tabla_vacia()

def test_tabla_vacia_llena(conexion):

	conexion.c.execute("INSERT INTO taxis VALUES ('2019-04'), ('2019-06'), ('2019-06-22')")

	conexion.confirmar()

	assert not conexion.tabla_vacia()

def test_obtener_fecha_maxima_no_hay(conexion):

	assert not conexion.fecha_maxima()

@pytest.mark.parametrize(["fechas", "fecha_maxima"],
	[
		(["2019-06","2019-04","1998-02","2024-01","2024-06","2024-08"], "2024-08"),
		(["2019-06","2019-04","1998-02","2024-01","2020-06","2010-08"], "2024-01"),
		(["2029-06","2019-04","1998-02","2024-01","2024-06","2024-08"], "2029-06"),
		(["2019-06","2025-04","1998-02","2024-01","2024-06","2024-08"], "2025-04")
	]
)
def test_obtener_fecha_maxima(conexion, fechas, fecha_maxima):

	for fecha in fechas:

		conexion.c.execute(f"INSERT INTO taxis VALUES ('{fecha}')")

	conexion.confirmar()

	assert conexion.fecha_maxima()==fecha_maxima

def test_insertar_fecha(conexion):

	assert conexion.tabla_vacia()

	conexion.insertarFecha("2019-06")

	assert not conexion.tabla_vacia()