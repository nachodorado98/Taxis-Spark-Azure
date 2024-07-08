import os
import sys
sys.path.append(os.path.abspath(".."))

import pytest

from src.datalake.conexion_data_lake import ConexionDataLake

from src.database.conexion import Conexion


@pytest.fixture()
def datalake():

    return ConexionDataLake()

@pytest.fixture()
def conexion():

    return Conexion()