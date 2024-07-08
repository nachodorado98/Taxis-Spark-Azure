import os
import sys
sys.path.append(os.path.abspath(".."))

import pytest

from src.datalake.conexion_data_lake import ConexionDataLake

@pytest.fixture()
def datalake():

    return ConexionDataLake()