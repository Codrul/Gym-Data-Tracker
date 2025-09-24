from unittest.mock import patch, MagicMock
import pytest
from DWH.extract_scripts.connect import connect_db

def test_db_connection():
    with patch('DWH.extract_scripts.connect.create_engine') as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine
        engine = connect_db()
        assert engine == mock_engine
