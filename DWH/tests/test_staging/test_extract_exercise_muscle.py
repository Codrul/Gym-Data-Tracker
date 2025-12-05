import pytest
from unittest.mock import MagicMock
import pandas as pd 
from DWH.extract_scripts.extract_exercise_muscles import load_exercise_muscle


@pytest.fixture
def mock_gc():
    # mock sheets 
    mock_sheet = MagicMock()
    mock_worksheet = MagicMock()

    # sample data 
    mock_worksheet.get_all_records.return_value = [
        {
            'Exercise_ID': '1',
            'Ex_name': 'Push up',
            'Muscle_ID': '1',
            'Musc_name': 'Chest',
            'Role': 'Primary'
        },
        {
             'Exercise_ID': '2',
            'Ex_name': 'Pull up',
            'Muscle_ID': '2',
            'Musc_name': 'Back',
            'Role': 'Primary'
        },
        {
             'Exercise_ID': '2',
            'Ex_name': 'Pull up',
            'Muscle_ID': '2',
            'Musc_name': 'Back',
            'Role': 'Primary'
        }
    ]

    mock_sheet.open_by_key.return_value.worksheet.return_value = mock_worksheet
    return mock_sheet


@pytest.fixture
def mock_engine():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    # Track inserts
    insert_calls = []

    def execute_side_effect(sql, *args, **kwargs):
        sql_str = str(sql).lower()
        if "select" in sql_str:
            # Decide if it's a duplicate check
            if len(insert_calls) < 2:  
                return MagicMock(fetchone=MagicMock(return_value=None))
            else:  
                return MagicMock(fetchone=MagicMock(return_value=("2",)))
        elif "insert" in sql_str:
            insert_calls.append(sql_str)
            return None
        return None

    mock_conn.execute.side_effect = execute_side_effect
    mock_engine._insert_calls = insert_calls  # so test can check later

    return mock_engine


def test_extract_exercise_muscle(mock_gc, mock_engine):
    error_msg = []
    success_msg = []

    load_exercise_muscle(mock_gc, mock_engine, error_msg, success_msg)

    assert not error_msg
    assert any("2 rows have been loaded" in msg for msg in success_msg)

    assert len(mock_engine._insert_calls) == 2
