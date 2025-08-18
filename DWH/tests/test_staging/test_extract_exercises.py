import pytest
from unittest.mock import MagicMock
from DWH.extract_scripts.extract_exercises import load_exercises


@pytest.fixture
def mock_gc():
    """Mock Google Sheets connection"""
    mock_sheet = MagicMock()
    mock_worksheet = MagicMock()

    mock_worksheet.get_all_records.return_value = [
        {'ID': '1', 'Name': 'Push up', 'Movement type': 'Push', 'Upper/Lower': 'Upper'},
        {'ID': '2', 'Name': 'Pull up', 'Movement type': 'Pull', 'Upper/Lower': 'Upper'},
        {'ID': '2', 'Name': 'Pull up', 'Movement type': 'Pull', 'Upper/Lower': 'Upper'},  
    ]

    mock_sheet.open_by_key.return_value.worksheet.return_value = mock_worksheet
    return mock_sheet


@pytest.fixture
def mock_engine():
    """Mock SQLAlchemy engine with duplicate check logic"""
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    insert_calls = []

    def execute_side_effect(stmt, *args, **kwargs):
        stmt_str = str(stmt).lower()
        if "select" in stmt_str:
            if len(insert_calls) < 2:
                return MagicMock(fetchone=MagicMock(return_value=None))

            return MagicMock(fetchone=MagicMock(return_value=("2",)))
        elif "insert" in stmt_str:
            insert_calls.append(stmt_str)
            return None
        return None

    mock_conn.execute.side_effect = execute_side_effect
    mock_engine._insert_calls = insert_calls

    return mock_engine


def test_load_exercises(mock_gc, mock_engine):
    error_msg = []
    success_msg = []

    load_exercises(mock_gc, mock_engine, error_msg, success_msg)
    # just like your mother telling you not to come home with bad grades
    assert not error_msg

    assert any("2 rows have been loaded" in msg for msg in success_msg)

    # Verify exactly 2 inserts happened
    assert len(mock_engine._insert_calls) == 2
