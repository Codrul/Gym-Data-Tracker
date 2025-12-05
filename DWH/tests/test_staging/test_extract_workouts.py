import pytest
from unittest.mock import MagicMock
from DWH.extract_scripts.extract_workouts import load_workouts


@pytest.fixture
def mock_gc():
    mock_sheet = MagicMock()
    mock_worksheet = MagicMock()

    mock_worksheet.get_all_records.return_value = [
        {
            'Workout number': 'W1', 'Date': '2025-08-01', 'Set': '1', 'Exercise': 'Squat',
            'Reps': '10', 'Load': '60', 'Resistance type': 'Barbell',
            'Set type': 'Straight', 'Comments': 'Good form', 'Workout type': 'Strength'
        },
        {
            'Workout number': 'W2', 'Date': '2025-08-02', 'Set': '1', 'Exercise': 'Bench Press',
            'Reps': '8', 'Load': '80', 'Resistance type': 'Barbell',
            'Set type': 'Straight', 'Comments': 'Heavy', 'Workout type': 'Strength'
        },
        {  # duplicate of W2
            'Workout number': 'W2', 'Date': '2025-08-02', 'Set': '1', 'Exercise': 'Bench Press',
            'Reps': '8', 'Load': '80', 'Resistance type': 'Barbell',
            'Set type': 'Straight', 'Comments': 'Heavy', 'Workout type': 'Strength'
        }
    ]

    mock_sheet.open_by_key.return_value.worksheet.return_value = mock_worksheet
    return mock_sheet


@pytest.fixture
def mock_engine():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    insert_calls = []

    def execute_side_effect(stmt, *args, **kwargs):
        stmt_str = str(stmt).lower()
        if "select" in stmt_str:
            if len(insert_calls) < 2:
                return MagicMock(fetchone=MagicMock(return_value=None))
            return MagicMock(fetchone=MagicMock(return_value=("W2",)))
        elif "insert" in stmt_str:
            insert_calls.append(stmt_str)
            return None
        return None

    mock_conn.execute.side_effect = execute_side_effect
    mock_engine._insert_calls = insert_calls

    return mock_engine


def test_load_workouts(mock_gc, mock_engine):
    success_msg = []
    error_msg = []

    load_workouts(mock_gc, mock_engine, success_msg, error_msg)

    assert not error_msg
    assert any("2 rows have been loaded into *staging_layer.workouts*" in msg for msg in success_msg)
    assert len(mock_engine._insert_calls) == 2
