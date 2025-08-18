import pytest
from google.auth.exceptions import GoogleAuthError
from gspread.exceptions import APIError
from DWH.extract_scripts.connect import connect_sheets 

ID =  '1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ'


def test_connection():
    
    gc = connect_sheets()
    assert gc is not None

    try:
        sh = gc.open_by_key(ID)
        worksheets = sh.worksheets()

        assert len(worksheets) > 0, "No worksheets found."
        
    except (GoogleAuthError, APIError) as e:
        pytest.fail('Google Sheets connection failed :{e}' '1OiufKuY1WB_-tzfvKWZh9OHeCCEX81jQ1KHuNE5lZsQ'
)
