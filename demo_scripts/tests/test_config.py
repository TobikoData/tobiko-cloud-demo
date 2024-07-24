import os
import json
import pytest
from demo_scripts.config import get_service_account_info

def test_get_service_account_info():
    # Mock the environment variable
    mock_credentials = {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "test-key-id",
        "private_key": "test-private-key",
        "client_email": "test@example.com",
        "client_id": "test-client-id",
    }
    os.environ["GOOGLE_SQLMESH_CREDENTIALS"] = json.dumps(mock_credentials)

    # Call the function
    result = get_service_account_info()

    # Assert the result
    assert result == mock_credentials
    assert isinstance(result, dict)
    assert "type" in result
    assert result["type"] == "service_account"

def test_get_service_account_info_missing_env_var():
    # Remove the environment variable if it exists
    if "GOOGLE_SQLMESH_CREDENTIALS" in os.environ:
        del os.environ["GOOGLE_SQLMESH_CREDENTIALS"]

    # Assert that the function raises a KeyError
    with pytest.raises(KeyError):
        get_service_account_info()