import pytest
import sys
import os

# Ensure dashboard is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dashboard')))

from app import app

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_health_endpoint(client):
    rv = client.get('/api/health')
    assert rv.status_code == 200
    json_data = rv.get_json()
    assert 'status' in json_data
