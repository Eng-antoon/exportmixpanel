import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from unittest.mock import patch
import tempfile
import importlib

class TestDbConfig:
    def test_db_uri_from_env(self):
        # Save original environment
        original_db_uri = os.environ.get('DB_URI')
        
        try:
            # Set environment variable
            os.environ['DB_URI'] = 'sqlite:///test_env.db'
            
            # Import the module after setting the environment variable
            import db.config
            importlib.reload(db.config)
            
            # Verify the DB_URI is taken from the environment
            assert db.config.DB_URI == 'sqlite:///test_env.db'
        finally:
            # Restore original environment
            if original_db_uri:
                os.environ['DB_URI'] = original_db_uri
            else:
                os.environ.pop('DB_URI', None)
            
            # Reload the module to restore default state
            import db.config
            importlib.reload(db.config)
    
    def test_db_uri_default(self):
        # Remove the environment variable if it exists
        original_db_uri = os.environ.get('DB_URI')
        if 'DB_URI' in os.environ:
            del os.environ['DB_URI']
        
        try:
            # Re-import to get the default value
            import db.config
            importlib.reload(db.config)
            
            # Verify it falls back to the default value
            assert db.config.DB_URI == 'sqlite:///my_dashboard.db'
        finally:
            # Restore original environment
            if original_db_uri:
                os.environ['DB_URI'] = original_db_uri
    
    def test_api_token_from_env(self):
        # Save original environment
        original_api_token = os.environ.get('API_TOKEN')
        
        try:
            # Set environment variable
            os.environ['API_TOKEN'] = 'test_token_from_env'
            
            # Import the module after setting the environment variable
            import db.config
            importlib.reload(db.config)
            
            # Verify the API_TOKEN is taken from the environment
            assert db.config.API_TOKEN == 'test_token_from_env'
        finally:
            # Restore original environment
            if original_api_token:
                os.environ['API_TOKEN'] = original_api_token
            else:
                os.environ.pop('API_TOKEN', None)
            
            # Reload the module to restore default state
            import db.config
            importlib.reload(db.config)
    
    def test_api_token_default(self):
        # Remove the environment variable if it exists
        original_api_token = os.environ.get('API_TOKEN')
        if 'API_TOKEN' in os.environ:
            del os.environ['API_TOKEN']
        
        try:
            # Re-import to get the default value
            import db.config
            importlib.reload(db.config)
            
            # Verify it falls back to the default value (check the start of the token)
            assert db.config.API_TOKEN.startswith('eyJhbGciOiJub25lIn0')
        finally:
            # Restore original environment
            if original_api_token:
                os.environ['API_TOKEN'] = original_api_token
    
    def test_base_api_url(self):
        # Import the config
        from db.config import BASE_API_URL
        
        # Verify the API URL is set
        assert BASE_API_URL == "https://app.illa.blue/api/v2"
    
    def test_api_credentials(self):
        # Import the config
        from db.config import API_EMAIL, API_PASSWORD
        
        # Verify credentials are set
        assert API_EMAIL == "antoon.kamel@illa.com.eg"
        assert API_PASSWORD == "1234567"
        
        # In a real application, this would be a good place to test that:
        # 1. Credentials are stored securely (not in plain text)
        # 2. Secrets are not checked into version control
        # 3. Production credentials differ from development 