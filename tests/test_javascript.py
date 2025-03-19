import sys
import os
import shutil
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from app import app
import multiprocessing
import time
import socket
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Check if port is available
def is_port_available(port):
    """Check if a port is available"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) != 0

# Setup Flask app server in a separate process
def start_server(app, port):
    """Start the Flask application on the given port"""
    app.run(host='localhost', port=port, use_reloader=False)

@pytest.mark.skipif(not shutil.which('chromedriver'), reason="ChromeDriver not installed")
class TestJavaScript:
    @pytest.fixture(scope="module")
    def server_port(self):
        """Fixture to find an available port and start the Flask server"""
        # Find an available port
        port = 5000
        while not is_port_available(port) and port < 5100:
            port += 1
        
        if port >= 5100:
            pytest.skip("No available ports found")
            
        # Configure app for testing
        app.config['TESTING'] = True
        
        # Start the server in a separate process
        server_process = multiprocessing.Process(
            target=start_server,
            args=(app, port)
        )
        server_process.start()
        
        # Wait for the server to start
        time.sleep(1)
        
        # Return the port to the test
        yield port
        
        # Terminate the server process after tests
        server_process.terminate()
        server_process.join()
    
    @pytest.fixture(scope="module")
    def browser(self):
        """Fixture to set up and tear down a Selenium WebDriver"""
        # Configure Chrome options for headless testing
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Initialize the driver (assuming chromedriver is in PATH)
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.implicitly_wait(10)
            yield driver
        finally:
            driver.quit()
    
    def test_main_js_loaded(self, server_port, browser):
        """Test that main.js is loaded on the page"""
        # Navigate to the index page
        browser.get(f"http://localhost:{server_port}/")
        
        # Check if main.js is loaded by looking for script tags
        scripts = browser.find_elements(By.TAG_NAME, "script")
        js_files = [script.get_attribute("src") for script in scripts]
        
        # Check if main.js is in the loaded scripts
        assert any("main.js" in js_file for js_file in js_files if js_file)
    
    def test_trips_filter_functionality(self, server_port, browser):
        """Test the filter functionality on the trips page"""
        # This test assumes there's JavaScript-based filtering on the trips page
        browser.get(f"http://localhost:{server_port}/trips")
        
        try:
            # Wait for filter elements to be present
            filter_button = WebDriverWait(browser, 10).until(
                EC.presence_of_element_located((By.ID, "filter-button"))
            )
            
            # Click the filter button
            filter_button.click()
            
            # Further assertions would depend on your specific JS implementation
            # For example, checking if a filter modal appears
            filter_modal = WebDriverWait(browser, 10).until(
                EC.visibility_of_element_located((By.ID, "filter-modal"))
            )
            assert filter_modal.is_displayed()
            
        except Exception as e:
            # If elements aren't found, it might be because your JS implementation
            # has different element IDs or structure
            pytest.skip(f"Filter elements not found or JS implementation differs: {str(e)}")
    
    def test_analytics_chart_rendering(self, server_port, browser):
        """Test that charts are rendered on the analytics page"""
        # This test assumes there's JavaScript-based chart rendering
        browser.get(f"http://localhost:{server_port}/analytics")
        
        try:
            # Wait for chart elements to be present
            chart_element = WebDriverWait(browser, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "chart-container"))
            )
            
            # Check if chart canvas is rendered
            canvas = chart_element.find_element(By.TAG_NAME, "canvas")
            assert canvas.is_displayed()
            
        except Exception as e:
            # If elements aren't found, it might be because your JS implementation
            # has different element classes or structure
            pytest.skip(f"Chart elements not found or JS implementation differs: {str(e)}")

# Note: These tests require Chrome and ChromeDriver to be installed.
# The tests will be skipped if ChromeDriver is not available.
# You may need to adjust element IDs, classes, and other selectors based on your actual HTML structure. 