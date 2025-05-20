#!/usr/bin/env python3
"""
Metabase API Client

This module provides a client for interacting with the Metabase API.
"""

import requests
import logging
import json
import os
import time
from datetime import datetime
from functools import lru_cache

# Configuration for Metabase instance
METABASE_URL = os.environ.get("METABASE_URL", "https://data.illa.blue")
USERNAME = os.environ.get("METABASE_USERNAME", "antoon.kamel@illa.com.eg")
PASSWORD = os.environ.get("METABASE_PASSWORD", "24621333@Tony")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MetabaseClient:
    def __init__(self, base_url=METABASE_URL, username=USERNAME, password=PASSWORD):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.session_token = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with the Metabase API and get a session token."""
        url = f"{self.base_url}/api/session"
        logger.info(f"Authenticating to Metabase at: {url}")
        
        try:
            response = requests.post(
                url, 
                json={"username": self.username, "password": self.password}
            )
            response.raise_for_status()
            self.session_token = response.json()['id']
            logger.info("Metabase authentication successful")
            return True
        except Exception as e:
            logger.error(f"Metabase authentication failed: {str(e)}")
            self.session_token = None
            return False
    
    def get_question_data(self, question_id, parameters=None, max_retries=3, retry_delay=2):
        """
        Fetch data from a specific Metabase question.
        
        Args:
            question_id (int): The numeric ID of the Metabase question
            parameters (list, optional): List of parameters to pass to the question
            max_retries (int): Maximum number of retry attempts
            retry_delay (int): Delay between retries in seconds
            
        Returns:
            dict: The JSON response from the API or None if the request fails
        """
        if not self.session_token:
            if not self._authenticate():
                return None
        
        # Make sure we're using the numeric ID
        if isinstance(question_id, str) and "-" in question_id:
            question_id = question_id.split("-")[0]
        
        url = f"{self.base_url}/api/card/{question_id}/query"
        headers = {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.session_token
        }
        
        # Build the query payload
        query_payload = {"parameters": parameters or []}
        
        # Log the request details for debugging
        logger.info(f"Querying Metabase question {question_id} with parameters: {json.dumps(parameters or [])}")
        
        retries = 0
        while retries <= max_retries:
            try:
                response = requests.post(url, json=query_payload, headers=headers, timeout=30)
                
                # If we get an unauthorized response, try to re-authenticate
                if response.status_code == 401:
                    logger.warning("Session expired, re-authenticating...")
                    if self._authenticate():
                        headers["X-Metabase-Session"] = self.session_token
                        response = requests.post(url, json=query_payload, headers=headers, timeout=30)
                
                # Handle 500 server errors specifically
                if response.status_code == 500:
                    logger.error(f"Server error (500) from Metabase question {question_id}")
                    if retries < max_retries:
                        # Try again with simpler parameters (might be a parameter issue)
                        if parameters:
                            logger.info("Trying with simplified parameters...")
                            retries += 1
                            # Try with empty parameters as fallback
                            return self.get_question_data(question_id, [], max_retries - retries, retry_delay)
                        else:
                            # If already using empty parameters, just retry after delay
                            retries += 1
                            logger.info(f"Retrying in {retry_delay} seconds... (Attempt {retries}/{max_retries})")
                            time.sleep(retry_delay)
                            continue
                    else:
                        # If question doesn't work with or without parameters, try a different approach
                        logger.error("Maximum retries reached. The question might need to be reconfigured in Metabase.")
                        return None
                
                response.raise_for_status()
                
                # Try to parse the response as JSON
                try:
                    return response.json()
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON response from Metabase: {response.text[:200]}...")
                    return None
                
            except requests.exceptions.Timeout:
                logger.warning(f"Request timed out. Retrying... (Attempt {retries+1}/{max_retries})")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data from question {question_id}: {str(e)}")
                # Try to get response content if available
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Response status: {e.response.status_code}")
                    try:
                        logger.error(f"Response content: {e.response.text[:500]}...")
                    except:
                        pass
            
            # Increment retry counter and wait before retrying
            retries += 1
            if retries <= max_retries:
                logger.info(f"Retrying in {retry_delay} seconds... (Attempt {retries}/{max_retries})")
                time.sleep(retry_delay)
            else:
                logger.error(f"Maximum retries ({max_retries}) reached. Giving up.")
                return None
        
        return None
    
    def get_question_data_export(self, question_id, parameters=None, format="json", max_retries=3, retry_delay=2):
        """
        Fetch data from a specific Metabase question using the export endpoint.
        This allows fetching up to 1,000,000 rows (Metabase export limit).
        
        Args:
            question_id (int): The numeric ID of the Metabase question
            parameters (list, optional): List of parameters to pass to the question
            format (str): Export format - 'json' or 'csv'
            max_retries (int): Maximum number of retry attempts
            retry_delay (int): Delay between retries in seconds
            
        Returns:
            dict or list: The JSON response data or None if the request fails
        """
        if not self.session_token:
            if not self._authenticate():
                return None
        
        # Make sure we're using the numeric ID
        if isinstance(question_id, str) and "-" in question_id:
            question_id = question_id.split("-")[0]
        
        # Build the query for export
        query = {
            "type": "native",
            "native": {
                "query": None,  # The native query will be derived from the card/question
                "template-tags": {}
            },
            "database": None,  # This will be derived from the card/question
            "parameters": parameters or [],
            "middleware": {
                "js-int-to-string?": True,
                "add-default-userland-constraints?": True
            }
        }
        
        # URL encode the query
        import urllib.parse
        query_string = f"query={urllib.parse.quote(json.dumps(query))}"
        
        # Endpoint for exporting data
        url = f"{self.base_url}/api/card/{question_id}/query/{format}"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Metabase-Session": self.session_token,
            "Accept": "application/json" if format == "json" else "text/csv"
        }
        
        # Log the request details for debugging
        logger.info(f"Exporting data from Metabase question {question_id} in {format} format with parameters: {json.dumps(parameters or [])}")
        
        retries = 0
        while retries <= max_retries:
            try:
                response = requests.post(url, data=query_string, headers=headers, timeout=120)  # Longer timeout for exports
                
                # If we get an unauthorized response, try to re-authenticate
                if response.status_code == 401:
                    logger.warning("Session expired, re-authenticating...")
                    if self._authenticate():
                        headers["X-Metabase-Session"] = self.session_token
                        response = requests.post(url, data=query_string, headers=headers, timeout=120)
                
                # Handle 500 server errors specifically
                if response.status_code == 500:
                    logger.error(f"Server error (500) from Metabase question {question_id} export")
                    if retries < max_retries:
                        retries += 1
                        wait_time = retry_delay * (2 ** (retries - 1))  # Exponential backoff
                        logger.info(f"Retrying export in {wait_time} seconds... (Attempt {retries}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        # If export doesn't work after retries, return None
                        logger.error("Maximum retries reached. The export might not be supported for this question.")
                        return None
                
                response.raise_for_status()
                
                # Parse the response based on format
                if format == "json":
                    try:
                        result = response.json()
                        # Log the response structure for debugging
                        logger.info(f"Response type: {type(result).__name__}, structure: {str(result)[:500] if not isinstance(result, list) else f'list with {len(result)} items'}")
                        return result
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON response from Metabase export: {response.text[:500]}...")
                        return None
                else:  # CSV format - return raw text to be processed by caller
                    return response.text
                
            except requests.exceptions.Timeout:
                logger.warning(f"Export request timed out. Retrying... (Attempt {retries+1}/{max_retries})")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error exporting data from question {question_id}: {str(e)}")
                # Try to get response content if available
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Response status: {e.response.status_code}")
                    try:
                        logger.error(f"Response content: {e.response.text[:500]}...")
                    except:
                        pass
            
            # Increment retry counter and wait before retrying
            retries += 1
            if retries <= max_retries:
                wait_time = retry_delay * (2 ** (retries - 1))  # Exponential backoff
                logger.info(f"Retrying export in {wait_time} seconds... (Attempt {retries}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"Maximum retries ({max_retries}) reached. Giving up on export.")
                return None
        
        return None
    
    def get_all_trip_points(self, question_id, trip_id=None):
        """
        Get all trip points data using the export API to bypass the 2000 row limit.
        
        Args:
            question_id (int): The question ID for trip points
            trip_id (int, optional): Specific trip ID to filter for
            
        Returns:
            list: Complete list of trip points data
        """
        parameters = []
        if trip_id:
            parameters.append({
                "type": "category",
                "target": ["variable", ["template-tag", "trip_id"]],
                "value": int(trip_id)
            })
        
        # Use the export endpoint to get all data (up to 1,000,000 rows)
        logger.info(f"Attempting to fetch all trip points using export API for trip_id: {trip_id if trip_id else 'ALL'}")
        response_data = self.get_question_data_export(question_id, parameters, format="json")
        
        # If export fails, try regular API as fallback
        if not response_data:
            logger.warning("Export API failed, falling back to regular API (limited to 2000 rows)")
            return self.get_trip_points(question_id, trip_id)
            
        # Process the JSON data into our expected format
        try:
            # The export format is a bit different - it's a list of dictionaries already
            if isinstance(response_data, list):
                if trip_id:
                    # Filter for the specific trip_id if needed
                    results = [point for point in response_data if str(point.get("trip_id")).strip() == str(trip_id).strip()]
                    logger.info(f"Successfully processed {len(results)} trip points from export data (filtered from {len(response_data)} total points)")
                    return results
                else:
                    logger.info(f"Successfully processed {len(response_data)} trip points from export data")
                    return response_data
            
            # If not a list, it might be in the same format as the regular API
            if "data" in response_data:
                rows = response_data["data"]["rows"]
                cols = response_data["data"]["cols"]
                
                # Create a list of dictionaries, each representing a trip point
                results = []
                filtered_count = 0
                
                for row in rows:
                    point = {}
                    for i, col in enumerate(cols):
                        col_name = col.get("display_name") or col.get("name", f"column_{i}")
                        point[col_name] = row[i]
                    
                    # If we're filtering by trip_id, only include matching points
                    if trip_id:
                        db_trip_id = str(point.get("trip_id")).strip() if point.get("trip_id") is not None else None
                        requested_trip_id = str(trip_id).strip()
                        
                        if db_trip_id != requested_trip_id:
                            filtered_count += 1
                            continue
                    
                    results.append(point)
                
                logger.info(f"Successfully processed {len(results)} trip points from export data" + 
                           (f" (filtered {filtered_count} points)" if filtered_count > 0 else ""))
                return results
            
            # Unexpected format
            logger.error(f"Unexpected data format from export: {response_data[:200] if isinstance(response_data, str) else str(response_data)[:200]}...")
            return []
            
        except Exception as e:
            logger.error(f"Error processing Metabase export response: {str(e)}")
            logger.exception("Detailed error:")
            return []
    
    def get_trip_points(self, question_id, trip_id=None):
        """
        Get trip points data for a specific trip ID or all trips.
        
        Args:
            question_id (int): The question ID for trip points
            trip_id (int, optional): Specific trip ID to filter for
            
        Returns:
            list: List of trip points data
        """
        parameters = []
        if trip_id:
            parameters.append({
                "type": "category",
                "target": ["variable", ["template-tag", "trip_id"]],
                "value": int(trip_id)
            })
        
        # First try with the specified parameters
        response_data = self.get_question_data(question_id, parameters)
        
        # If that fails and we were using parameters, try without parameters as fallback
        if (not response_data or "data" not in response_data) and trip_id:
            logger.warning(f"Failed to get data with trip_id parameter. Trying without parameters...")
            response_data = self.get_question_data(question_id, [])
        
        if not response_data or "data" not in response_data:
            logger.error("No data received from Metabase")
            return []
        
        # Process the response into a more usable format
        try:
            rows = response_data["data"]["rows"]
            cols = response_data["data"]["cols"]
            
            # Create a list of dictionaries, each representing a trip point
            results = []
            filtered_count = 0
            
            for row in rows:
                point = {}
                for i, col in enumerate(cols):
                    col_name = col.get("display_name") or col.get("name", f"column_{i}")
                    point[col_name] = row[i]
                
                # If we're filtering by trip_id, only include matching points
                if trip_id and point.get("trip_id") != trip_id:
                    filtered_count += 1
                    continue
                    
                results.append(point)
                print(point)
            
            logger.info(f"Successfully processed {len(results)} trip points" + 
                       (f" (filtered {filtered_count} points)" if filtered_count > 0 else ""))
            return results
        except Exception as e:
            logger.error(f"Error processing Metabase response: {str(e)}")
            return []
    
    @staticmethod
    def calculate_point_match(driver_lat, driver_long, location_lat, location_long):
        """
        Calculate if a driver point matches a location point.
        Returns the status (True, False, or "Unknown") and the reason.
        """
        if location_lat is None or location_long is None:
            return "Unknown", "Location coordinates not available"
        
        if driver_lat is None or driver_long is None:
            return "Unknown", "Driver coordinates not available"
        
        # Simple distance check (could be enhanced with Haversine formula)
        # For now, using a simple threshold based on the given data
        distance_threshold = 0.1  # approximately 100m in decimal degrees
        
        distance = (
            ((driver_lat - location_lat) ** 2) + 
            ((driver_long - location_long) ** 2)
        ) ** 0.5
        
        if distance <= distance_threshold:
            return True, f"Distance: {distance:.6f}"
        else:
            return False, f"Distance: {distance:.6f}"

    def get_question_metadata(self, question_id):
        """
        Get metadata about a specific Metabase question.
        
        Args:
            question_id (int): The numeric ID of the Metabase question
            
        Returns:
            dict: The JSON response containing question metadata or None if the request fails
        """
        if not self.session_token:
            if not self._authenticate():
                return None
        
        # Make sure we're using the numeric ID
        if isinstance(question_id, str) and "-" in question_id:
            question_id = question_id.split("-")[0]
        
        url = f"{self.base_url}/api/card/{question_id}"
        headers = {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.session_token
        }
        
        logger.info(f"Fetching metadata for Metabase question {question_id}")
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            # If we get an unauthorized response, try to re-authenticate
            if response.status_code == 401:
                logger.warning("Session expired, re-authenticating...")
                if self._authenticate():
                    headers["X-Metabase-Session"] = self.session_token
                    response = requests.get(url, headers=headers, timeout=30)
            
            response.raise_for_status()
            
            # Try to parse the response as JSON
            try:
                result = response.json()
                logger.info(f"Successfully retrieved metadata for question {question_id}")
                return result
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON response for question metadata: {response.text[:200]}...")
                return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching metadata for question {question_id}: {str(e)}")
            return None

# Create a singleton instance for easy import
metabase = MetabaseClient()

# For backward compatibility
def get_trip_points_data(trip_id=None):
    """Get trip points data for a specific trip ID or all trips."""
    return metabase.get_all_trip_points(5651, trip_id)

# Import functions from trip_points_helper for convenience
try:
    from trip_points_helper import validate_dropoff_point, is_point_in_city
except ImportError:
    logger.warning("Could not import validation functions from trip_points_helper.")
    
    def validate_dropoff_point(point):
        """Placeholder function if trip_points_helper can't be imported."""
        return "Unknown", "Validation not available"
    
    def is_point_in_city(lon, lat, area_name):
        """Placeholder function if trip_points_helper can't be imported."""
        return False, "Validation not available" 