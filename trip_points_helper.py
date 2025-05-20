"""
Trip Points Helper

This script provides a standalone way to test the Metabase integration
and fetch trip points data.
"""

import requests
import logging
import json
import os
import sys
import math
import time
from datetime import datetime
from functools import lru_cache
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration for Metabase instance
METABASE_URL = "https://data.illa.blue"
USERNAME = "antoon.kamel@illa.com.eg"
PASSWORD = "24621333@Tony"
QUESTION_ID = 5651  # The question ID for trip points

# Path to the GeoJSON file
GEOJSON_PATH = os.path.join('static', 'cities', 'geoBoundaries-EGY-ADM2.geojson')
# Path to the mapping file
MAPPING_PATH = 'egypt_cities.xlsx'

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
                response = requests.post(url, json=query_payload, headers=headers, timeout=45)  # Increased timeout for older data
                
                # If we get an unauthorized response, try to re-authenticate
                if response.status_code == 401:
                    logger.warning("Session expired, re-authenticating...")
                    if self._authenticate():
                        headers["X-Metabase-Session"] = self.session_token
                        response = requests.post(url, json=query_payload, headers=headers, timeout=45)
                
                # Handle 500 server errors specifically
                if response.status_code == 500:
                    logger.error(f"Server error (500) from Metabase question {question_id}")
                    
                    # First retry strategy: Simplify parameters
                    if retries == 0 and parameters:
                        logger.info("Trying with simplified parameters...")
                        retries += 1
                        # Try with empty parameters as fallback
                        return self.get_question_data(question_id, [], max_retries - retries, retry_delay)
                    
                    # Second retry strategy: Increase timeout and retry with delay
                    elif retries < max_retries:
                        retries += 1
                        # Exponential backoff for retry delay to give Metabase more time to recover
                        curr_retry_delay = retry_delay * (2 ** (retries - 1))
                        logger.info(f"Retrying in {curr_retry_delay} seconds... (Attempt {retries}/{max_retries})")
                        time.sleep(curr_retry_delay)
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
                # Exponential backoff for retry delay to give Metabase more time to recover
                curr_retry_delay = retry_delay * (2 ** (retries - 1))
                logger.info(f"Retrying in {curr_retry_delay} seconds... (Attempt {retries}/{max_retries})")
                time.sleep(curr_retry_delay)
            else:
                logger.error(f"Maximum retries ({max_retries}) reached. Giving up.")
                return None
        
        return None
    
    def get_trip_points(self, question_id, trip_id=None, max_retries=3):
        """
        Get trip points data for a specific trip ID or all trips.
        
        Args:
            question_id (int): The question ID for trip points
            trip_id (int, optional): Specific trip ID to filter for
            max_retries (int): Maximum number of retry attempts for alternative approaches
            
        Returns:
            list: List of trip points data
        """
        # Use the get_all_trip_points method to bypass the 2000 row limit
        try:
            logger.info(f"Fetching all trip points for trip_id {trip_id} using export API")
            return self.get_all_trip_points(question_id, trip_id)
        except Exception as e:
            logger.error(f"Error using export API to fetch trip points: {str(e)}")
            logger.info("Falling back to standard API (limited to 2000 rows)")
            # Continue with the original implementation as fallback
            
        # Original implementation starts here (as fallback)
        parameters = []
        if trip_id:
            # Note: It seems the Metabase question might not support trip_id filtering
            # in the way we expected. For now, we'll fetch all points and filter in Python.
            logger.warning(f"Trip ID filtering via API parameters may not work. Will fetch all and filter in Python.")
            # We'll still try with the parameter in case it works
            parameters.append({
                "type": "category",
                "target": ["variable", ["template-tag", "trip_id"]],
                "value": int(trip_id)
            })
        
        # First try with the specified parameters
        response_data = self.get_question_data(question_id, parameters)
        
        # If that fails and we were using parameters, try without parameters as fallback
        if (not response_data or "data" not in response_data or not response_data.get("data", {}).get("rows", [])) and trip_id:
            logger.warning(f"Failed to get data with trip_id parameter. Trying without parameters...")
            response_data = self.get_question_data(question_id, [])
            
            # If we still don't have data, try an alternative approach for older trips
            if not response_data or "data" not in response_data or not response_data.get("data", {}).get("rows", []):
                logger.warning(f"Failed to get data for trip_id {trip_id} with or without parameters. Trying alternative approach...")
                
                # Try with a different date range parameter that might include older data
                logger.info(f"Trying with extended date range for trip_id {trip_id}...")
                extended_params = [
                    {
                        "type": "date/range", 
                        "target": ["variable", ["template-tag", "date_range"]], 
                        "value": "past90days"
                    }
                ]
                
                response_data = self.get_question_data(question_id, extended_params)
                
                # If that fails, try another alternative question ID if available
                if not response_data or "data" not in response_data or not response_data.get("data", {}).get("rows", []):
                    # For legacy data, try a different question
                    alt_question_id = 5652  # Alternative question ID that might contain older data
                    logger.info(f"Trying alternative question {alt_question_id} for older trip data...")
                    # Try without trip_id filter first since that's been causing 500 errors
                    response_data = self.get_question_data(alt_question_id, [])
                    
                    # If we got data but no rows, try one more time with all available data
                    if (not response_data or "data" not in response_data or 
                            not response_data.get("data", {}).get("rows", [])):
                        logger.info("Trying one final approach with raw API access...")
                        # This is a fallback that will try to get all data and filter in memory
                        response_data = self.get_question_data(question_id, [])
        
        if not response_data or "data" not in response_data:
            logger.error("No data received from Metabase")
            return []
        
        # Process the response into a more usable format
        try:
            rows = response_data["data"]["rows"]
            cols = response_data["data"]["cols"]
            
            if not rows:
                logger.warning(f"Empty data set received for trip {trip_id}")
                return []
                
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
                    # Handle both string and integer trip IDs for comparison
                    db_trip_id = str(point.get("trip_id")).strip() if point.get("trip_id") is not None else None
                    requested_trip_id = str(trip_id).strip()
                    
                    if db_trip_id != requested_trip_id:
                        filtered_count += 1
                        continue
                    
                results.append(point)
            
            logger.info(f"Successfully processed {len(results)} trip points" + 
                      (f" (filtered {filtered_count} points)" if filtered_count > 0 else ""))
            return results
        except Exception as e:
            logger.error(f"Error processing Metabase response: {str(e)}")
            return []
    
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
                        return response.json()
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON response from Metabase export: {response.text[:200]}...")
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
            # Continue with regular API implementation below
            # Note: We don't call self.get_trip_points to avoid recursive loop
            parameters = []
            if trip_id:
                parameters.append({
                    "type": "category",
                    "target": ["variable", ["template-tag", "trip_id"]],
                    "value": int(trip_id)
                })
            
            # Use regular API
            response_data = self.get_question_data(question_id, parameters)
            
            if not response_data or "data" not in response_data:
                logger.error("No data received from Metabase")
                return []
            
            # Process the response from regular API
            try:
                rows = response_data["data"]["rows"]
                cols = response_data["data"]["cols"]
                
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
                
                logger.info(f"Successfully processed {len(results)} trip points" + 
                          (f" (filtered {filtered_count} points)" if filtered_count > 0 else ""))
                return results
            except Exception as e:
                logger.error(f"Error processing Metabase response: {str(e)}")
                return []
        
        # Process the JSON data from export API into our expected format
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

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

@lru_cache(maxsize=1)
def load_geojson_boundaries():
    """
    Load the GeoJSON file containing city boundaries for Egypt.
    Uses caching to avoid reloading the file every time.
    
    Returns:
        dict: The loaded GeoJSON data or None if loading fails
    """
    try:
        logger.info(f"Loading GeoJSON boundaries from {GEOJSON_PATH}")
        with open(GEOJSON_PATH, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        logger.info(f"Successfully loaded GeoJSON with {len(geojson_data.get('features', []))} features")
        return geojson_data
    except Exception as e:
        logger.error(f"Error loading GeoJSON boundaries: {str(e)}")
        return None

@lru_cache(maxsize=1)
def load_area_name_mapping():
    """
    Load the mapping between database names and shape names from Excel.
    Uses caching to avoid reloading the file every time.
    
    Returns:
        dict: Mapping from database names to shape names
    """
    try:
        logger.info(f"Loading area mapping from {MAPPING_PATH}")
        df = pd.read_excel(MAPPING_PATH)
        
        # Create mapping dictionaries - only include entries where Shape Name is a valid string
        db_name1_to_shape = {row['Database Name 1'].lower().strip(): row['Shape Name'] 
                           for _, row in df.iterrows() 
                           if pd.notna(row['Database Name 1']) and pd.notna(row['Shape Name'])}
        
        db_name2_to_shape = {row['Database Name 2'].lower().strip(): row['Shape Name'] 
                           for _, row in df.iterrows() 
                           if pd.notna(row['Database Name 2']) and pd.notna(row['Shape Name'])}
        
        # Combine both mappings
        mapping = {**db_name1_to_shape, **db_name2_to_shape}
        
        logger.info(f"Successfully loaded area mapping with {len(mapping)} entries")
        return mapping
    except Exception as e:
        logger.error(f"Error loading area mapping: {str(e)}")
        return {}

def point_in_polygon(point, polygon):
    """
    Check if a point is inside a polygon using the ray casting algorithm.
    
    Args:
        point (tuple): The point as (longitude, latitude)
        polygon (list): List of points that form the polygon, each point as [longitude, latitude]
        
    Returns:
        bool: True if the point is inside the polygon, False otherwise
    """
    x, y = point
    n = len(polygon)
    inside = False
    
    p1x, p1y = polygon[0]
    for i in range(1, n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    
    return inside

def is_point_in_city(lon, lat, area_name):
    """
    Check if a point is within the boundaries of a specified city area.
    
    Args:
        lon (float): Longitude of the point
        lat (float): Latitude of the point
        area_name (str): Name of the city area to check (e.g., 'Al Attarin')
        
    Returns:
        tuple: (is_in_city, details, shape_name) where:
            - is_in_city (bool): True if the point is in the city, False otherwise
            - details (str): Additional details about the match
            - shape_name (str): The name of the matched shape or None if no match
    """
    # Load GeoJSON data
    geojson_data = load_geojson_boundaries()
    if not geojson_data:
        return False, "GeoJSON data could not be loaded", None
    
    # Load area name mapping
    area_mapping = load_area_name_mapping()
    
    # Normalize input area_name for comparison
    db_area_name = area_name.lower().strip() if area_name else ""
    
    # Look up the shape name from the mapping
    shape_name = area_mapping.get(db_area_name)
    
    # Log the mapping process
    if shape_name:
        logger.info(f"Area name mapping: {area_name} -> {shape_name}")
    else:
        # If no mapping found, use the original name
        shape_name = area_name
        logger.warning(f"No mapping found for area name: {area_name}, using original name")
    
    # Make sure shape_name is a string
    if not isinstance(shape_name, str):
        logger.warning(f"Invalid shape name: {shape_name} for area: {area_name}")
        return False, f"Invalid shape name for area '{area_name}'", None
    
    # Find the matching feature for the shape name
    matched_feature = None
    for feature in geojson_data.get('features', []):
        feature_shape_name = feature.get('properties', {}).get('shapeName', '')
        
        # Case-insensitive comparison with the shape name
        if feature_shape_name.lower().strip() == shape_name.lower().strip():
            matched_feature = feature
            logger.info(f"Found matching feature for shape name: {shape_name}")
            break
    
    if not matched_feature:
        logger.warning(f"No boundary found for shape: {shape_name}")
        return False, f"No boundary found for area '{area_name}' (Shape: {shape_name})", shape_name
    
    # Get the coordinates of the matched feature
    geometry = matched_feature.get('geometry', {})
    geometry_type = geometry.get('type', '')
    coordinates = geometry.get('coordinates', [])
    
    # Check if the point is inside any of the polygons
    if geometry_type == 'Polygon':
        # For a single polygon
        outer_ring = coordinates[0]  # First ring is always the outer ring
        if point_in_polygon((lon, lat), outer_ring):
            logger.info(f"Point ({lon}, {lat}) is within {shape_name}")
            return True, f"Point is within {shape_name}", shape_name
        
    elif geometry_type == 'MultiPolygon':
        # For multiple polygons
        for polygon in coordinates:
            outer_ring = polygon[0]  # First ring is always the outer ring
            if point_in_polygon((lon, lat), outer_ring):
                logger.info(f"Point ({lon}, {lat}) is within {shape_name}")
                return True, f"Point is within {shape_name}", shape_name
    
    logger.warning(f"Point ({lon}, {lat}) is not within {shape_name}")
    return False, f"Point is not within {shape_name}", shape_name

def find_actual_city(lon, lat):
    """
    Find the actual city name for a given latitude and longitude by checking
    which city boundary contains the point.
    
    Args:
        lon (float): Longitude of the point
        lat (float): Latitude of the point
        
    Returns:
        str: Name of the city that contains the point, or None if not found
    """
    # Load GeoJSON data
    geojson_data = load_geojson_boundaries()
    if not geojson_data:
        logger.error("GeoJSON data could not be loaded")
        return None
    
    # Check point against each city boundary
    for feature in geojson_data.get('features', []):
        shape_name = feature.get('properties', {}).get('shapeName', '')
        geometry = feature.get('geometry', {})
        geometry_type = geometry.get('type', '')
        coordinates = geometry.get('coordinates', [])
        
        # Check if the point is inside this city's boundary
        is_in_city = False
        
        if geometry_type == 'Polygon':
            # For a single polygon
            outer_ring = coordinates[0]  # First ring is always the outer ring
            if point_in_polygon((lon, lat), outer_ring):
                is_in_city = True
        elif geometry_type == 'MultiPolygon':
            # For multiple polygons
            for polygon in coordinates:
                outer_ring = polygon[0]  # First ring is always the outer ring
                if point_in_polygon((lon, lat), outer_ring):
                    is_in_city = True
                    break
        
        if is_in_city:
            logger.info(f"Point ({lon}, {lat}) is within {shape_name}")
            return shape_name
    
    logger.warning(f"Point ({lon}, {lat}) is not within any known city boundary")
    return None

def calculate_point_match(driver_lat, driver_long, location_lat, location_long):
    """
    Calculate if a driver point matches a location point.
    Returns the status (True, False, or "Unknown") and the reason.
    """
    if location_lat is None or location_long is None:
        return "Unknown", "Location coordinates not available"
    
    if driver_lat is None or driver_long is None:
        return "Unknown", "Driver coordinates not available"
    
    # Calculate distance using Haversine formula
    distance = haversine_distance(driver_lat, driver_long, location_lat, location_long)
    
    # Define a threshold in kilometers (100 meters)
    distance_threshold = 0.1
    
    if distance <= distance_threshold:
        return True, f"Distance: {distance:.2f} km"
    else:
        return False, f"Distance: {distance:.2f} km"

def validate_dropoff_point(point):
    """
    Validate if a dropoff point with no location coordinates is in the correct city.
    
    Args:
        point (dict): The point data
        
    Returns:
        tuple: (is_valid, reason) where:
            - is_valid (bool/str): True if valid, False if invalid, "Unknown" if can't determine
            - reason (str): Reason for the validation result
    """
    # We should only proceed if this is a dropoff point
    if point.get("point_type") != "dropoff":
        return "Unknown", "Not a dropoff point"
    
    # We should only proceed if location coordinates are missing
    if point.get("location_coordinates") or (point.get("location_lat") and point.get("location_long")):
        return "Unknown", "Location coordinates available"
    
    # Get driver coordinates
    driver_lat = point.get("driver_trip_points_lat")
    driver_long = point.get("driver_trip_points_long")
    
    if not driver_lat or not driver_long:
        return "Unknown", "Driver coordinates not available"
    
    # Get the area name
    area_name = point.get("area_name") or point.get("city_name")
    if not area_name:
        return "Unknown", "No area name available"
    
    logger.info(f"Validating dropoff point in area: {area_name}")
    
    # Validate if the driver's position is within the specified area
    is_in_area, details, shape_name = is_point_in_city(float(driver_long), float(driver_lat), area_name)
    
    if is_in_area:
        return True, f"Driver is within {area_name} city boundary: {details}"
    else:
        return False, f"Driver is not within {area_name} city boundary: {details}"

def fetch_and_process_trip_points(trip_id=None):
    """
    Fetch trip points from Metabase and process them.
    
    Args:
        trip_id (int, optional): Specific trip ID to fetch
        
    Returns:
        list: Processed trip points data
    """
    client = MetabaseClient()
    
    # Use get_all_trip_points to fetch all data without the 2000 row limitation
    try:
        logger.info(f"Fetching all trip points for trip_id {trip_id} using export API")
        points = client.get_all_trip_points(QUESTION_ID, trip_id)
    except Exception as e:
        logger.error(f"Error using export API to fetch trip points: {str(e)}")
        logger.info("Falling back to standard API (limited to 2000 rows)")
        # If the export API fails, fall back to the original method
        points = client.get_trip_points(QUESTION_ID, trip_id)
    
    logger.info(f"Processing {len(points)} trip points")
    
    # Track validation statistics
    stats = {
        "total": len(points),
        "area_validation_applied": 0,
        "area_validation_success": 0,
        "area_validation_failure": 0,
        "area_validation_unknown": 0,
        "using_metabase_match": 0,
        "calculated_match": 0,
        "unknown_status": 0
    }
    
    # Process points to add calculated fields
    for point in points:
        point_id = point.get("id", "unknown")
        trip_id = point.get("trip_id", "unknown")
        
        # Add actual city name for the driver's position
        if point.get("driver_trip_points_lat") and point.get("driver_trip_points_long"):
            actual_city = find_actual_city(
                float(point.get("driver_trip_points_long")), 
                float(point.get("driver_trip_points_lat"))
            )
            point["actual_city_name"] = actual_city if actual_city else "Unknown"
            logger.info(f"Determined actual city for point {point_id}: {point['actual_city_name']}")
        else:
            point["actual_city_name"] = "Unknown"
        
        # Use city boundary validation ONLY for dropoff points with empty location coordinates
        if point.get("point_type") == "dropoff" and not point.get("location_coordinates") and not (point.get("location_lat") and point.get("location_long")):
            logger.info(f"Applying area validation for point {point_id} in trip {trip_id}")
            stats["area_validation_applied"] += 1
            
            # Validate if they are in the correct city
            area_valid, reason = validate_dropoff_point(point)
            point["calculated_match"] = area_valid
            point["match_reason"] = reason
            point["calculated_distance_km"] = None
            
            # Log validation result
            if area_valid is True:
                logger.info(f"Area validation SUCCESS for point {point_id}: {reason}")
                stats["area_validation_success"] += 1
            elif area_valid is False:
                logger.warning(f"Area validation FAILURE for point {point_id}: {reason}")
                stats["area_validation_failure"] += 1
            else:
                logger.info(f"Area validation UNKNOWN for point {point_id}: {reason}")
                stats["area_validation_unknown"] += 1
        else:
            # For ALL other points, preserve the original point_match from Metabase
            if "point_match" in point:
                point["calculated_match"] = point.get("point_match")
                point["match_reason"] = "Using Metabase point_match value"
                logger.info(f"Using Metabase point_match for point {point_id}: {point.get('point_match')}")
                stats["using_metabase_match"] += 1
            elif all([
                point.get("driver_trip_points_lat"),
                point.get("driver_trip_points_long"),
                point.get("location_lat"),
                point.get("location_long")
            ]):
                # Only calculate match if no point_match exists from Metabase
                match_status, reason = calculate_point_match(
                    point.get("driver_trip_points_lat"),
                    point.get("driver_trip_points_long"),
                    point.get("location_lat"),
                    point.get("location_long")
                )
                point["calculated_match"] = match_status
                point["match_reason"] = reason
                logger.info(f"Calculated match for point {point_id}: {match_status}, {reason}")
                stats["calculated_match"] += 1
                
                # Calculate distance in km
                point["calculated_distance_km"] = haversine_distance(
                    point.get("driver_trip_points_lat"),
                    point.get("driver_trip_points_long"),
                    point.get("location_lat"),
                    point.get("location_long")
                )
            else:
                point["calculated_match"] = "Unknown"
                point["match_reason"] = "Missing coordinates"
                point["calculated_distance_km"] = None
                logger.info(f"Unknown match status for point {point_id}: Missing coordinates")
                stats["unknown_status"] += 1
    
    # Log overall statistics
    logger.info(f"Trip points processing statistics:")
    logger.info(f"  Total points: {stats['total']}")
    logger.info(f"  Area validation applied: {stats['area_validation_applied']}")
    logger.info(f"  Area validation success: {stats['area_validation_success']}")
    logger.info(f"  Area validation failure: {stats['area_validation_failure']}")
    logger.info(f"  Area validation unknown: {stats['area_validation_unknown']}")
    logger.info(f"  Using Metabase match: {stats['using_metabase_match']}")
    logger.info(f"  Calculated match: {stats['calculated_match']}")
    logger.info(f"  Unknown status: {stats['unknown_status']}")
    
    return points

def print_trip_points_stats(points):
    """Print statistics about the trip points."""
    if not points:
        print("No trip points found.")
        return
    
    total = len(points)
    matching = sum(1 for p in points if p.get("calculated_match") is True)
    non_matching = sum(1 for p in points if p.get("calculated_match") is False)
    unknown = sum(1 for p in points if p.get("calculated_match") == "Unknown")
    
    print(f"Total points: {total}")
    print(f"Matching points: {matching} ({matching/total*100:.1f}%)")
    print(f"Non-matching points: {non_matching} ({non_matching/total*100:.1f}%)")
    print(f"Unknown status: {unknown} ({unknown/total*100:.1f}%)")
    
    # Print some sample points
    print("\nSample points:")
    for i, point in enumerate(points[:5]):
        print(f"Point {i+1}:")
        print(f"  Driver: {point.get('driver_name')}")
        print(f"  Trip ID: {point.get('trip_id')}")
        print(f"  Point Type: {point.get('point_type')}")
        print(f"  Action Type: {point.get('action_type')}")
        print(f"  Driver Coordinates: {point.get('driver_coordinates')}")
        print(f"  Location Coordinates: {point.get('location_coordinates')}")
        print(f"  Match Status: {point.get('calculated_match')}")
        print(f"  Match Reason: {point.get('match_reason')}")
        print(f"  Distance: {point.get('calculated_distance_km'):.2f} km" if point.get('calculated_distance_km') is not None else "  Distance: N/A")
        print()

def calculate_trip_points_stats(trip_id):
    """
    Calculate trip points statistics for a specific trip.
    
    Args:
        trip_id (int): The trip ID to analyze
        
    Returns:
        dict: A dictionary containing statistics about pickup and dropoff points
    """
    try:
        logger.info(f"Calculating trip points stats for trip ID: {trip_id}")
        
        # Fetch all trip points for this trip
        points = fetch_and_process_trip_points(trip_id)
        
        if not points:
            logger.warning(f"Failed to get trip points stats for trip {trip_id}: No trip points found")
            # Provide a more helpful error message for old trips
            return {
                "status": "error",
                "message": "No trip points found for this trip. This may be because the trip is too old (over a week) or the data is not available in Metabase.",
                "pickup_success_rate": 0,
                "dropoff_success_rate": 0,
                "total_success_rate": 0,
                "total_points": 0,
                "pickup_points": 0,
                "dropoff_points": 0,
                "pickup_correct": 0,
                "dropoff_correct": 0
            }
        
        # Initialize counters
        stats = {
            "total_points": len(points),
            "pickup_points": 0,
            "dropoff_points": 0,
            "pickup_correct": 0,
            "dropoff_correct": 0
        }
        
        # Process each point
        for point in points:
            # Get point type and match status
            point_type = point.get("point_type")
            # Use existing calculated_match which already handles city validation
            match_status = point.get("calculated_match")
            
            # Count points by type
            if point_type == "pickup":
                stats["pickup_points"] += 1
                if match_status is True:
                    stats["pickup_correct"] += 1
            elif point_type == "dropoff":
                stats["dropoff_points"] += 1
                if match_status is True:
                    stats["dropoff_correct"] += 1
        
        # Calculate percentages
        stats["pickup_success_rate"] = round((stats["pickup_correct"] / stats["pickup_points"] * 100) if stats["pickup_points"] > 0 else 0, 2)
        stats["dropoff_success_rate"] = round((stats["dropoff_correct"] / stats["dropoff_points"] * 100) if stats["dropoff_points"] > 0 else 0, 2)
        stats["total_success_rate"] = round(((stats["pickup_correct"] + stats["dropoff_correct"]) / stats["total_points"] * 100) if stats["total_points"] > 0 else 0, 2)
        stats["status"] = "success"
        
        logger.info(f"Successfully calculated stats for trip {trip_id}: {stats['total_points']} points found")
        return stats
    except Exception as e:
        logger.error(f"Error calculating trip points stats: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "message": f"Error processing trip points: {str(e)}",
            "pickup_success_rate": 0,
            "dropoff_success_rate": 0,
            "total_success_rate": 0,
            "total_points": 0,
            "pickup_points": 0,
            "dropoff_points": 0,
            "pickup_correct": 0,
            "dropoff_correct": 0
        }

def main():
    """Main function to run the script."""
    # Check if a trip ID was provided as a command line argument
    trip_id = None
    if len(sys.argv) > 1:
        try:
            trip_id = int(sys.argv[1])
        except ValueError:
            print(f"Invalid trip ID: {sys.argv[1]}. Must be an integer.")
            sys.exit(1)
    
    # Fetch and process trip points
    print(f"Fetching trip points for {'trip ID ' + str(trip_id) if trip_id else 'all trips'}...")
    points = fetch_and_process_trip_points(trip_id)
    
    # Print statistics
    print_trip_points_stats(points)
    
    # Save to JSON file
    filename = f"trip_points_{trip_id if trip_id else 'all'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w') as f:
        json.dump(points, f, indent=2)
    
    print(f"Saved {len(points)} points to {filename}")

if __name__ == "__main__":
    main() 