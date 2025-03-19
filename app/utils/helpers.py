import math
import requests
from flask import session as flask_session
import openpyxl
from db.config import BASE_API_URL, API_EMAIL, API_PASSWORD

# Helper function for calculating haversine distance between two coordinates
def haversine_distance(coord1, coord2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    
    Args:
        coord1: tuple or list with (lat, lon)
        coord2: tuple or list with (lat, lon)
        
    Returns:
        Distance in kilometers
    """
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

# Function to analyze trip segments and distances
def analyze_trip_segments(coordinates):
    """
    Analyze coordinates to calculate distance metrics:
    - Count and total distance of short segments (<1km)
    - Count and total distance of medium segments (1-5km)
    - Count and total distance of long segments (>5km)
    - Maximum segment distance
    - Average segment distance
    
    Args:
        coordinates: list of [lon, lat] points from API
        
    Returns:
        Dictionary with analysis metrics
    """
    if not coordinates or len(coordinates) < 2:
        return {
            "short_segments_count": 0,
            "medium_segments_count": 0,
            "long_segments_count": 0,
            "short_segments_distance": 0,
            "medium_segments_distance": 0,
            "long_segments_distance": 0,
            "max_segment_distance": 0,
            "avg_segment_distance": 0
        }
    
    # Note: API returns coordinates as [lon, lat], so we need to swap
    # Let's convert to [lat, lon] for calculations
    coords = [[float(point[1]), float(point[0])] for point in coordinates]
    
    short_segments_count = 0
    medium_segments_count = 0
    long_segments_count = 0
    short_segments_distance = 0
    medium_segments_distance = 0
    long_segments_distance = 0
    max_segment_distance = 0
    total_distance = 0
    segment_count = 0
    
    for i in range(len(coords) - 1):
        distance = haversine_distance(coords[i], coords[i+1])
        segment_count += 1
        total_distance += distance
        
        if distance < 1:
            short_segments_count += 1
            short_segments_distance += distance
        elif distance <= 5:
            medium_segments_count += 1
            medium_segments_distance += distance
        else:
            long_segments_count += 1
            long_segments_distance += distance
            
        if distance > max_segment_distance:
            max_segment_distance = distance
            
    avg_segment_distance = total_distance / segment_count if segment_count > 0 else 0
    
    return {
        "short_segments_count": short_segments_count,
        "medium_segments_count": medium_segments_count,
        "long_segments_count": long_segments_count,
        "short_segments_distance": round(short_segments_distance, 2),
        "medium_segments_distance": round(medium_segments_distance, 2),
        "long_segments_distance": round(long_segments_distance, 2),
        "max_segment_distance": round(max_segment_distance, 2),
        "avg_segment_distance": round(avg_segment_distance, 2)
    }

# Session management for filters
def get_saved_filters():
    return flask_session.get("saved_filters", {})

def save_filter_to_session(name, filters):
    saved = flask_session.get("saved_filters", {})
    saved[name] = filters
    flask_session["saved_filters"] = saved

# API Authentication functions
def fetch_api_token():
    url = f"{BASE_API_URL}/auth/sign_in"
    payload = {"admin_user": {"email": API_EMAIL, "password": API_PASSWORD}}
    resp = requests.post(url, json=payload)
    if resp.status_code == 200:
        return resp.json().get("token", None)
    else:
        print("Error fetching primary token:", resp.text)
        return None

def fetch_api_token_alternative():
    alt_email = "SupplyPartner@illa.com.eg"
    alt_password = "654321"
    url = f"{BASE_API_URL}/auth/sign_in"
    payload = {"admin_user": {"email": alt_email, "password": alt_password}}
    try:
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        return resp.json().get("token", None)
    except Exception as e:
        print("Error fetching alternative token:", e)
        return None

# Excel data loading
def load_excel_data(excel_path):
    workbook = openpyxl.load_workbook(excel_path)
    sheet = workbook.active
    headers = []
    data = []
    for i, row in enumerate(sheet.iter_rows(values_only=True)):
        if i == 0:
            headers = row
        else:
            row_dict = {headers[j]: row[j] for j in range(len(row))}
            data.append(row_dict)
    print(f"Loaded {len(data)} rows from Excel.")
    return data

# Carrier normalization
CARRIER_GROUPS = {
    "Vodafone": ["vodafone", "voda fone", "tegi ne3eesh"],
    "Orange": ["orange", "orangeeg", "orange eg"],
    "Etisalat": ["etisalat", "e& etisalat", "e&"],
    "We": ["we"]
}

def normalize_carrier(carrier_name):
    if not carrier_name:
        return ""
    lower = carrier_name.lower().strip()
    for group, variants in CARRIER_GROUPS.items():
        for variant in variants:
            if variant in lower:
                return group
    return carrier_name.title() 