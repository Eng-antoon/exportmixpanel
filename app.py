import os
import io
import requests
import openpyxl
from openpyxl import Workbook
from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    redirect,
    url_for,
    flash,
    send_file,
    session as flask_session
)
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from datetime import datetime
import shutil
import subprocess
from collections import defaultdict, Counter
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

from db.config import DB_URI, API_TOKEN, BASE_API_URL, API_EMAIL, API_PASSWORD
from db.models import Base, Trip, Tag

app = Flask(__name__)
engine = create_engine(DB_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
update_jobs = {}
executor = ThreadPoolExecutor(max_workers=40)
app.secret_key = "your_secret_key"  # for flashing and session

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

def calculate_expected_trip_quality(logs_count, lack_of_accuracy, medium_segments_count, long_segments_count, short_dist_total, medium_dist_total, long_dist_total):
    """
    Calculate the Expected Trip Quality based on provided metrics.

    Criteria:
      - "No Logs Trip":
            Logs Count < 5
            AND Medium Segments == 0
            AND Long Segments == 0.
      - "Trip Points Only Exist":
            Logs Count < 50
            AND (Medium Segments > 0 OR Long Segments > 0).
      - "High Quality Trip":
            Logs Count > 500,
            AND Long Segments == 0,
            AND the ratio R = Short Dist Total / (Medium Dist Total + Long Dist Total + ε) is >= 5.
      - "Moderate Quality Trip":
            If there is at least one medium or long segment and either:
                • 0.5 ≤ R < 5, OR
                • R < 0.5 but the absolute difference between Short Dist Total and (Medium + Long Dist Total)
                  is within 50% of (Medium + Long Dist Total).
      - "Low Quality Trip":
            Otherwise.

    After computing the quality, if Lack of Accuracy is True,
    downgrade the quality by one level:
         High → Moderate, Moderate → Low, Trip Points Only Exist → No Logs Trip.
         (If already "No Logs Trip" or "Low Quality Trip", the quality remains unchanged.)

    Parameters:
      logs_count (int): Number of logs recorded during the trip.
      lack_of_accuracy (bool): True if GPS was inaccurate.
      medium_segments_count (int): Count of segments between 1–5 km.
      long_segments_count (int): Count of segments > 5 km.
      short_dist_total (float): Total distance for segments < 1 km.
      medium_dist_total (float): Total distance for segments 1–5 km.
      long_dist_total (float): Total distance for segments > 5 km.
      
    Returns:
      str: The expected trip quality category.
    """
    epsilon = 0.01  # Small constant to avoid division by zero
    ratio = short_dist_total / (medium_dist_total + long_dist_total + epsilon)

    # Determine base quality
    if logs_count < 5 and medium_segments_count == 0 and long_segments_count == 0:
        quality = "No Logs Trip"
    elif logs_count < 50 and (medium_segments_count > 0 or long_segments_count > 0):
        quality = "Trip Points Only Exist"
    elif logs_count > 500 and long_segments_count == 0 and ratio >= 5:
        quality = "High Quality Trip"
    else:
        combined_distance = medium_dist_total + long_dist_total
        if (0.5 <= ratio < 5) or (ratio < 0.5 and abs(short_dist_total - combined_distance) / (combined_distance + epsilon) <= 0.5):
            quality = "Moderate Quality Trip"
        else:
            quality = "Low Quality Trip"
    
    # Downgrade quality by one level if Lack of Accuracy is True.
    if lack_of_accuracy:
        downgrade = {
            "High Quality Trip": "Moderate Quality Trip",
            "Moderate Quality Trip": "Low Quality Trip",
            "Trip Points Only Exist": "No Logs Trip"
        }
        quality = downgrade.get(quality, quality)
    
    return quality




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


# --- Begin Migration to update schema with new columns ---
def migrate_db():
    try:
        print("Creating database tables from models...")
        Base.metadata.create_all(bind=engine)
        print("Database tables created successfully")
    except Exception as e:
        app.logger.error(f"Migration error: {e}")
        print(f"Error during database migration: {e}")

print("Running database migration...")
migrate_db()
print("Database migration completed")
# --- End Migration ---

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

# ---------------------------
# Utility Functions
# ---------------------------

def get_saved_filters():
    return flask_session.get("saved_filters", {})

def save_filter_to_session(name, filters):
    saved = flask_session.get("saved_filters", {})
    saved[name] = filters
    flask_session["saved_filters"] = saved

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

def load_excel_data(excel_path):
    if not os.path.exists(excel_path):
        print(f"Excel file not found: {excel_path}. Returning empty data.")
        return []
    try:
        workbook = openpyxl.load_workbook(excel_path)
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return []
    
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


# Carrier grouping
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

# NEW FUNCTION: determine_completed_by
# This function inspects an activity list to find the latest event where the status changes to 'completed'
# and returns the corresponding user_type (admin or driver), or None if not found.
def determine_completed_by(activity_list):
    best_candidate = None
    best_time = None
    for event in activity_list:
        changes = event.get("changes", {})
        status_change = changes.get("status")
        if status_change and isinstance(status_change, list) and len(status_change) >= 2:
            if str(status_change[-1]).lower() == "completed":
                created_str = event.get("created_at", "").replace(" UTC", "")
                event_time = None
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]:
                    try:
                        event_time = datetime.strptime(created_str, fmt)
                        break
                    except ValueError:
                        continue
                if event_time:
                    if best_time is None or event_time > best_time:
                        best_time = event_time
                        best_candidate = event
    if best_candidate:
        return best_candidate.get("user_type", None)
    return None

# This function calculates the trip time (in hours) based on the time difference
# between the first arrival event and the completion event from the activity list
def calculate_trip_time(activity_list):
    arrival_time = None
    completion_time = None
    
    # Find first arrival time (status changes from pending to arrived)
    for event in activity_list:
        changes = event.get("changes", {})
        status_change = changes.get("status")
        if status_change and isinstance(status_change, list) and len(status_change) >= 2:
            if str(status_change[0]).lower() == "pending" and str(status_change[1]).lower() == "arrived":
                created_str = event.get("created_at", "").replace(" UTC", "")
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]:
                    try:
                        arrival_time = datetime.strptime(created_str, fmt)
                        break
                    except ValueError:
                        continue
                if arrival_time:
                    break  # Found the first arrival time, so stop looking
    
    # Find completion time (status changes to completed)
    for event in activity_list:
        changes = event.get("changes", {})
        status_change = changes.get("status")
        if status_change and isinstance(status_change, list) and len(status_change) >= 2:
            if str(status_change[1]).lower() == "completed":
                created_str = event.get("created_at", "").replace(" UTC", "")
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]:
                    try:
                        completion_time = datetime.strptime(created_str, fmt)
                        break
                    except ValueError:
                        continue
    
    # Calculate trip time in hours if both times were found
    if arrival_time and completion_time:
        time_diff = completion_time - arrival_time
        hours = time_diff.total_seconds() / 3600.0
        return round(hours, 2)  # Round to 2 decimal places
    
    return None

def fetch_coordinates_count(trip_id, token=API_TOKEN):
    url = f"{BASE_API_URL}/trips/{trip_id}/coordinates"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        # Return the 'count' from the attributes; default to 0 if not found
        return data["data"]["attributes"].get("count", 0)
    except Exception as e:
        print(f"Error fetching coordinates for trip {trip_id}: {e}")
        return None

def fetch_trip_from_api(trip_id, token=API_TOKEN):
    url = f"{BASE_API_URL}/trips/{trip_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        calc = data.get("data", {}).get("attributes", {}).get("calculatedDistance")
        if not calc or calc in [None, "", "N/A"]:
            raise ValueError("Missing calculatedDistance")
        return data
    except Exception as e:
        print("Error fetching trip data with primary token:", e)
        alt_token = fetch_api_token_alternative()
        if alt_token:
            headers = {"Authorization": f"Bearer {alt_token}", "Content-Type": "application/json"}
            try:
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                data["used_alternative"] = True
                return data
            except requests.HTTPError as http_err:
                if resp.status_code == 404:
                    print(f"Trip {trip_id} not found with alternative token (404).")
                else:
                    print(f"HTTP error with alternative token for trip {trip_id}: {http_err}")
            except Exception as e:
                print(f"Alternative fetch failed for trip {trip_id}: {e}")
        else:
            return None

def update_trip_db(trip_id, force_update=False):
    session_local = db_session()
    # Flags to ensure alternative is only tried once
    tried_alternative_for_main = False
    tried_alternative_for_coordinate = False
    
    # Track what was updated for better reporting
    update_status = {
        "needed_update": False,
        "record_exists": False,
        "updated_fields": [],
        "reason_for_update": []
    }

    # Helper to validate field values
    def is_valid(value):
        return value is not None and str(value).strip() != "" and str(value).strip().upper() != "N/A"

    try:
        # Step 1: Check if trip exists and what fields need updating
        db_trip = session_local.query(Trip).filter_by(trip_id=trip_id).first()
        
        if db_trip:
            update_status["record_exists"] = True
            
            # If we're forcing an update, don't bother checking what's missing
            if force_update:
                update_status["needed_update"] = True
                update_status["reason_for_update"].append("Forced update")
            else:
                # Otherwise, check each field to see what needs updating
                missing_fields = []
                
                # Check manual_distance
                if not is_valid(db_trip.manual_distance):
                    missing_fields.append("manual_distance")
                    update_status["reason_for_update"].append("Missing manual_distance")
                
                # Check calculated_distance
                if not is_valid(db_trip.calculated_distance):
                    missing_fields.append("calculated_distance")
                    update_status["reason_for_update"].append("Missing calculated_distance")
                
                # Check trip_time
                if not is_valid(db_trip.trip_time):
                    missing_fields.append("trip_time")
                    update_status["reason_for_update"].append("Missing trip_time")
                
                # Check completed_by
                if not is_valid(db_trip.completed_by):
                    missing_fields.append("completed_by")
                    update_status["reason_for_update"].append("Missing completed_by")
                
                # Check coordinate_count
                if not is_valid(db_trip.coordinate_count):
                    missing_fields.append("coordinate_count")
                    update_status["reason_for_update"].append("Missing coordinate_count")
                
                # Check lack_of_accuracy (boolean should be explicitly set)
                if db_trip.lack_of_accuracy is None:
                    missing_fields.append("lack_of_accuracy")
                    update_status["reason_for_update"].append("Missing lack_of_accuracy")
                
                # Check segment counts
                if not is_valid(db_trip.short_segments_count):
                    missing_fields.append("segment_counts")
                    update_status["reason_for_update"].append("Missing segment counts")
                elif not is_valid(db_trip.medium_segments_count):
                    missing_fields.append("segment_counts")
                    update_status["reason_for_update"].append("Missing segment counts")
                elif not is_valid(db_trip.long_segments_count):
                    missing_fields.append("segment_counts")
                    update_status["reason_for_update"].append("Missing segment counts")
                
                # If no missing fields, return the trip without further API calls
                if not missing_fields:
                    return db_trip, update_status
                
                # Mark that this record needs update
                update_status["needed_update"] = True
        else:
            # Trip doesn't exist, so we'll create it
            update_status["needed_update"] = True
            update_status["reason_for_update"].append("New record")
            # Create an empty trip record that we'll populate later
            db_trip = Trip(trip_id=trip_id)
            session_local.add(db_trip)
            # Add all fields to missing_fields to ensure we fetch everything
            missing_fields = ["manual_distance", "calculated_distance", "trip_time", 
                             "completed_by", "coordinate_count", "lack_of_accuracy", 
                             "segment_counts"]
        
        # Step 2: Only proceed with API calls if the trip needs updating
        if update_status["needed_update"] or force_update:
            
            # Determine what API calls we need to make based on missing fields
            need_main_data = force_update or any(field in missing_fields for field 
                                                 in ["manual_distance", "calculated_distance", 
                                                     "trip_time", "completed_by", "lack_of_accuracy"])
            
            need_coordinates = force_update or "coordinate_count" in missing_fields
            
            need_segments = force_update or "segment_counts" in missing_fields
            
            # Step 2a: Fetch main trip data if needed
            if need_main_data:
                api_data = fetch_trip_from_api(trip_id)
                
                # If initial fetch fails, try alternative token
                if not (api_data and "data" in api_data):
                    if not tried_alternative_for_main:
                        tried_alternative_for_main = True
                        alt_token = fetch_api_token_alternative()
                        if alt_token:
                            headers = {"Authorization": f"Bearer {alt_token}", "Content-Type": "application/json"}
                            url = f"{BASE_API_URL}/trips/{trip_id}"
                            try:
                                resp = requests.get(url, headers=headers)
                                resp.raise_for_status()
                                api_data = resp.json()
                                api_data["used_alternative"] = True
                            except requests.HTTPError as http_err:
                                if resp.status_code == 404:
                                    print(f"Trip {trip_id} not found with alternative token (404).")
                                else:
                                    print(f"HTTP error with alternative token for trip {trip_id}: {http_err}")
                            except Exception as e:
                                print(f"Alternative fetch failed for trip {trip_id}: {e}")
                
                # Process the trip data if we got it
                if api_data and "data" in api_data:
                    trip_attributes = api_data["data"]["attributes"]
                    
                    # Update status regardless of what fields need updating
                    old_status = db_trip.status
                    db_trip.status = trip_attributes.get("status")
                    if db_trip.status != old_status:
                        update_status["updated_fields"].append("status")
                    
                    # Update manual_distance if needed
                    if force_update or "manual_distance" in missing_fields:
                        try:
                            old_value = db_trip.manual_distance
                            db_trip.manual_distance = float(trip_attributes.get("manualDistance") or 0)
                            if db_trip.manual_distance != old_value:
                                update_status["updated_fields"].append("manual_distance")
                        except ValueError:
                            db_trip.manual_distance = None
                    
                    # Update calculated_distance if needed
                    if force_update or "calculated_distance" in missing_fields:
                        try:
                            old_value = db_trip.calculated_distance
                            db_trip.calculated_distance = float(trip_attributes.get("calculatedDistance") or 0)
                            if db_trip.calculated_distance != old_value:
                                update_status["updated_fields"].append("calculated_distance")
                        except ValueError:
                            db_trip.calculated_distance = None
                    
                    # Mark supply partner if needed
                    if api_data.get("used_alternative"):
                        db_trip.supply_partner = True
                    
                    # Process trip_time only if missing or force_update
                    if force_update or "trip_time" in missing_fields:
                        activity_list = trip_attributes.get("activity", [])
                        trip_time = calculate_trip_time(activity_list)
                        
                        if trip_time is not None:
                            old_value = db_trip.trip_time
                            db_trip.trip_time = trip_time
                            if db_trip.trip_time != old_value:
                                update_status["updated_fields"].append("trip_time")
                                app.logger.info(f"Trip {trip_id}: trip_time updated to {trip_time} hours based on activity events")
                    
                    # Determine completed_by if missing or force_update
                    if force_update or "completed_by" in missing_fields:
                        comp_by = determine_completed_by(trip_attributes.get("activity", []))
                        if comp_by is not None:
                            old_value = db_trip.completed_by
                            db_trip.completed_by = comp_by
                            if db_trip.completed_by != old_value:
                                update_status["updated_fields"].append("completed_by")
                            app.logger.info(f"Trip {trip_id}: completed_by set to {db_trip.completed_by} based on activity events")
                        else:
                            db_trip.completed_by = None
                            app.logger.info(f"Trip {trip_id}: No completion event found, completed_by remains None")
                    
                    # Update lack_of_accuracy if missing or force_update
                    if force_update or "lack_of_accuracy" in missing_fields:
                        old_value = db_trip.lack_of_accuracy
                        tags_count = api_data["data"]["attributes"].get("tagsCount", [])
                        if isinstance(tags_count, list) and any(item.get("tag_name") == "lack_of_accuracy" and int(item.get("count", 0)) > 0 for item in tags_count):
                            db_trip.lack_of_accuracy = True
                        else:
                            db_trip.lack_of_accuracy = False
                        if db_trip.lack_of_accuracy != old_value:
                            update_status["updated_fields"].append("lack_of_accuracy")
            
            # Step 2b: Fetch coordinate count if needed
            if need_coordinates:
                coordinate_count = fetch_coordinates_count(trip_id)
                
                # Try alternative token if needed
                if not is_valid(coordinate_count) and not tried_alternative_for_coordinate:
                    tried_alternative_for_coordinate = True
                    alt_token = fetch_api_token_alternative()
                    if alt_token:
                        coordinate_count = fetch_coordinates_count(trip_id, token=alt_token)
                
                # Update the coordinate count if it changed
                if coordinate_count != db_trip.coordinate_count:
                    db_trip.coordinate_count = coordinate_count
                    update_status["updated_fields"].append("coordinate_count")
            
            # Step 2c: Fetch segment analysis if needed
            if need_segments:
                # Fetch coordinates
                url = f"{BASE_API_URL}/trips/{trip_id}/coordinates"
                token = fetch_api_token() or API_TOKEN
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                
                try:
                    resp = requests.get(url, headers=headers)
                    # If unauthorized, try alternative token
                    if resp.status_code == 401:
                        alt_token = fetch_api_token_alternative()
                        if alt_token:
                            headers["Authorization"] = f"Bearer {alt_token}"
                            resp = requests.get(url, headers=headers)
                    
                    resp.raise_for_status()
                    coordinates_data = resp.json()
                    
                    if coordinates_data and "data" in coordinates_data and "attributes" in coordinates_data["data"]:
                        coordinates = coordinates_data["data"]["attributes"].get("coordinates", [])
                        
                        # Calculate distance metrics
                        if coordinates and len(coordinates) >= 2:
                            analysis = analyze_trip_segments(coordinates)
                            
                            # Check if any segment metrics have changed
                            segments_changed = False
                            for key, value in analysis.items():
                                if getattr(db_trip, key, None) != value:
                                    segments_changed = True
                                    break
                                    
                            # Update trip with analysis results
                            db_trip.short_segments_count = analysis["short_segments_count"]
                            db_trip.medium_segments_count = analysis["medium_segments_count"]
                            db_trip.long_segments_count = analysis["long_segments_count"]
                            db_trip.short_segments_distance = analysis["short_segments_distance"]
                            db_trip.medium_segments_distance = analysis["medium_segments_distance"]
                            db_trip.long_segments_distance = analysis["long_segments_distance"]
                            db_trip.max_segment_distance = analysis["max_segment_distance"]
                            db_trip.avg_segment_distance = analysis["avg_segment_distance"]
                            
                            if segments_changed:
                                update_status["updated_fields"].append("segment_metrics")
                                
                            app.logger.info(f"Trip {trip_id}: Updated distance analysis metrics")
                            
                            # NEW: Compute Expected Trip Quality based on our criteria.
                            expected_quality = calculate_expected_trip_quality(
                                logs_count = db_trip.coordinate_count if db_trip.coordinate_count is not None else 0,
                                lack_of_accuracy = db_trip.lack_of_accuracy if db_trip.lack_of_accuracy is not None else False,
                                medium_segments_count = db_trip.medium_segments_count if db_trip.medium_segments_count is not None else 0,
                                long_segments_count = db_trip.long_segments_count if db_trip.long_segments_count is not None else 0,
                                short_dist_total = db_trip.short_segments_distance if db_trip.short_segments_distance is not None else 0.0,
                                medium_dist_total = db_trip.medium_segments_distance if db_trip.medium_segments_distance is not None else 0.0,
                                long_dist_total = db_trip.long_segments_distance if db_trip.long_segments_distance is not None else 0.0
                            )
                            if db_trip.expected_trip_quality != expected_quality:
                                db_trip.expected_trip_quality = expected_quality
                                update_status["updated_fields"].append("expected_trip_quality")
                            app.logger.info(f"Trip {trip_id}: Expected Trip Quality updated to '{expected_quality}'")
                except Exception as e:
                    app.logger.error(f"Error fetching coordinates for trip {trip_id}: {e}")
            
            # If we made any updates, commit them
            if update_status["updated_fields"]:
                session_local.commit()
                session_local.refresh(db_trip)
            
        return db_trip, update_status
    except Exception as e:
        print("Error in update_trip_db:", e)
        session_local.rollback()
        db_trip = session_local.query(Trip).filter_by(trip_id=trip_id).first()
        return db_trip, {"error": str(e)}
    finally:
        session_local.close()


# ---------------------------
# Routes 
# ---------------------------

@app.route("/update_db", methods=["POST"])
def update_db():
    """
    Bulk update DB from Excel (fetch each trip from the API) with improved performance.
    Only fetches data for trips that are missing critical fields or where force_update is True.
    """
    session_local = db_session()
    excel_path = os.path.join("data", "data.xlsx")
    excel_data = load_excel_data(excel_path)
    
    # Track statistics
    stats = {
        "total": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "created": 0,
        "updated_fields": Counter(),  # Count which fields were updated most often
        "reasons": Counter()          # Count reasons for updates
    }
    
    # Get all trip IDs from Excel
    trip_ids = [row.get("tripId") for row in excel_data if row.get("tripId")]
    stats["total"] = len(trip_ids)
    
    # Process trips with feedback
    for trip_id in trip_ids:
        # False means don't force updates if all fields are present
        db_trip, update_status = update_trip_db(trip_id, force_update=False)
        
        # Track statistics
        if "error" in update_status:
            stats["errors"] += 1
            continue
            
        if not update_status["record_exists"]:
            stats["created"] += 1
            stats["updated"] += 1
        elif update_status["updated_fields"]:
            stats["updated"] += 1
            # Count which fields were updated
            for field in update_status["updated_fields"]:
                stats["updated_fields"][field] += 1
        else:
            stats["skipped"] += 1
            
        # Track reasons for updates
        for reason in update_status["reason_for_update"]:
            stats["reasons"][reason] += 1
    
    session_local.close()
    
    # Prepare detailed feedback message
    if stats["updated"] > 0:
        message = f"Updated {stats['updated']} out of {stats['total']} trips. "
        message += f"Created {stats['created']} new records. "
        message += f"Skipped {stats['skipped']} complete records. "
        if stats["errors"] > 0:
            message += f"Encountered {stats['errors']} errors. "
            
        # Add details about which fields were updated most often
        if stats["updated_fields"]:
            top_fields = stats["updated_fields"].most_common(3)
            field_msg = ", ".join([f"{field} ({count})" for field, count in top_fields])
            message += f"Most updated fields: {field_msg}. "
            
        # Add details about common reasons for updates
        if stats["reasons"]:
            top_reasons = stats["reasons"].most_common(3)
            reason_msg = ", ".join([f"{reason} ({count})" for reason, count in top_reasons])
            message += f"Top reasons for updates: {reason_msg}."
            
        flash(message, "success")
    else:
        flash(f"All {stats['total']} trips are up to date! No updates needed.", "info")
        
    return redirect(url_for("trips"))

@app.route("/export_trips")
def export_trips():
    """
    Export filtered trips to XLSX, merging with DB data (including trip_time, completed_by,
    coordinate_count (log count), status, route_quality, and lack_of_accuracy). Supports both operator-based filtering 
    and new range filtering for trip_time and log_count.
    """
    session_local = db_session()
    # Basic filters from the request (note: route_quality filtering will be applied after merging)
    filters = {
        "driver": request.args.get("driver"),
        "trip_id": request.args.get("trip_id"),
        "model": request.args.get("model"),
        "ram": request.args.get("ram"),
        "carrier": request.args.get("carrier"),
        "variance_min": request.args.get("variance_min"),
        "variance_max": request.args.get("variance_max"),
        "export_name": request.args.get("export_name", "exported_trips"),
        "route_quality": request.args.get("route_quality", "").strip(),
        "trip_issues": request.args.get("trip_issues", "").strip(),
        "lack_of_accuracy": request.args.get("lack_of_accuracy", "").strip(),
        "tags": request.args.get("tags", "").strip()
    }
    # New filter parameters with operator strings (in English)
    trip_time = request.args.get("trip_time", "").strip()
    trip_time_op = request.args.get("trip_time_op", "equal").strip()
    completed_by_filter = request.args.get("completed_by", "").strip()
    log_count = request.args.get("log_count", "").strip()
    log_count_op = request.args.get("log_count_op", "equal").strip()
    status_filter = request.args.get("status", "").strip()
    
    # New range filter parameters for trip_time and log_count
    trip_time_min = request.args.get("trip_time_min", "").strip()
    trip_time_max = request.args.get("trip_time_max", "").strip()
    log_count_min = request.args.get("log_count_min", "").strip()
    log_count_max = request.args.get("log_count_max", "").strip()

    excel_path = os.path.join("data", "data.xlsx")
    excel_data = load_excel_data(excel_path)
    merged = []

    # Apply basic Excel filters (except route_quality)
    if filters["driver"]:
        excel_data = [row for row in excel_data if str(row.get("UserName", "")).strip() == filters["driver"]]
    if filters["trip_id"]:
        try:
            tid = int(filters["trip_id"])
            excel_data = [row for row in excel_data if row.get("tripId") == tid]
        except ValueError:
            pass
    if filters["model"]:
        excel_data = [row for row in excel_data if str(row.get("model", "")).strip() == filters["model"]]
    if filters["ram"]:
        excel_data = [row for row in excel_data if str(row.get("RAM", "")).strip() == filters["ram"]]
    if filters["carrier"]:
        excel_data = [row for row in excel_data if str(row.get("carrier", "")).strip().lower() == filters["carrier"].lower()]

    # Merge Excel data with DB records (this will update/overwrite the route_quality field)
    excel_trip_ids = [row.get("tripId") for row in excel_data if row.get("tripId")]
    
    # Apply tags filter if provided by modifying the database query
    if filters["tags"]:
        query = db_session.query(Trip).filter(Trip.trip_id.in_(excel_trip_ids)).join(Trip.tags).filter(Tag.name.ilike('%' + filters["tags"] + '%'))
        # Get filtered trip IDs to filter excel data
        db_trips = query.all()
        filtered_trip_ids = [trip.trip_id for trip in db_trips]
        excel_data = [r for r in excel_data if r.get("tripId") in filtered_trip_ids]
        db_trip_map = {trip.trip_id: trip for trip in db_trips}
    else:
        trip_issues_filter = filters.get("trip_issues", "")
        query = db_session.query(Trip).filter(Trip.trip_id.in_(excel_trip_ids))
        if trip_issues_filter:
            query = query.join(Trip.tags).filter(Tag.name.ilike('%' + trip_issues_filter + '%'))
        db_trips = query.all()
        db_trip_map = {trip.trip_id: trip for trip in db_trips}

    for row in excel_data:
        trip_id = row.get("tripId")
        db_trip = db_trip_map.get(trip_id)
        if db_trip:
            try:
                md = float(db_trip.manual_distance)
            except (TypeError, ValueError):
                md = None
            try:
                cd = float(db_trip.calculated_distance)
            except (TypeError, ValueError):
                cd = None
            row["route_quality"] = db_trip.route_quality or ""
            row["manual_distance"] = md if md is not None else ""
            row["calculated_distance"] = cd if cd is not None else ""
            if md and cd and md != 0:
                pct = (cd / md) * 100
                row["distance_percentage"] = f"{pct:.2f}%"
                variance = abs(cd - md) / md * 100
                row["variance"] = variance
            else:
                row["distance_percentage"] = "N/A"
                row["variance"] = None
            # Other fields
            row["trip_time"] = db_trip.trip_time if db_trip.trip_time is not None else ""
            row["completed_by"] = db_trip.completed_by if db_trip.completed_by is not None else ""
            row["coordinate_count"] = db_trip.coordinate_count if db_trip.coordinate_count is not None else ""
            row["status"] = db_trip.status if db_trip.status is not None else ""
            row["lack_of_accuracy"] = db_trip.lack_of_accuracy if db_trip.lack_of_accuracy is not None else ""
            row["trip_issues"] = ", ".join([tag.name for tag in db_trip.tags]) if db_trip.tags else ""
            row["tags"] = row["trip_issues"]
            # NEW: Merge the Expected Trip Quality value
            row["expected_trip_quality"] = str(db_trip.expected_trip_quality) if db_trip.expected_trip_quality is not None else "N/A"
        else:
            row["route_quality"] = ""
            row["manual_distance"] = ""
            row["calculated_distance"] = ""
            row["distance_percentage"] = "N/A"
            row["variance"] = None
            row["trip_time"] = ""
            row["completed_by"] = ""
            row["coordinate_count"] = ""
            row["status"] = ""
            row["lack_of_accuracy"] = ""
            row["trip_issues"] = ""
            row["tags"] = ""
            row["expected_trip_quality"] = "N/A"
        merged.append(row)


    # Additional variance filters
    if filters["variance_min"]:
        try:
            vmin = float(filters["variance_min"])
            merged = [r for r in merged if r.get("variance") is not None and r["variance"] >= vmin]
        except ValueError:
            pass
    if filters["variance_max"]:
        try:
            vmax = float(filters["variance_max"])
            merged = [r for r in merged if r.get("variance") is not None and r["variance"] <= vmax]
        except ValueError:
            pass

    # Now filter by route_quality based on the merged (DB) value.
    if filters["route_quality"]:
        rq_filter = filters["route_quality"].lower().strip()
        if rq_filter == "not assigned":
            merged = [r for r in merged if str(r.get("route_quality", "")).strip() == ""]
        else:
            merged = [r for r in merged if str(r.get("route_quality", "")).strip().lower() == rq_filter]
    
    # Apply lack_of_accuracy filter after merging
    if filters["lack_of_accuracy"]:
        lack_of_accuracy_filter = filters["lack_of_accuracy"].lower()
        if lack_of_accuracy_filter in ['true', 'yes', '1']:
            # Filter for trips with lack_of_accuracy = True
            merged = [r for r in merged if r.get("lack_of_accuracy") is True]
        elif lack_of_accuracy_filter in ['false', 'no', '0']:
            # Filter for trips with lack_of_accuracy = False
            merged = [r for r in merged if r.get("lack_of_accuracy") is False]

    # Helper functions for numeric comparisons
    def normalize_op(op):
        op = op.lower().strip()
        mapping = {
            "equal": "=",
            "equals": "=",
            "=": "=",
            "less than": "<",
            "more than": ">",
            "less than or equal": "<=",
            "less than or equal to": "<=",
            "more than or equal": ">=",
            "more than or equal to": ">="
        }
        return mapping.get(op, "=")

    def compare(value, op, threshold):
        op = normalize_op(op)
        if op == "=":
            return value == threshold
        elif op == "<":
            return value < threshold
        elif op == ">":
            return value > threshold
        elif op == "<=":
            return value <= threshold
        elif op == ">=":
            return value >= threshold
        return False

    # Filter by trip_time: use range filtering if trip_time_min or trip_time_max provided, else operator filtering
    if trip_time_min or trip_time_max:
        if trip_time_min:
            try:
                tt_min = float(trip_time_min)
                merged = [r for r in merged if r.get("trip_time") not in (None, "") and float(r.get("trip_time")) >= tt_min]
            except ValueError:
                pass
        if trip_time_max:
            try:
                tt_max = float(trip_time_max)
                merged = [r for r in merged if r.get("trip_time") not in (None, "") and float(r.get("trip_time")) <= tt_max]
            except ValueError:
                pass
    elif trip_time:
        try:
            tt_value = float(trip_time)
            merged = [r for r in merged if r.get("trip_time") not in (None, "") and compare(float(r.get("trip_time")), trip_time_op, tt_value)]
        except ValueError:
            pass

    # Filter by completed_by (exact, case-insensitive)
    if completed_by_filter:
        merged = [r for r in merged if r.get("completed_by") and str(r.get("completed_by")).strip().lower() == completed_by_filter.lower()]

    # Filter by log_count: use range filtering if log_count_min or log_count_max provided, else operator filtering
    if log_count_min or log_count_max:
        if log_count_min:
            try:
                lc_min = int(log_count_min)
                merged = [r for r in merged if r.get("coordinate_count") not in (None, "") and int(r.get("coordinate_count")) >= lc_min]
            except ValueError:
                pass
        if log_count_max:
            try:
                lc_max = int(log_count_max)
                merged = [r for r in merged if r.get("coordinate_count") not in (None, "") and int(r.get("coordinate_count")) <= lc_max]
            except ValueError:
                pass
    elif log_count:
        try:
            lc_value = int(log_count)
            merged = [r for r in merged if r.get("coordinate_count") not in (None, "") and compare(int(r.get("coordinate_count")), log_count_op, lc_value)]
        except ValueError:
            pass

    # Filter by status (exact, case-insensitive)
    if status_filter:
        status_lower = status_filter.lower().strip()
        if status_lower in ("empty", "not assigned"):
            merged = [r for r in merged if not r.get("status") or str(r.get("status")).strip() == ""]
        else:
            merged = [r for r in merged if r.get("status") and str(r.get("status")).strip().lower() == status_lower]

    # Build the Excel workbook
    wb = Workbook()
    ws = wb.active
    if merged:
        headers = list(merged[0].keys())
        ws.append(headers)
        for row in merged:
            ws.append([row.get(col) for col in headers])
    else:
        ws.append(["No data found"])

    file_stream = io.BytesIO()
    wb.save(file_stream)
    file_stream.seek(0)
    filename = f"{filters['export_name']}.xlsx"
    session_local.close()
    return send_file(
        file_stream,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )









# ---------------------------
# Dashboard (Analytics) - Consolidated by User, with Date Range
# ---------------------------
@app.route("/")
def analytics():
    """
    Main dashboard page with a toggle for:
      - data_scope = 'all'   => analyze ALL trips in DB
      - data_scope = 'excel' => only the trip IDs in the current data.xlsx
    We store the user's choice in the session so it persists until changed.
    """
    session_local = db_session()

    # 1) Check if user provided data_scope in request
    if "data_scope" in request.args:
        chosen_scope = request.args.get("data_scope", "all")
        flask_session["data_scope"] = chosen_scope
    else:
        chosen_scope = flask_session.get("data_scope", "all")  # default 'all'

    # 2) Additional filters for analytics page
    driver_filter = request.args.get("driver", "").strip()
    carrier_filter = request.args.get("carrier", "").strip()

    # 3) Load Excel data & merge route_quality from DB
    excel_path = os.path.join("data", "data.xlsx")
    excel_data = load_excel_data(excel_path)
    excel_trip_ids = [r["tripId"] for r in excel_data if r.get("tripId")]
    session_local = db_session()
    db_trips_for_excel = session_local.query(Trip).filter(Trip.trip_id.in_(excel_trip_ids)).all()
    db_map = {t.trip_id: t for t in db_trips_for_excel}
    for row in excel_data:
        trip_id = row.get("tripId")
        if trip_id in db_map:
            row["route_quality"] = db_map[trip_id].route_quality or ""
        else:
            row.setdefault("route_quality", "")

    # 4) Decide which DB trips to analyze for distance accuracy
    if chosen_scope == "excel":
        trips_db = db_trips_for_excel
    else:
        trips_db = session_local.query(Trip).all()

    # 5) Compute distance accuracy
    correct = 0
    incorrect = 0
    for trip in trips_db:
        try:
            md = float(trip.manual_distance)
            cd = float(trip.calculated_distance)
            if md and md != 0:
                if abs(cd - md) / md <= 0.2:
                    correct += 1
                else:
                    incorrect += 1
        except:
            pass
    total_trips = correct + incorrect
    if total_trips > 0:
        correct_pct = correct / total_trips * 100
        incorrect_pct = incorrect / total_trips * 100
    else:
        correct_pct = 0
        incorrect_pct = 0

    # 6) Build a filtered "excel-like" dataset for the user-level charts
    if chosen_scope == "excel":
        # Just the real Excel data
        filtered_excel_data = excel_data[:]
    else:
        # All DB trips, but we create placeholders if a trip isn't in Excel
        all_db = trips_db
        excel_map = {r["tripId"]: r for r in excel_data if r.get("tripId")}
        all_data_rows = []
        for tdb in all_db:
            if tdb.trip_id in excel_map:
                row_copy = dict(excel_map[tdb.trip_id])
                row_copy["route_quality"] = tdb.route_quality or ""
            else:
                row_copy = {
                    "tripId": tdb.trip_id,
                    "UserName": "",
                    "carrier": "",
                    "Android Version": "",
                    "manufacturer": "",
                    "model": "",
                    "RAM": "",
                    "route_quality": tdb.route_quality or ""
                }
            all_data_rows.append(row_copy)
        filtered_excel_data = all_data_rows

    # 7) Apply driver & carrier filters
    if driver_filter:
        filtered_excel_data = [r for r in filtered_excel_data if str(r.get("UserName","")).strip() == driver_filter]

    if carrier_filter:
        # user picked one of the 4 carriers => keep only matching normalized
        new_list = []
        for row in filtered_excel_data:
            norm_car = normalize_carrier(row.get("carrier",""))
            if norm_car == carrier_filter:
                new_list.append(row)
        filtered_excel_data = new_list

    # 8) Consolidate user-latest for charts
    user_latest = {}
    for row in filtered_excel_data:
        user = str(row.get("UserName","")).strip()
        if user:
            user_latest[user] = row
    consolidated_rows = list(user_latest.values())

    # Prepare chart data
    carrier_counts = {}
    os_counts = {}
    manufacturer_counts = {}
    model_counts = {}

    for row in consolidated_rows:
        c = normalize_carrier(row.get("carrier",""))
        carrier_counts[c] = carrier_counts.get(c,0)+1

        osv = row.get("Android Version")
        osv = str(osv) if osv is not None else "Unknown"
        os_counts[osv] = os_counts.get(osv, 0) + 1

        manu = row.get("manufacturer","Unknown")
        manufacturer_counts[manu] = manufacturer_counts.get(manu,0)+1

        mdl = row.get("model","UnknownModel")
        model_counts[mdl] = model_counts.get(mdl,0)+1

    total_users = len(consolidated_rows)
    device_usage = []
    for mdl, cnt in model_counts.items():
        pct = (cnt / total_users * 100) if total_users else 0
        device_usage.append({"model": mdl, "count": cnt, "percentage": round(pct,2)})

    # Build user_data for High/Low/Other
    user_data = {}
    for row in filtered_excel_data:
        user = str(row.get("UserName","")).strip()
        if not user:
            continue
        if user not in user_data:
            user_data[user] = {
                "total_trips": 0,
                "No Logs Trips": 0,
                "Trip Points Only Exist": 0,
                "Low": 0,
                "Moderate": 0,
                "High": 0,
                "Other": 0
            }
        user_data[user]["total_trips"] += 1
        q = row.get("route_quality", "")
        if q in ["No Logs Trips", "Trip Points Only Exist", "Low", "Moderate", "High"]:
            user_data[user][q] += 1
        else:
            user_data[user]["Other"] += 1

    # Quality analysis
    high_quality_models = {}
    low_quality_models = {}
    high_quality_android = {}
    low_quality_android = {}
    high_quality_ram = {}
    low_quality_ram = {}

    sensor_cols = [
        "Fingerprint Sensor","Accelerometer","Gyro",
        "Proximity Sensor","Compass","Barometer",
        "Background Task Killing Tendency"
    ]
    high_quality_sensors = {s:0 for s in sensor_cols}
    total_high_quality = 0

    for row in filtered_excel_data:
        q = row.get("route_quality","")
        mdl = row.get("model","UnknownModel")
        av = row.get("Android Version","Unknown")
        ram = row.get("RAM","")
        if q == "High":
            total_high_quality +=1
            high_quality_models[mdl] = high_quality_models.get(mdl,0)+1
            high_quality_android[av] = high_quality_android.get(av,0)+1
            high_quality_ram[ram] = high_quality_ram.get(ram,0)+1
            for sensor in sensor_cols:
                val = row.get(sensor,"")
                if (isinstance(val,str) and val.lower()=="true") or (val is True):
                    high_quality_sensors[sensor]+=1
        elif q == "Low":
            low_quality_models[mdl] = low_quality_models.get(mdl,0)+1
            low_quality_android[av] = low_quality_android.get(av,0)+1
            low_quality_ram[ram] = low_quality_ram.get(ram,0)+1

    session_local.close()

    # Build driver list for the dropdown
    all_drivers = sorted({str(r.get("UserName","")).strip() for r in excel_data if r.get("UserName")})
    carriers_for_dropdown = ["Vodafone","Orange","Etisalat","We"]

    return render_template(
        "analytics.html",
        data_scope=chosen_scope,
        driver_filter=driver_filter,
        carrier_filter=carrier_filter,
        drivers=all_drivers,
        carriers_for_dropdown=carriers_for_dropdown,
        carrier_counts=carrier_counts,
        os_counts=os_counts,
        manufacturer_counts=manufacturer_counts,
        device_usage=device_usage,
        total_trips=total_trips,
        correct_pct=correct_pct,
        incorrect_pct=incorrect_pct,
        user_data=user_data,
        high_quality_models=high_quality_models,
        low_quality_models=low_quality_models,
        high_quality_android=high_quality_android,
        low_quality_android=low_quality_android,
        high_quality_ram=high_quality_ram,
        low_quality_ram=low_quality_ram,
        high_quality_sensors=high_quality_sensors,
        total_high_quality=total_high_quality
    )


# ---------------------------
# Trips Page with Variance, Pagination, etc.
# ---------------------------
@app.route("/trips")
def trips():
    """
    Trips page with filtering (including trip_time, completed_by, log_count, status, route_quality,
    lack_of_accuracy, and tags) with operator support for trip_time and log_count) and pagination.
    """
    session_local = db_session()
    page = request.args.get("page", type=int, default=1)
    page_size = 100
    if page < 1:
        page = 1

    driver_filter = request.args.get("driver", "").strip()
    trip_id_search = request.args.get("trip_id", "").strip()
    # Route quality filter from the dropdown
    route_quality_filter = request.args.get("route_quality", "").strip()
    model_filter = request.args.get("model", "").strip()
    ram_filter = request.args.get("ram", "").strip()
    carrier_filter = request.args.get("carrier", "").strip()
    variance_min = request.args.get("variance_min", type=float)
    variance_max = request.args.get("variance_max", type=float)
    # New filters for operator-based filtering
    trip_time_filter = request.args.get("trip_time", "").strip()
    trip_time_op = request.args.get("trip_time_op", "equal").strip()
    completed_by_filter = request.args.get("completed_by", "").strip()
    log_count_filter = request.args.get("log_count", "").strip()
    log_count_op = request.args.get("log_count_op", "equal").strip()
    status_filter = request.args.get("status")
    if not status_filter:
        status_filter = "completed"
    else:
        status_filter = status_filter.strip()
    # Get lack_of_accuracy filter - convert to boolean for comparison
    lack_of_accuracy_filter = request.args.get("lack_of_accuracy", "").strip().lower()
    # Get tags filter
    tags_filter = request.args.get("tags", "").strip()

    # New range filter parameters for trip_time and log_count
    trip_time_min = request.args.get("trip_time_min", "").strip()
    trip_time_max = request.args.get("trip_time_max", "").strip()
    log_count_min = request.args.get("log_count_min", "").strip()
    log_count_max = request.args.get("log_count_max", "").strip()

    excel_path = os.path.join("data", "data.xlsx")
    excel_data = load_excel_data(excel_path)
    merged = []

    # New code: if start_date and end_date query parameters are provided, filter excel_data by the 'time' field
    start_date_param = request.args.get('start_date')
    end_date_param = request.args.get('end_date')
    if start_date_param and end_date_param:
        start_date_filter = None
        end_date_filter = None
        for fmt in ["%Y-%m-%d", "%d-%m-%Y"]:
            try:
                start_date_filter = datetime.strptime(start_date_param, fmt)
                end_date_filter = datetime.strptime(end_date_param, fmt)
                break
            except ValueError:
                continue
        if start_date_filter and end_date_filter:
            filtered_data = []
            for row in excel_data:
                if row.get('time'):
                    try:
                        row_time = row['time']
                        if isinstance(row_time, str):
                            row_time = datetime.strptime(row_time, "%Y-%m-%d %H:%M:%S")
                        # Compare only the date part
                        if start_date_filter.date() <= row_time.date() < end_date_filter.date():
                            filtered_data.append(row)
                    except Exception as e:
                        continue
            excel_data = filtered_data

    # Determine min and max date from the filtered excel_data
    all_times = []
    for row in excel_data:
        if row.get('time'):
            try:
                row_time = row['time']
                if isinstance(row_time, str):
                    row_time = datetime.strptime(row_time, "%Y-%m-%d %H:%M:%S")
                all_times.append(row_time)
            except Exception as e:
                continue
    min_date = min(all_times) if all_times else None
    max_date = max(all_times) if all_times else None

    # Basic Excel filters (except route_quality; we'll apply that later)
    if driver_filter:
        excel_data = [r for r in excel_data if str(r.get("UserName", "")).strip() == driver_filter]
    if trip_id_search:
        try:
            tid = int(trip_id_search)
            excel_data = [r for r in excel_data if r.get("tripId") == tid]
        except ValueError:
            pass
    if model_filter:
        excel_data = [r for r in excel_data if str(r.get("model", "")).strip() == model_filter]
    if ram_filter:
        excel_data = [r for r in excel_data if str(r.get("RAM", "")).strip() == ram_filter]
    if carrier_filter:
        new_list = []
        for row in excel_data:
            norm_car = normalize_carrier(row.get("carrier", ""))
            if norm_car == carrier_filter:
                new_list.append(row)
        excel_data = new_list

    # Merge with DB records
    excel_trip_ids = [r["tripId"] for r in excel_data if r.get("tripId")]
    
    # Apply tags filter if provided by modifying the database query
    if tags_filter:
        db_trips = session_local.query(Trip).filter(Trip.trip_id.in_(excel_trip_ids)).join(Trip.tags).filter(Tag.name.ilike('%' + tags_filter + '%')).all()
        # Get filtered trip IDs to filter excel data
        filtered_trip_ids = [trip.trip_id for trip in db_trips]
        excel_data = [r for r in excel_data if r.get("tripId") in filtered_trip_ids]
    else:
        db_trips = session_local.query(Trip).filter(Trip.trip_id.in_(excel_trip_ids)).all()
    
    db_map = {t.trip_id: t for t in db_trips}
    for row in excel_data:
        tdb = db_map.get(row["tripId"])
        if tdb:
            try:
                md = float(tdb.manual_distance)
            except:
                md = None
            try:
                cd = float(tdb.calculated_distance)
            except:
                cd = None
            row["route_quality"] = tdb.route_quality or ""
            row["manual_distance"] = md if md is not None else ""
            row["calculated_distance"] = cd if cd is not None else ""
            row["trip_time"] = tdb.trip_time if tdb.trip_time is not None else ""
            row["completed_by"] = tdb.completed_by if tdb.completed_by is not None else ""
            row["coordinate_count"] = tdb.coordinate_count if tdb.coordinate_count is not None else ""
            row["status"] = tdb.status if tdb.status is not None else ""
            row["lack_of_accuracy"] = tdb.lack_of_accuracy if tdb.lack_of_accuracy is not None else ""
            # Add distance analysis metrics
            row["short_segments_count"] = tdb.short_segments_count
            row["medium_segments_count"] = tdb.medium_segments_count
            row["long_segments_count"] = tdb.long_segments_count
            row["short_segments_distance"] = tdb.short_segments_distance
            row["medium_segments_distance"] = tdb.medium_segments_distance
            row["long_segments_distance"] = tdb.long_segments_distance
            row["max_segment_distance"] = tdb.max_segment_distance
            row["avg_segment_distance"] = tdb.avg_segment_distance
            row["trip_issues"] = ", ".join([tag.name for tag in tdb.tags]) if tdb.tags else ""
            row["tags"] = row["trip_issues"]
            if md and cd and md != 0:
                pct = (cd / md) * 100
                row["distance_percentage"] = f"{pct:.2f}%"
                var = abs(cd - md) / md * 100
                row["variance"] = var
            else:
                row["distance_percentage"] = "N/A"
                row["variance"] = None
        else:
            row["route_quality"] = ""
            row["manual_distance"] = ""
            row["calculated_distance"] = ""
            row["trip_time"] = ""
            row["completed_by"] = ""
            row["coordinate_count"] = ""
            row["status"] = ""
            row["lack_of_accuracy"] = ""
            # Initialize distance analysis metrics
            row["short_segments_count"] = None
            row["medium_segments_count"] = None
            row["long_segments_count"] = None
            row["short_segments_distance"] = None
            row["medium_segments_distance"] = None
            row["long_segments_distance"] = None
            row["max_segment_distance"] = None
            row["avg_segment_distance"] = None
            row["trip_issues"] = ""
            row["tags"] = ""
        merged.append(row)

    # Now apply route_quality filter after merging
    if route_quality_filter:
        rq_filter = route_quality_filter.lower().strip()
        if rq_filter == "not assigned":
            excel_data = [r for r in excel_data if str(r.get("route_quality", "")).strip() == ""]
        else:
            excel_data = [r for r in excel_data if str(r.get("route_quality", "")).strip().lower() == rq_filter]
    
    # Apply lack_of_accuracy filter after merging
    if lack_of_accuracy_filter:
        if lack_of_accuracy_filter in ['true', 'yes', '1']:
            # Filter for trips with lack_of_accuracy = True
            excel_data = [r for r in excel_data if r.get("lack_of_accuracy") is True]
        elif lack_of_accuracy_filter in ['false', 'no', '0']:
            # Filter for trips with lack_of_accuracy = False
            excel_data = [r for r in excel_data if r.get("lack_of_accuracy") is False]
    
    if variance_min is not None:
        excel_data = [r for r in excel_data if r.get("variance") is not None and r["variance"] >= variance_min]
    if variance_max is not None:
        excel_data = [r for r in excel_data if r.get("variance") is not None and r["variance"] <= variance_max]

    # Helper: Normalize operator strings
    def normalize_op(op):
        op = op.lower().strip()
        mapping = {
            "equal": "=",
            "equals": "=",
            "=": "=",
            "less than": "<",
            "more than": ">",
            "less than or equal": "<=",
            "less than or equal to": "<=",
            "more than or equal": ">=",
            "more than or equal to": ">="
        }
        return mapping.get(op, "=")

    def compare(value, op, threshold):
        op = normalize_op(op)
        if op == "=":
            return value == threshold
        elif op == "<":
            return value < threshold
        elif op == ">":
            return value > threshold
        elif op == "<=":
            return value <= threshold
        elif op == ">=":
            return value >= threshold
        return False

    # Apply new filters for trip_time: range filtering takes precedence over operator filtering
    if trip_time_min or trip_time_max:
        if trip_time_min:
            try:
                tt_min = float(trip_time_min)
                excel_data = [r for r in excel_data if r.get("trip_time") not in (None, "") and float(r.get("trip_time")) >= tt_min]
            except ValueError:
                pass
        if trip_time_max:
            try:
                tt_max = float(trip_time_max)
                excel_data = [r for r in excel_data if r.get("trip_time") not in (None, "") and float(r.get("trip_time")) <= tt_max]
            except ValueError:
                pass
    elif trip_time_filter:
        try:
            tt_value = float(trip_time_filter)
            excel_data = [r for r in excel_data if r.get("trip_time") not in (None, "") and compare(float(r.get("trip_time")), trip_time_op, tt_value)]
        except ValueError:
            pass

    if completed_by_filter:
        excel_data = [r for r in excel_data if r.get("completed_by") and str(r.get("completed_by")).strip().lower() == completed_by_filter.lower()]

    # Apply new filters for log_count: range filtering takes precedence over operator filtering
    if log_count_min or log_count_max:
        if log_count_min:
            try:
                lc_min = int(log_count_min)
                excel_data = [r for r in excel_data if r.get("coordinate_count") not in (None, "") and int(r.get("coordinate_count")) >= lc_min]
            except ValueError:
                pass
        if log_count_max:
            try:
                lc_max = int(log_count_max)
                excel_data = [r for r in excel_data if r.get("coordinate_count") not in (None, "") and int(r.get("coordinate_count")) <= lc_max]
            except ValueError:
                pass
    elif log_count_filter:
        try:
            lc_value = int(log_count_filter)
            excel_data = [r for r in excel_data if r.get("coordinate_count") not in (None, "") and compare(int(r.get("coordinate_count")), log_count_op, lc_value)]
        except ValueError:
            pass

    if status_filter:
        status_lower = status_filter.lower().strip()
        if status_lower in ("empty", "not assigned"):
            excel_data = [r for r in excel_data if not r.get("status") or str(r.get("status")).strip() == ""]
        else:
            excel_data = [r for r in excel_data if r.get("status") and str(r.get("status")).strip().lower() == status_lower]

    total_rows = len(excel_data)
    total_pages = (total_rows + page_size - 1) // page_size if total_rows else 1
    if page > total_pages and total_pages > 0:
        page = total_pages
    start = (page - 1) * page_size
    end = start + page_size
    page_data = excel_data[start:end]

    # Get all available tags for the dropdown
    all_tags = session_local.query(Tag).all()
    tags_for_dropdown = [tag.name for tag in all_tags]

    session_local.close()

    # For filter dropdowns: list drivers, carriers, and additional options from the Excel file
    all_excel = load_excel_data(excel_path)
    statuses = sorted(set(r.get("status", "").strip() for r in all_excel if r.get("status") and r.get("status").strip()))
    completed_by_options = sorted(set(r.get("completed_by", "").strip() for r in all_excel if r.get("completed_by") and r.get("completed_by").strip()))
    model_set = {}
    for r in all_excel:
        m = r.get("model", "").strip()
        device = r.get("Device Name", "").strip() if r.get("Device Name") else ""
        if m:
            display = m
            if device:
                display += " - " + device
            model_set[m] = display
    models_options = sorted(model_set.items(), key=lambda x: x[1])

    # Fallback: if the status or completed_by dropdowns are empty, query the database for distinct values.
    if not statuses:
        session_temp = db_session()
        statuses = sorted(set(row[0].strip() for row in session_temp.query(Trip.status).filter(Trip.status != None).distinct().all() if row[0] and row[0].strip()))
        session_temp.close()
    if not completed_by_options:
        session_temp = db_session()
        completed_by_options = sorted(set(row[0].strip() for row in session_temp.query(Trip.completed_by).filter(Trip.completed_by != None).distinct().all() if row[0] and row[0].strip()))
        session_temp.close()
    drivers = sorted({str(r.get("UserName", "")).strip() for r in all_excel if r.get("UserName")})
    carriers_for_dropdown = ["Vodafone", "Orange", "Etisalat", "We"]

    return render_template(
        "trips.html",
        driver_filter=driver_filter,
        trips=page_data,
        trip_id_search=trip_id_search,
        route_quality_filter=route_quality_filter,
        model_filter=model_filter,
        ram_filter=ram_filter,
        carrier_filter=carrier_filter,
        variance_min=variance_min if variance_min is not None else "",
        variance_max=variance_max if variance_max is not None else "",
        trip_time=trip_time_filter,
        trip_time_op=trip_time_op,
        completed_by=completed_by_filter,
        log_count=log_count_filter,
        log_count_op=log_count_op,
        status=status_filter,
        lack_of_accuracy_filter=lack_of_accuracy_filter,
        tags_filter=tags_filter,
        total_rows=total_rows,
        page=page,
        total_pages=total_pages,
        page_size=page_size,
        min_date=min_date,
        max_date=max_date,
        drivers=drivers,
        carriers_for_dropdown=carriers_for_dropdown,
        statuses=statuses,
        completed_by_options=completed_by_options,
        models_options=models_options,
        tags_for_dropdown=tags_for_dropdown
    )









@app.route("/trip/<int:trip_id>")
def trip_detail(trip_id):
    """
    Show detail page for a single trip, merges with DB.
    """
    session_local = db_session()
    db_trip, update_status = update_trip_db(trip_id)
    
    # Ensure update_status has all required keys even if there was an error
    if "error" in update_status:
        update_status = {
            "needed_update": False,
            "record_exists": True if db_trip else False,
            "updated_fields": [],
            "reason_for_update": ["Error: " + update_status.get("error", "Unknown error")],
            "error": update_status["error"]
        }
    

    if db_trip and db_trip.status and db_trip.status.lower() == "completed":
        api_data = None
    else:
        api_data = fetch_trip_from_api(trip_id)
    trip_attributes = {}
    if api_data and "data" in api_data:
        trip_attributes = api_data["data"]["attributes"]

    excel_path = os.path.join("data", "data.xlsx")
    excel_data = load_excel_data(excel_path)
    excel_trip_data = None
    for row in excel_data:
        if row.get("tripId") == trip_id:
            excel_trip_data = row
            break

    distance_verification = "N/A"
    trip_insight = ""
    distance_percentage = "N/A"
    if db_trip:
        try:
            md = float(db_trip.manual_distance)
        except (TypeError, ValueError):
            md = None
        try:
            cd = float(db_trip.calculated_distance)
        except (TypeError, ValueError):
            cd = None
        if md is not None and cd is not None:
            lower_bound = md * 0.8
            upper_bound = md * 1.2
            if lower_bound <= cd <= upper_bound:
                distance_verification = "Calculated distance is true"
                trip_insight = "Trip data is consistent."
            else:
                distance_verification = "Manual distance is true"
                trip_insight = "Trip data is inconsistent."
            if md != 0:
                distance_percentage = f"{(cd / md * 100):.2f}%"
        else:
            distance_verification = "N/A"
            trip_insight = "N/A"
            distance_percentage = "N/A"

    session_local.close()
    return render_template(
        "trip_detail.html",
        db_trip=db_trip,
        trip_attributes=trip_attributes,
        excel_trip_data=excel_trip_data,
        distance_verification=distance_verification,
        trip_insight=trip_insight,
        distance_percentage=distance_percentage,
        update_status=update_status
    )

@app.route("/update_route_quality", methods=["POST"])
def update_route_quality():
    """
    AJAX endpoint to update route_quality for a given trip_id.
    """
    session_local = db_session()
    data = request.get_json()
    trip_id = data.get("trip_id")
    quality = data.get("route_quality")
    db_trip = session_local.query(Trip).filter_by(trip_id=trip_id).first()
    if not db_trip:
        db_trip = Trip(
            trip_id=trip_id,
            route_quality=quality,
            status="",
            manual_distance=None,
            calculated_distance=None
        )
        session_local.add(db_trip)
    else:
        db_trip.route_quality = quality
    session_local.commit()
    session_local.close()
    return jsonify({"status": "success", "message": "Route quality updated."}), 200

@app.route("/update_trip_tags", methods=["POST"])
def update_trip_tags():
    session_local = db_session()
    data = request.get_json()
    trip_id = data.get("trip_id")
    tags_list = data.get("tags", [])
    if not trip_id:
        session_local.close()
        return jsonify({"status": "error", "message": "trip_id is required"}), 400
    trip = session_local.query(Trip).filter_by(trip_id=trip_id).first()
    if not trip:
        session_local.close()
        return jsonify({"status": "error", "message": "Trip not found"}), 404
    # Clear existing tags
    trip.tags = []
    updated_tags = []
    for tag_name in tags_list:
        tag = session_local.query(Tag).filter_by(name=tag_name).first()
        if not tag:
            tag = Tag(name=tag_name)
            session_local.add(tag)
            session_local.flush()
        trip.tags.append(tag)
        updated_tags.append(tag.name)
    session_local.commit()
    session_local.close()
    return jsonify({"status": "success", "tags": updated_tags}), 200

@app.route("/get_tags", methods=["GET"])
def get_tags():
    session_local = db_session()
    tags = session_local.query(Tag).all()
    data = [{"id": tag.id, "name": tag.name} for tag in tags]
    session_local.close()
    return jsonify({"status": "success", "tags": data}), 200

@app.route("/create_tag", methods=["POST"])
def create_tag():
    session_local = db_session()
    data = request.get_json()
    tag_name = data.get("name")
    if not tag_name:
        session_local.close()
        return jsonify({"status": "error", "message": "Tag name is required"}), 400
    existing = session_local.query(Tag).filter_by(name=tag_name).first()
    if existing:
        session_local.close()
        return jsonify({"status": "error", "message": "Tag already exists"}), 400
    tag = Tag(name=tag_name)
    session_local.add(tag)
    session_local.commit()
    session_local.refresh(tag)
    session_local.close()
    return jsonify({"status": "success", "tag": {"id": tag.id, "name": tag.name}}), 200

@app.route("/trip_insights")
def trip_insights():
    """
    Shows route quality counts, distance averages, distance consistency, and additional dashboards:
      - Average Trip Duration vs Trip Quality
      - Completed By vs Trip Quality
      - Average Logs Count vs Trip Quality
      - App Version vs Trip Quality
    Respects the data_scope from session so it matches the user's choice (all data or excel-only).
    """
    session_local = db_session()

    data_scope = flask_session.get("data_scope", "all")

    # If excel-only, limit to those trip IDs in data.xlsx
    excel_path = os.path.join("data", "data.xlsx")
    excel_data = load_excel_data(excel_path)
    excel_trip_ids = [r["tripId"] for r in excel_data if r.get("tripId")]

    if data_scope == "excel":
        trips_db = session_local.query(Trip).filter(Trip.trip_id.in_(excel_trip_ids)).all()
    else:
        trips_db = session_local.query(Trip).all()

    quality_counts = {
        "No Logs Trips": 0,
        "Trip Points Only Exist": 0,
        "Low": 0,
        "Moderate": 0,
        "High": 0,
        "": 0
    }
    total_manual = 0
    total_calculated = 0
    count_manual = 0
    count_calculated = 0
    consistent = 0
    inconsistent = 0

    for trip in trips_db:
        q = trip.route_quality if trip.route_quality else ""
        quality_counts[q] = quality_counts.get(q, 0) + 1
        try:
            md = float(trip.manual_distance)
            cd = float(trip.calculated_distance)
            total_manual += md
            total_calculated += cd
            count_manual += 1
            count_calculated += 1
            if md != 0 and abs(cd - md) / md <= 0.2:
                consistent += 1
            else:
                inconsistent += 1
        except:
            pass

    avg_manual = total_manual / count_manual if count_manual else 0
    avg_calculated = total_calculated / count_calculated if count_calculated else 0

    excel_map = {r['tripId']: r for r in excel_data if r.get('tripId')}
    device_specs = defaultdict(lambda: defaultdict(list))
    for trip in trips_db:
        trip_id = trip.trip_id
        quality = trip.route_quality if trip.route_quality else "Unknown"
        if trip_id in excel_map:
            row = excel_map[trip_id]
            device_specs[quality]['model'].append(row.get('model', 'Unknown'))
            device_specs[quality]['android'].append(row.get('Android Version', 'Unknown'))
            device_specs[quality]['manufacturer'].append(row.get('manufacturer', 'Unknown'))
            device_specs[quality]['ram'].append(row.get('RAM', 'Unknown'))
    automatic_insights = {}
    for quality, specs in device_specs.items():
        model_counter = Counter(specs['model'])
        android_counter = Counter(specs['android'])
        manufacturer_counter = Counter(specs['manufacturer'])
        ram_counter = Counter(specs['ram'])
        most_common_model = model_counter.most_common(1)[0][0] if model_counter else 'N/A'
        most_common_android = android_counter.most_common(1)[0][0] if android_counter else 'N/A'
        most_common_manufacturer = manufacturer_counter.most_common(1)[0][0] if manufacturer_counter else 'N/A'
        most_common_ram = ram_counter.most_common(1)[0][0] if ram_counter else 'N/A'
        insight = f"For trips with quality '{quality}', most devices are {most_common_manufacturer} {most_common_model} (Android {most_common_android}, RAM {most_common_ram})."
        if quality.lower() == 'high':
            insight += " This suggests that high quality trips are associated with robust mobile specs, contributing to accurate tracking."
        elif quality.lower() == 'low':
            insight += " This might indicate that lower quality trips could be influenced by devices with suboptimal specifications."
        automatic_insights[quality] = insight

    # New Aggregation: Lack of Accuracy vs Trip Quality
    accuracy_data = {}
    for trip in trips_db:
        quality = trip.route_quality if trip.route_quality else "Unspecified"
        if quality not in accuracy_data:
            accuracy_data[quality] = {"count": 0, "lack_count": 0}
        accuracy_data[quality]["count"] += 1
        if trip.lack_of_accuracy:
            accuracy_data[quality]["lack_count"] += 1
    accuracy_percentages = {}
    for quality, data in accuracy_data.items():
        count = data["count"]
        lack = data["lack_count"]
        percentage = round((lack / count) * 100, 2) if count > 0 else 0
        accuracy_percentages[quality] = percentage

    # --- New Dashboard Aggregations ---
    # 1. Average Trip Duration vs Trip Quality
    trip_duration_sum = {}
    trip_duration_count = {}
    for trip in trips_db:
        quality = trip.route_quality if trip.route_quality else "Unspecified"
        if trip.trip_time is not None and trip.trip_time != "":
            trip_duration_sum[quality] = trip_duration_sum.get(quality, 0) + float(trip.trip_time)
            trip_duration_count[quality] = trip_duration_count.get(quality, 0) + 1
    avg_trip_duration_quality = {}
    for quality in trip_duration_sum:
        avg_trip_duration_quality[quality] = trip_duration_sum[quality] / trip_duration_count[quality]

    # 2. Completed By vs Trip Quality
    completed_by_quality = {}
    for trip in trips_db:
        quality = trip.route_quality if trip.route_quality else "Unspecified"
        comp = trip.completed_by if trip.completed_by else "Unknown"
        if quality not in completed_by_quality:
            completed_by_quality[quality] = {}
        completed_by_quality[quality][comp] = completed_by_quality[quality].get(comp, 0) + 1

    # 3. Average Logs Count vs Trip Quality
    logs_sum = {}
    logs_count = {}
    for trip in trips_db:
        quality = trip.route_quality if trip.route_quality else "Unspecified"
        if trip.coordinate_count is not None and trip.coordinate_count != "":
            logs_sum[quality] = logs_sum.get(quality, 0) + int(trip.coordinate_count)
            logs_count[quality] = logs_count.get(quality, 0) + 1
    avg_logs_count_quality = {}
    for quality in logs_sum:
        avg_logs_count_quality[quality] = logs_sum[quality] / logs_count[quality]

    # 4. App Version vs Trip Quality
    app_version_quality = {}
    for trip in trips_db:
        row = excel_map.get(trip.trip_id)
        if row:
            app_ver = row.get("app_version", "Unknown")
        else:
            app_ver = "Unknown"
        quality = trip.route_quality if trip.route_quality else "Unspecified"
        if app_ver not in app_version_quality:
            app_version_quality[app_ver] = {}
        app_version_quality[app_ver][quality] = app_version_quality[app_ver].get(quality, 0) + 1

    # --- End New Dashboard Aggregations ---

    # Existing aggregations below...
    quality_drilldown = {}
    for quality, specs in device_specs.items():
        quality_drilldown[quality] = {
            'model': dict(Counter(specs['model'])),
            'android': dict(Counter(specs['android'])),
            'manufacturer': dict(Counter(specs['manufacturer'])),
            'ram': dict(Counter(specs['ram']))
        }

    allowed_ram_str = ["2GB", "3GB", "4GB", "6GB", "8GB", "12GB", "16GB"]
    ram_quality_counts = {ram: {} for ram in allowed_ram_str}
    import re
    for trip in trips_db:
        row = excel_map.get(trip.trip_id)
        if row:
            ram_str = row.get("RAM", "")
            match = re.search(r'(\d+(?:\.\d+)?)', str(ram_str))
            if match:
                ram_value = float(match.group(1))
                try:
                    ram_int = int(round(ram_value))
                except:
                    continue
                nearest = min([2, 3, 4, 6, 8, 12, 16], key=lambda v: abs(v - ram_int))
                ram_label = f"{nearest}GB"
                quality_val = trip.route_quality if trip.route_quality in ["High", "Moderate", "Low", "No Logs Trips", "Trip Points Only Exist"] else "Empty"
                if quality_val not in ram_quality_counts[ram_label]:
                    ram_quality_counts[ram_label][quality_val] = 0
                ram_quality_counts[ram_label][quality_val] += 1

    sensor_cols = ["Fingerprint Sensor", "Accelerometer", "Gyro",
                   "Proximity Sensor", "Compass", "Barometer",
                   "Background Task Killing Tendency"]
    sensor_stats = {}
    for sensor in sensor_cols:
        sensor_stats[sensor] = {}
    for trip in trips_db:
        quality_val = trip.route_quality if trip.route_quality else "Unspecified"
        row = excel_map.get(trip.trip_id)
        if row:
            for sensor in sensor_cols:
                value = row.get(sensor, "")
                present = False
                if isinstance(value, str) and value.lower() == "true":
                    present = True
                elif value is True:
                    present = True
                if quality_val not in sensor_stats[sensor]:
                    sensor_stats[sensor][quality_val] = {"present": 0, "total": 0}
                sensor_stats[sensor][quality_val]["total"] += 1
                if present:
                    sensor_stats[sensor][quality_val]["present"] += 1

    quality_by_os = {}
    for trip in trips_db:
        row = excel_map.get(trip.trip_id)
        if row:
            os_ver = row.get("Android Version", "Unknown")
            q = trip.route_quality if trip.route_quality else "Unspecified"
            if os_ver not in quality_by_os:
                quality_by_os[os_ver] = {}
            quality_by_os[os_ver][q] = quality_by_os[os_ver].get(q, 0) + 1

    manufacturer_quality = {}
    for trip in trips_db:
        row = excel_map.get(trip.trip_id)
        if row:
            manu = row.get("manufacturer", "Unknown")
            q = trip.route_quality if trip.route_quality else "Unspecified"
            if manu not in manufacturer_quality:
                manufacturer_quality[manu] = {}
            manufacturer_quality[manu][q] = manufacturer_quality[manu].get(q, 0) + 1

    carrier_quality = {}
    for trip in trips_db:
        row = excel_map.get(trip.trip_id)
        if row:
            carrier_val = normalize_carrier(row.get("carrier", "Unknown"))
            q = trip.route_quality if trip.route_quality else "Unspecified"
            if carrier_val not in carrier_quality:
                carrier_quality[carrier_val] = {}
            carrier_quality[carrier_val][q] = carrier_quality[carrier_val].get(q, 0) + 1

    time_series = {}
    for row in excel_data:
        try:
            time_str = row.get("time", "")
            if time_str:
                dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                date_str = dt.strftime("%Y-%m-%d")
                q = row.get("route_quality", "Unspecified")
                if date_str not in time_series:
                    time_series[date_str] = {}
                time_series[date_str][q] = time_series[date_str].get(q, 0) + 1
        except:
            continue

    session_local.close()
    return render_template(
        "trip_insights.html",
        quality_counts=quality_counts,
        avg_manual=avg_manual,
        avg_calculated=avg_calculated,
        consistent=consistent,
        inconsistent=inconsistent,
        automatic_insights=automatic_insights,
        quality_drilldown=quality_drilldown,
        ram_quality_counts=ram_quality_counts,
        sensor_stats=sensor_stats,
        quality_by_os=quality_by_os,
        manufacturer_quality=manufacturer_quality,
        carrier_quality=carrier_quality,
        time_series=time_series,
        avg_trip_duration_quality=avg_trip_duration_quality,
        completed_by_quality=completed_by_quality,
        avg_logs_count_quality=avg_logs_count_quality,
        app_version_quality=app_version_quality,
        accuracy_data=accuracy_percentages
    )




@app.route("/save_filter", methods=["POST"])
def save_filter():
    """
    Store current filter parameters in session under a filter name.
    """
    filter_name = request.form.get("filter_name")
    filters = {
        "trip_id": request.form.get("trip_id"),
        "route_quality": request.form.get("route_quality"),
        "model": request.form.get("model"),
        "ram": request.form.get("ram"),
        "carrier": request.form.get("carrier"),
        "variance_min": request.form.get("variance_min"),
        "variance_max": request.form.get("variance_max"),
        "driver": request.form.get("driver")
    }
    if filter_name:
        saved = flask_session.get("saved_filters", {})
        saved[filter_name] = filters
        flask_session["saved_filters"] = saved
        flash(f"Filter '{filter_name}' saved.", "success")
    else:
        flash("Please provide a filter name.", "danger")
    return redirect(url_for("trips"))

@app.route("/apply_filter/<filter_name>")
def apply_filter(filter_name):
    """
    Apply a saved filter by redirecting to /trips with the saved query params.
    """
    saved = flask_session.get("saved_filters", {})
    filters = saved.get(filter_name)
    if filters:
        qs = "&".join(f"{key}={value}" for key, value in filters.items() if value)
        return redirect(url_for("trips") + "?" + qs)
    else:
        flash("Saved filter not found.", "danger")
        return redirect(url_for("trips"))

@app.route('/update_date_range', methods=['POST'])
def update_date_range():
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    if not start_date or not end_date:
        return jsonify({'error': 'Both start_date and end_date are required.'}), 400

    # Backup existing consolidated data
    data_file = 'data/data.xlsx'
    backup_dir = 'data/backup'
    if os.path.exists(data_file):
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        backup_file = os.path.join(backup_dir, f"data_{start_date}_{end_date}.xlsx")
        try:
            shutil.move(data_file, backup_file)
        except Exception as e:
            return jsonify({'error': 'Failed to backup data file: ' + str(e)}), 500

    # Run exportmix.py with new dates
    try:
        subprocess.check_call(['python3', 'exportmix.py', '--start-date', start_date, '--end-date', end_date])
    except subprocess.CalledProcessError as e:
        return jsonify({'error': 'Failed to export data: ' + str(e)}), 500

    # Run consolidatemixpanel.py
    try:
        subprocess.check_call(['python3', 'consolidatemixpanel.py'])
    except subprocess.CalledProcessError as e:
        return jsonify({'error': 'Failed to consolidate data: ' + str(e)}), 500

    return jsonify({'message': 'Data updated successfully.'})

@app.route("/update_db_async", methods=["POST"])
def update_db_async():
    job_id = str(uuid.uuid4())
    update_jobs[job_id] = {
        "status": "processing", 
        "total": 0, 
        "completed": 0, 
        "updated": 0,
        "skipped": 0,
        "errors": 0, 
        "created": 0,
        "updated_fields": Counter(),
        "reasons": Counter()
    }
    threading.Thread(target=process_update_db_async, args=(job_id,)).start()
    return jsonify({"job_id": job_id})

def process_update_db_async(job_id):
    try:
        excel_path = os.path.join("data", "data.xlsx")
        excel_data = load_excel_data(excel_path)
        trips_to_update = [row.get("tripId") for row in excel_data if row.get("tripId")]
        update_jobs[job_id]["total"] = len(trips_to_update)
        
        # Process trips using ThreadPoolExecutor
        futures_to_trips = {}
        with ThreadPoolExecutor(max_workers=40) as executor:
            # Submit jobs to the executor
            for trip_id in trips_to_update:
                # Use force_update=False to skip complete records
                future = executor.submit(update_trip_db, trip_id, False)
                futures_to_trips[future] = trip_id
            
            # Process results as they complete
            for future in as_completed(futures_to_trips):
                trip_id = futures_to_trips[future]
                try:
                    db_trip, update_status = future.result()
                    
                    # Track statistics similar to update_db route
                    if "error" in update_status:
                        update_jobs[job_id]["errors"] += 1
                    elif not update_status["record_exists"]:
                        update_jobs[job_id]["created"] += 1
                        update_jobs[job_id]["updated"] += 1
                    elif update_status["updated_fields"]:
                        update_jobs[job_id]["updated"] += 1
                        # Count which fields were updated
                        for field in update_status["updated_fields"]:
                            update_jobs[job_id]["updated_fields"][field] = update_jobs[job_id]["updated_fields"].get(field, 0) + 1
                    else:
                        update_jobs[job_id]["skipped"] += 1
                        
                    # Track reasons for updates
                    for reason in update_status.get("reason_for_update", []):
                        update_jobs[job_id]["reasons"][reason] = update_jobs[job_id]["reasons"].get(reason, 0) + 1
                        
                except Exception as e:
                    print(f"Error processing trip {trip_id}: {e}")
                    update_jobs[job_id]["errors"] += 1
                
                update_jobs[job_id]["completed"] += 1
                
        update_jobs[job_id]["status"] = "completed"
        
        # Prepare summary message
        if update_jobs[job_id]["updated"] > 0:
            most_updated_fields = sorted(update_jobs[job_id]["updated_fields"].items(), 
                                         key=lambda x: x[1], reverse=True)[:3]
            update_jobs[job_id]["summary_fields"] = [f"{field} ({count})" for field, count in most_updated_fields]
            
            most_common_reasons = sorted(update_jobs[job_id]["reasons"].items(), 
                                        key=lambda x: x[1], reverse=True)[:3]
            update_jobs[job_id]["summary_reasons"] = [f"{reason} ({count})" for reason, count in most_common_reasons]
        
    except Exception as e:
        update_jobs[job_id]["status"] = "error"
        update_jobs[job_id]["error_message"] = str(e)

@app.route("/update_all_db_async", methods=["POST"])
def update_all_db_async():
    job_id = str(uuid.uuid4())
    update_jobs[job_id] = {
        "status": "processing", 
        "total": 0, 
        "completed": 0, 
        "updated": 0,
        "skipped": 0,
        "errors": 0, 
        "created": 0,
        "updated_fields": Counter(),
        "reasons": Counter()
    }
    threading.Thread(target=process_update_all_db_async, args=(job_id,)).start()
    return jsonify({"job_id": job_id})

def process_update_all_db_async(job_id):
    try:
        # Load trip IDs from Excel instead of getting all trips from DB
        excel_path = os.path.join("data", "data.xlsx")
        excel_data = load_excel_data(excel_path)
        trips_to_update = [row.get("tripId") for row in excel_data if row.get("tripId")]
        update_jobs[job_id]["total"] = len(trips_to_update)
        
        # Process trips using ThreadPoolExecutor
        futures_to_trips = {}
        with ThreadPoolExecutor(max_workers=40) as executor:
            # Submit jobs to the executor
            for trip_id in trips_to_update:
                # Use force_update=True for full update from API
                future = executor.submit(update_trip_db, trip_id, True)
                futures_to_trips[future] = trip_id
            
            # Process results as they complete
            for future in as_completed(futures_to_trips):
                trip_id = futures_to_trips[future]
                try:
                    db_trip, update_status = future.result()
                    
                    # Track statistics
                    if "error" in update_status:
                        update_jobs[job_id]["errors"] += 1
                    elif not update_status["record_exists"]:
                        update_jobs[job_id]["created"] += 1
                        update_jobs[job_id]["updated"] += 1
                    elif update_status["updated_fields"]:
                        update_jobs[job_id]["updated"] += 1
                        # Count which fields were updated
                        for field in update_status["updated_fields"]:
                            update_jobs[job_id]["updated_fields"][field] = update_jobs[job_id]["updated_fields"].get(field, 0) + 1
                    else:
                        update_jobs[job_id]["skipped"] += 1
                        
                    # Track reasons for updates
                    for reason in update_status.get("reason_for_update", []):
                        update_jobs[job_id]["reasons"][reason] = update_jobs[job_id]["reasons"].get(reason, 0) + 1
                        
                except Exception as e:
                    print(f"Error processing trip {trip_id}: {e}")
                    update_jobs[job_id]["errors"] += 1
                
                update_jobs[job_id]["completed"] += 1
                
        update_jobs[job_id]["status"] = "completed"
        
        # Prepare summary message
        if update_jobs[job_id]["updated"] > 0:
            most_updated_fields = sorted(update_jobs[job_id]["updated_fields"].items(), 
                                         key=lambda x: x[1], reverse=True)[:3]
            update_jobs[job_id]["summary_fields"] = [f"{field} ({count})" for field, count in most_updated_fields]
            
            # Add reasons summary like in process_update_db_async
            most_common_reasons = sorted(update_jobs[job_id]["reasons"].items(), 
                                        key=lambda x: x[1], reverse=True)[:3]
            update_jobs[job_id]["summary_reasons"] = [f"{reason} ({count})" for reason, count in most_common_reasons]
        
    except Exception as e:
        update_jobs[job_id]["status"] = "error"
        update_jobs[job_id]["error_message"] = str(e)

@app.route("/update_progress", methods=["GET"])
def update_progress():
    job_id = request.args.get("job_id")
    if job_id in update_jobs:
        job = update_jobs[job_id]
        total = job.get("total", 0)
        completed = job.get("completed", 0)
        updated = job.get("updated", 0)
        skipped = job.get("skipped", 0)
        percent = (completed / total * 100) if total > 0 else 0
        
        response = {
            "status": job["status"], 
            "total": total, 
            "completed": completed, 
            "percent": percent,
            "updated": updated,
            "skipped": skipped,
            "errors": job.get("errors", 0),
            "created": job.get("created", 0)
        }
        
        # Add summary information if available
        if job["status"] == "completed":
            if "summary_fields" in job:
                response["summary_fields"] = job["summary_fields"]
            if "summary_reasons" in job:
                response["summary_reasons"] = job["summary_reasons"]
            
        return jsonify(response)
    else:
        return jsonify({"error": "Job not found"}), 404

# New endpoint to proxy trip coordinates with proper authentication
@app.route('/trip_coordinates/<int:trip_id>')
def trip_coordinates(trip_id):
    url = f"{BASE_API_URL}/trips/{trip_id}/coordinates"
    # Try to get primary token
    token = fetch_api_token() or API_TOKEN
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers)
    # If unauthorized, try alternative token
    if resp.status_code == 401:
        alt_token = fetch_api_token_alternative()
        if alt_token:
            headers["Authorization"] = f"Bearer {alt_token}"
            resp = requests.get(url, headers=headers)
    try:
        resp.raise_for_status()
        return jsonify(resp.json())
    except Exception as e:
        app.logger.error(f"Error fetching coordinates for trip {trip_id}: {e}")
        return jsonify({"message": "Error fetching coordinates", "error": str(e)}), 500

@app.route("/delete_tag", methods=["POST"])
def delete_tag():
    data = request.get_json()
    tag_name = data.get("name")
    if not tag_name:
        return jsonify(status="error", message="Tag name is required"), 400
    tag = db_session.query(Tag).filter_by(name=tag_name).first()
    if not tag:
        return jsonify(status="error", message="Tag not found"), 404
    # Remove tag from all associated trips
    for trip in list(tag.trips):
        trip.tags.remove(tag)
    db_session.delete(tag)
    db_session.commit()
    return jsonify(status="success", message="Tag deleted successfully")

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
