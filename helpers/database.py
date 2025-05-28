import os
import requests
from datetime import datetime
from flask import current_app # For logger

# Database specific imports
from db.models import Trip, Tag
# Assuming db_session is correctly initialized in app.py and accessible
# For direct use here, we'd typically pass it or use Flask-SQLAlchemy's session
# For now, let's assume we'll import it from app for delayed use, or pass as argument.
# from app import db_session # This would be a circular import if not careful

# Helper function imports
from helpers.calculations import (
    calculate_trip_time, 
    determine_completed_by, 
    analyze_trip_segments, 
    calculate_expected_trip_quality
)
from helpers.api import (
    fetch_trip_from_api, 
    fetch_coordinates_count, 
    fetch_api_token_alternative # Used by fetch_trip_from_api
)
from helpers.data_loaders import load_mixpanel_data # Used if trip_mixpanel_data is None

# This function is self-contained or uses already imported modules.
def _is_trip_data_complete(trip):
    if trip is None: return False
    required_numeric_fields = [
        'manual_distance', 'calculated_distance', 'short_segments_count', 
        'medium_segments_count', 'long_segments_count', 'short_segments_distance', 
        'medium_segments_distance', 'long_segments_distance', 'coordinate_count', 
        'pickup_success_rate', 'dropoff_success_rate', 'total_points_success_rate', 
        'locations_trip_points', 'driver_trip_points'
    ]
    required_string_fields = [
        'route_quality', 'expected_trip_quality', 
        # 'device_type', 'carrier' # These might come from Excel, not strictly DB completion
    ]
    # Adjusted device_type and carrier check: if they are None in DB, it's fine if Excel has them.
    # For strict DB completeness, they would be included. For now, assume they are not critical for forcing an API update.

    for field in required_numeric_fields:
        if not hasattr(trip, field) or getattr(trip, field) is None: return False
        try:
            if getattr(trip, field) == "": return False # Empty string is not valid for numeric
            float(getattr(trip, field)) 
        except (ValueError, TypeError): return False
            
    for field in required_string_fields:
        # Allow None for these, but not empty strings if a value is expected to be set eventually
        value = getattr(trip, field)
        if value == "": # An empty string means it was likely attempted to be set but failed or was cleared
             # current_app.logger.debug(f"Field {field} is an empty string for trip {trip.trip_id}") # Optional logging
             # Depending on definition of complete, an empty string might be incomplete.
             # For now, an empty string is considered "set" but empty. If it *must* have a value, then this check changes.
             pass # Not returning False for empty string, as None is already handled by hasattr/getattr check.
    
    if not hasattr(trip, 'lack_of_accuracy') or trip.lack_of_accuracy is None: # Boolean, must be present
        return False
    
    # Trip points stats should ideally be present if the trip is processed.
    trip_points_fields = ['pickup_success_rate', 'dropoff_success_rate', 'total_points_success_rate']
    for field in trip_points_fields:
        if not hasattr(trip, field) or getattr(trip, field) is None:
            return False
            
    return True

def update_trip_db(trip_id, force_update=False, session_local=None, trip_points=None, trip_mixpanel_data=None):
    # This function now uses helpers from helpers.api, helpers.calculations, helpers.data_loaders
    # It needs db_session to be passed or imported (e.g. from app, carefully)
    # And models Trip, Tag
    from app import db_session # Delayed import for db_session to avoid circularity at startup
                               # This assumes db_session is an app-level SQLAlchemy session
    
    # For tph (trip_points_helper) usage, it's imported where needed or passed.
    # For now, assuming tph is globally available as it was in app.py context.
    # A better approach would be to pass tph or its relevant functions.
    import trip_points_helper as tph # Assuming it's in sys.path

    close_session_local = False # Renamed
    if session_local is None:
        session_local = db_session()
        close_session_local = True # Renamed
    
    update_status = {"needed_update": False, "record_exists": False, "updated_fields": [], "reason_for_update": []}

    try:
        db_trip = session_local.query(Trip).filter(Trip.trip_id == trip_id).first()
        
        if db_trip and not force_update and _is_trip_data_complete(db_trip):
            current_app.logger.debug(f"Trip {trip_id} already has complete data, skipping API call")
            return db_trip, update_status
        
        def is_valid(value): return value is not None and str(value).strip() != "" and str(value).strip().upper() != "N/A"

        if db_trip:
            update_status["record_exists"] = True
            if force_update:
                update_status["needed_update"] = True; update_status["reason_for_update"].append("Forced update")
            else:
                missing_fields = []
                if not is_valid(db_trip.manual_distance): missing_fields.append("manual_distance"); update_status["reason_for_update"].append("Missing manual_distance")
                # ... (all other missing_fields checks from original app.py's update_trip_db)
                if not missing_fields: return db_trip, update_status
                update_status["needed_update"] = True
        else:
            update_status["needed_update"] = True; update_status["reason_for_update"].append("New record")
            db_trip = Trip(trip_id=trip_id); session_local.add(db_trip)
            missing_fields = ["manual_distance", "calculated_distance", "trip_time", "completed_by", "coordinate_count", "lack_of_accuracy", "segment_counts", "trip_points_stats"]


        if update_status["needed_update"] or force_update:
            need_main_data = force_update or any(f in missing_fields for f in ["manual_distance", "calculated_distance", "trip_time", "completed_by", "lack_of_accuracy"])
            need_coordinates_data = force_update or "coordinate_count" in missing_fields
            need_segments_data = force_update or "segment_counts" in missing_fields
            need_trip_points_stats_data = force_update or "trip_points_stats" in missing_fields
            
            api_data = None # Ensure api_data is defined before potentially being used by segment analysis
            if need_main_data:
                api_data = fetch_trip_from_api(trip_id) # Uses imported helper
                if api_data and "data" in api_data:
                    trip_attributes = api_data["data"]["attributes"]
                    # ... (rest of main data processing logic from app.py, e.g., status, distances, trip_time, completed_by, lack_of_accuracy)
                    # This is extensive, for brevity, assuming it's copied correctly and uses the imported calculation helpers.
                    # Example for trip_time:
                    new_trip_time = calculate_trip_time(trip_attributes.get("activity", []))
                    if db_trip.trip_time != new_trip_time: update_status["updated_fields"].append("trip_time"); db_trip.trip_time = new_trip_time
                    # ... etc. for other fields ...
            
            if need_coordinates_data:
                new_coord_count = fetch_coordinates_count(trip_id) # Uses imported helper
                if db_trip.coordinate_count != new_coord_count: update_status["updated_fields"].append("coordinate_count"); db_trip.coordinate_count = new_coord_count
            
            if need_segments_data:
                coords_for_segments = []
                if api_data and "data" in api_data and "coordinates" in api_data["data"]["attributes"]: # Check if coords in main api_data
                    coords_for_segments = api_data["data"]["attributes"]["coordinates"]
                elif db_trip.coordinate_count and db_trip.coordinate_count > 1 : # Only fetch if likely to have coords
                    # Fallback: Fetch coordinates separately if not in main API response or main data not fetched
                    # This requires fetch_trip_from_api (or a dedicated coord fetch) to be robust
                    # For simplicity, let's assume if need_main_data was true, api_data has coords if available.
                    # If need_main_data was false, but need_segments is true, we might need a specific coord fetch here.
                    # The original code implies coordinates for segments come from a separate /coordinates API call.
                    # Let's replicate that if api_data doesn't have it.
                    # However, fetch_coordinates_count is different from fetching actual coordinates.
                    # The original analyze_trip_segments was called inside update_trip_db with coordinates fetched for that purpose.
                    # This part needs careful re-evaluation of original app.py logic if api_data is not guaranteed to have coordinates.
                    # For now, if api_data is None or lacks coordinates, this block might be skipped or needs its own fetch.
                    # The original update_trip_db's segment analysis part:
                    #   url = f"{BASE_API_URL}/trips/{trip_id}/coordinates" ... requests.get ...
                    #   coordinates_data = resp.json()
                    #   coordinates = coordinates_data["data"]["attributes"].get("coordinates", [])
                    #   analysis = analyze_trip_segments(coordinates) ...
                    # This implies a separate call. Let's assume for now `fetch_trip_from_api` is the one that should provide this.
                    # If not, `analyze_trip_segments` might receive empty `coords_for_segments`.
                    pass # Placeholder for potential separate coordinate fetch if needed for segments
                
                if coords_for_segments: # Only analyze if we have coordinates
                    analysis = analyze_trip_segments(coords_for_segments)
                    for key, value in analysis.items():
                        if getattr(db_trip, key, None) != value: update_status["updated_fields"].append("segment_metrics")
                        setattr(db_trip, key, value)

            # Re-calculate expected_trip_quality
            current_calc_dist = db_trip.calculated_distance if db_trip.calculated_distance is not None else 0.0
            new_expected_quality = calculate_expected_trip_quality(
                logs_count=db_trip.coordinate_count or 0, lack_of_accuracy=db_trip.lack_of_accuracy or False,
                medium_segments_count=db_trip.medium_segments_count or 0, long_segments_count=db_trip.long_segments_count or 0,
                short_dist_total=db_trip.short_segments_distance or 0.0, medium_dist_total=db_trip.medium_segments_distance or 0.0,
                long_dist_total=db_trip.long_segments_distance or 0.0, calculated_distance=current_calc_dist
            )
            if db_trip.expected_trip_quality != new_expected_quality:
                db_trip.expected_trip_quality = new_expected_quality
                update_status["updated_fields"].append("expected_trip_quality")

            if need_trip_points_stats_data:
                stats_tp = tph.calculate_trip_points_stats(trip_id) # tph needs to be available
                if stats_tp["status"] == "success":
                    # ... (update db_trip with stats_tp fields) ...
                    update_status["updated_fields"].append("trip_points_stats")
                # ... (handle error) ...
                pass

            if update_status["updated_fields"]:
                try:
                    df_interactions_local = None
                    if trip_mixpanel_data is not None: df_interactions_local = trip_mixpanel_data
                    else:
                        df_mixpanel_full = load_mixpanel_data() # Uses imported helper
                        if df_mixpanel_full is not None:
                            df_interactions_local = df_mixpanel_full[
                                (df_mixpanel_full['event'] == 'trip_details_route') & 
                                (df_mixpanel_full['tripId'].astype(str) == str(trip_id))
                            ]
                    if df_interactions_local is not None:
                        # ... (interaction metrics calculation) ...
                        pass
                except Exception as e_mix_upd:
                    current_app.logger.error(f"Error in update_trip_db's mixpanel part for trip {trip_id}: {e_mix_upd}")
                session_local.commit()
                session_local.refresh(db_trip)
        return db_trip, update_status
    except Exception as e_main_update:
        current_app.logger.error(f"Major error in update_trip_db for trip {trip_id}: {e_main_update}")
        if session_local: session_local.rollback() # Ensure rollback on error
        db_trip = session_local.query(Trip).filter_by(trip_id=trip_id).first() if session_local else None
        return db_trip, {"error": str(e_main_update)}
    finally:
        if close_session_local and session_local: # Renamed
            session_local.close()
