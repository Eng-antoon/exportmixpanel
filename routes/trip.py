from flask import Blueprint, render_template, request, jsonify, session as flask_session
import os
import requests # For fetch_api_token, fetch_trip_from_api etc.

# db.models.Trip will be needed for type hinting or queries if any are done directly here.
# However, if update_trip_db handles all DB interaction, direct model import might not be needed.
# from db.models import Trip

# Config constants might be needed by helpers imported from app.py
# from db.config import API_TOKEN, BASE_API_URL # Not importing directly to avoid issues if app not fully initialized

trip_bp = Blueprint('trip_bp', __name__)

@trip_bp.route("/trip/<int:trip_id>")
def trip_detail(trip_id):
    # Delayed imports to access components from app.py
    from app import db_session
    from helpers.data_loaders import load_excel_data
    from helpers.api import fetch_trip_from_api 
    from helpers.database import update_trip_db # Import from helpers
    
    session_local = db_session()
    # update_trip_db is assumed to handle its own db_session if not passed
    db_trip, update_status = update_trip_db(trip_id, session_local=session_local) 
    
    if "error" in update_status:
        update_status = {
            "needed_update": False,
            "record_exists": True if db_trip else False,
            "updated_fields": [],
            "reason_for_update": ["Error: " + update_status.get("error", "Unknown error")],
            "error": update_status["error"]
        }
    
    api_data = None
    if not (db_trip and db_trip.status and db_trip.status.lower() == "completed"):
        api_data = fetch_trip_from_api(trip_id) # fetch_trip_from_api needs BASE_API_URL, API_TOKEN
                                                # and fetch_api_token_alternative, assumed to be accessible
                                                # via its own imports or app context.

    trip_attributes = {}
    if api_data and "data" in api_data:
        trip_attributes = api_data["data"]["attributes"]

    excel_path = os.path.join("data", "data.xlsx")
    excel_data = load_excel_data(excel_path) # load_excel_data is from app.py
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
        
        if md is not None and cd is not None and md != 0:
            variance = abs(cd - md) / md * 100
            if variance <= 10.0:
                distance_verification = "Calculated distance is true"
                trip_insight = "Trip data is consistent."
            else:
                distance_verification = "Manual distance is true"
                trip_insight = "Trip data is inconsistent."
            distance_percentage = f"{(cd / md * 100):.2f}%"
        # Removed the 'else' for N/A to ensure they remain N/A if conditions not met

    session_local.close()
    return render_template(
        "trip_detail.html",
        db_trip=db_trip,
        trip_attributes=trip_attributes,
        excel_trip_data=excel_trip_data,
        distance_verification=distance_verification,
        trip_insight=trip_insight,
        distance_percentage=distance_percentage,
        update_status=update_status,
        trip_id=trip_id
    )

@trip_bp.route('/trip_coordinates/<int:trip_id>')
def trip_coordinates(trip_id):
    # Delayed imports for app-specific components
    # Assuming app.logger is available through current_app
    from flask import current_app 
    from helpers.api import fetch_api_token, fetch_api_token_alternative # Import from helpers
    from db.config import API_TOKEN, BASE_API_URL # Import directly from config

    url = f"{BASE_API_URL}/trips/{trip_id}/coordinates"
    try:
        token = fetch_api_token() or API_TOKEN 
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        resp = requests.get(url, headers=headers)
        
        if resp.status_code == 401: # Unauthorized
            alt_token = fetch_api_token_alternative() 
            if alt_token:
                headers["Authorization"] = f"Bearer {alt_token}"
                resp = requests.get(url, headers=headers)
        
        resp.raise_for_status() # Raises HTTPError for bad responses (4XX or 5XX)
        data = resp.json()
        
        if not data or "data" not in data or "attributes" not in data["data"]:
            raise ValueError("Invalid response format from API")
            
        return jsonify(data)
        
    except requests.exceptions.RequestException as e:
        current_app.logger.error(f"Network error fetching coordinates for trip {trip_id}: {str(e)}")
        return jsonify({"status": "error", "message": "Failed to fetch coordinates from API", "error": str(e)}), 500
    except ValueError as e: # Catch specific error for invalid format
        current_app.logger.error(f"Invalid data format for trip {trip_id}: {str(e)}")
        return jsonify({"status": "error", "message": "Invalid data format from API", "error": str(e)}), 500
    except Exception as e: # Catch-all for other unexpected errors
        current_app.logger.error(f"Unexpected error fetching coordinates for trip {trip_id}: {str(e)}")
        return jsonify({"status": "error", "message": "Unexpected error fetching coordinates", "error": str(e)}), 500
