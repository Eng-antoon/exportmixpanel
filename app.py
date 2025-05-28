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
    session as flask_session,
)
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import scoped_session, sessionmaker
from datetime import datetime, timedelta
import shutil
import subprocess
from collections import Counter
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import pandas as pd
import traceback
import re
import time
from threading import Thread
from datetime import date 
import sys
import json

from db.config import DB_URI, API_TOKEN, BASE_API_URL, API_EMAIL, API_PASSWORD 
from db.models import Base, Trip, Tag

from exportmix import export_data_for_comparison

from metabase_client import get_trip_points_data, metabase
import trip_points_helper as tph
import trip_metrics
import device_metrics

# Import calculation helpers
from helpers.calculations import (
    haversine_distance, 
    calculate_expected_trip_quality,
    analyze_trip_segments,
    calculate_trip_time,
    determine_completed_by,
    normalize_carrier,
    CARRIER_GROUPS
)
# Import data loading helpers
from helpers.data_loaders import load_excel_data, load_mixpanel_data
# Import API helpers
from helpers.api import (
    fetch_api_token,
    fetch_api_token_alternative,
    fetch_coordinates_count,
    fetch_trip_from_api
)
# Import database helpers
from helpers.database import (
    _is_trip_data_complete,
    update_trip_db 
)
# Session helpers are not directly used by app.py after route move


# Import the Blueprints
from routes.main import main_bp
from routes.trip import trip_bp
from routes.data import data_bp
from routes.insights import insights_bp
from routes.device import device_bp
from routes.tags import tags_bp
from routes.async_tasks import async_bp

app = Flask(__name__)

# Register the Blueprints
app.register_blueprint(main_bp)
app.register_blueprint(trip_bp)
app.register_blueprint(data_bp)
app.register_blueprint(insights_bp)
app.register_blueprint(device_bp)
app.register_blueprint(tags_bp)
app.register_blueprint(async_bp)

engine = create_engine(
    DB_URI,
    pool_size=20,
    max_overflow=20,
    pool_timeout=30
)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
update_jobs = {} 
executor = ThreadPoolExecutor(max_workers=40) 
app.secret_key = "your_secret_key"

progress_data = {}

def migrate_db():
    try:
        print("Creating database tables from models...")
        Base.metadata.create_all(bind=engine)
        print("Database tables created successfully")
        
        connection = engine.connect()
        inspector = inspect(engine)
        existing_columns = [column['name'] for column in inspector.get_columns('trips')]
        
        # Migration logic for columns (remains unchanged from previous state)
        if 'pickup_success_rate' not in existing_columns:
            try:
                connection.execute(text("ALTER TABLE trips ADD COLUMN pickup_success_rate FLOAT"))
                connection.commit() 
            except Exception as e: print(f"Error adding pickup_success_rate column: {e}"); connection.rollback()
        if 'dropoff_success_rate' not in existing_columns:
            try:
                connection.execute(text("ALTER TABLE trips ADD COLUMN dropoff_success_rate FLOAT"))
                connection.commit()
            except Exception as e: print(f"Error adding dropoff_success_rate column: {e}"); connection.rollback()
        if 'total_points_success_rate' not in existing_columns:
            try:
                connection.execute(text("ALTER TABLE trips ADD COLUMN total_points_success_rate FLOAT"))
                connection.commit()
            except Exception as e: print(f"Error adding total_points_success_rate column: {e}"); connection.rollback()
        if 'locations_trip_points' not in existing_columns:
            try:
                connection.execute(text("ALTER TABLE trips ADD COLUMN locations_trip_points INTEGER"))
                connection.commit()
            except Exception as e: print(f"Error adding locations_trip_points column: {e}"); connection.rollback()
        if 'driver_trip_points' not in existing_columns:
            try:
                connection.execute(text("ALTER TABLE trips ADD COLUMN driver_trip_points INTEGER"))
                connection.commit()
            except Exception as e: print(f"Error adding driver_trip_points column: {e}"); connection.rollback()
        if 'autoending' not in existing_columns:
            try:
                connection.execute(text("ALTER TABLE trips ADD COLUMN autoending BOOLEAN"))
                connection.commit()
            except Exception as e: print(f"Error adding autoending column: {e}"); connection.rollback()
        if 'driver_app_interactions_per_trip' not in existing_columns:
            try:
                connection.execute(text("ALTER TABLE trips ADD COLUMN driver_app_interactions_per_trip FLOAT"))
                connection.commit()
            except Exception as e: print(f"Error adding driver_app_interactions_per_trip column: {e}"); connection.rollback()
        if 'driver_app_interaction_rate' not in existing_columns:
            try:
                connection.execute(text("ALTER TABLE trips ADD COLUMN driver_app_interaction_rate FLOAT"))
                connection.commit()
            except Exception as e: print(f"Error adding driver_app_interaction_rate column: {e}"); connection.rollback()
        if 'trip_points_interaction_ratio' not in existing_columns:
            try:
                connection.execute(text("ALTER TABLE trips ADD COLUMN trip_points_interaction_ratio FLOAT"))
                connection.commit()
            except Exception as e: print(f"Error adding trip_points_interaction_ratio column: {e}"); connection.rollback()

        connection.close()
        print("Database migration completed")
    except Exception as e:
        # Use app.logger if available, otherwise print
        if app and hasattr(app, 'logger'): app.logger.error(f"Migration error: {e}")
        else: print(f"Migration error: {e}")
        print(f"Error during database migration: {e}")

print("Running database migration...")
migrate_db()

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

# API helper functions have been moved to helpers/api.py
# Database helper functions have been moved to helpers/database.py
# Session helper functions (get_saved_filters, save_filter_to_session) moved to helpers/session.py

# Routes that are still part of app.py 
@app.route("/force_update_autoending", methods=["GET", "POST"])
def force_update_autoending():
    session_local = db_session()
    try:
        null_count_before = session_local.query(Trip).filter(Trip.autoending.is_(None)).count()
        if request.method == "POST":
            session_local.query(Trip).filter(Trip.autoending.is_(None)).update({Trip.autoending: False})
            session_local.commit()
            null_count_after = session_local.query(Trip).filter(Trip.autoending.is_(None)).count()
            true_count = session_local.query(Trip).filter(Trip.autoending.is_(True)).count()
            false_count = session_local.query(Trip).filter(Trip.autoending.is_(False)).count()
            return jsonify({"status": "success", "message": f"Updated {null_count_before - null_count_after} trips...", "null_count_before": null_count_before, "null_count_after": null_count_after, "true_count": true_count, "false_count": false_count})
        return jsonify({"null_count": null_count_before, "message": "Use POST method to update NULL values to False"})
    except Exception as e: return jsonify({"status": "error", "message": str(e)})
    finally: session_local.close()

@app.route("/update_route_quality", methods=["POST"])
def update_route_quality():
    session_local = db_session()
    data = request.get_json()
    trip_id = data.get("trip_id")
    quality = data.get("route_quality")
    db_trip = session_local.query(Trip).filter_by(trip_id=trip_id).first()
    if not db_trip:
        db_trip = Trip(trip_id=trip_id, route_quality=quality, status="", manual_distance=None, calculated_distance=None)
        session_local.add(db_trip)
    else:
        db_trip.route_quality = quality
    session_local.commit()
    session_local.close()
    return jsonify({"status": "success", "message": "Route quality updated."}), 200

# /save_filter and /apply_filter routes were moved to routes/main.py

@app.route('/update_date_range', methods=['POST'])
def update_date_range():
    start_date = request.form.get('start_date'); end_date = request.form.get('end_date')
    if not start_date or not end_date: return jsonify({'error': 'Both start_date and end_date are required.'}), 400
    flask_session['start_date'] = start_date; flask_session['end_date'] = end_date
    data_file = 'data/data.xlsx'; backup_dir = 'data/backup'
    if os.path.exists(data_file):
        if not os.path.exists(backup_dir): os.makedirs(backup_dir)
        backup_file = os.path.join(backup_dir, f"data_{start_date}_{end_date}.xlsx")
        try: shutil.move(data_file, backup_file)
        except Exception as e: return jsonify({'error': 'Failed to backup data file: ' + str(e)}), 500
    try: subprocess.check_call(['python3', 'exportmix.py', '--start-date', start_date, '--end-date', end_date])
    except subprocess.CalledProcessError as e: return jsonify({'error': 'Failed to export data: ' + str(e)}), 500
    try: subprocess.check_call(['python3', 'consolidatemixpanel.py'])
    except subprocess.CalledProcessError as e: return jsonify({'error': 'Failed to consolidate data: ' + str(e)}), 500
    return jsonify({'message': 'Data updated successfully.'})

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run the Flask application.')
    parser.add_argument('--port', type=int, default=5000, help='Port to run the server on')
    args = parser.parse_args()
    app.run(debug=True, host="0.0.0.0", port=args.port)
