"""
Mixpanel Export and Analysis Tool

This is the main application initialization module.
"""

from flask import Flask, session
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import os
from concurrent.futures import ThreadPoolExecutor

# Create Flask application
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev_key_for_session_management')
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///trips.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Create database engine and session factory
engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

# Create SQLAlchemy extension
db = SQLAlchemy(app)

# Create thread pool executor for async operations
executor = ThreadPoolExecutor(max_workers=4)

# Job status tracking
job_status = {}
update_jobs = {}  # Added for backward compatibility with tests

# Set default session values
@app.before_request
def before_request():
    if 'date_range' not in session:
        session['date_range'] = 'last_30_days'
    if 'saved_filters' not in session:
        session['saved_filters'] = {}

# Import routes
from app.routes.analytics import analytics, update_date_range, save_filter, apply_filter, trip_insights
from app.routes.trips import trips, export_trips
from app.routes.trip_details import (
    trip_detail, update_route_quality, update_trip_tags, 
    get_tags, create_tag, delete_tag, trip_coordinates
)
from app.routes.api import update_db, update_db_async, update_all_db_async, update_progress

# Register routes
app.add_url_rule('/', 'analytics', analytics)
app.add_url_rule('/update_date_range', 'update_date_range', update_date_range, methods=['POST'])
app.add_url_rule('/save_filter', 'save_filter', save_filter, methods=['POST'])
app.add_url_rule('/apply_filter', 'apply_filter', apply_filter)
app.add_url_rule('/trip_insights', 'trip_insights', trip_insights)

app.add_url_rule('/trips', 'trips', trips)
app.add_url_rule('/export_trips', 'export_trips', export_trips)

app.add_url_rule('/trip/<int:trip_id>', 'trip_detail', trip_detail)
app.add_url_rule('/update_route_quality', 'update_route_quality', update_route_quality, methods=['POST'])
app.add_url_rule('/update_trip_tags', 'update_trip_tags', update_trip_tags, methods=['POST'])
app.add_url_rule('/get_tags', 'get_tags', get_tags)
app.add_url_rule('/create_tag', 'create_tag', create_tag, methods=['POST'])
app.add_url_rule('/delete_tag', 'delete_tag', delete_tag, methods=['POST'])
app.add_url_rule('/trip_coordinates/<int:trip_id>', 'trip_coordinates', trip_coordinates)

app.add_url_rule('/api/update_db', 'update_db', update_db, methods=['POST'])
app.add_url_rule('/api/update_db_async', 'update_db_async', update_db_async, methods=['POST'])
app.add_url_rule('/api/update_all_db_async', 'update_all_db_async', update_all_db_async, methods=['POST'])
app.add_url_rule('/api/update_progress', 'update_progress', update_progress)

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()
