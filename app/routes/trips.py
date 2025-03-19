from flask import render_template, request, jsonify, redirect, url_for, flash, session as flask_session, send_file
from datetime import datetime
import io
import openpyxl
from openpyxl import Workbook
from app import app, db_session
from db.models import Trip, Tag
from app.utils.helpers import get_saved_filters
from app.models.operations import get_all_trips, get_all_tags, update_trip_route_quality, get_trip_tags, add_tag_to_trip, remove_tag_from_trip, create_tag, delete_tag
from sqlalchemy import and_, or_, func

@app.route("/trips")
def trips():
    """
    Display trips list, with optional filtering.
    """
    # Get filter parameters from query string
    filter_params = {}
    for key in request.args:
        if key.startswith("filter_") and request.args.get(key):
            filter_params[key] = request.args.get(key)
    
    # Start with a base query
    query = db_session.query(Trip)
    
    # Initialize list to store filter conditions
    filter_conditions = []
    
    # Define a helper function to normalize operators
    def normalize_op(op):
        op_map = {
            "eq": "==",
            "neq": "!=",
            "gt": ">",
            "gte": ">=", 
            "lt": "<",
            "lte": "<="
        }
        return op_map.get(op, "==")
    
    # Define a comparison function
    def compare(value, op, threshold):
        if op == "==":
            return value == threshold
        elif op == "!=":
            return value != threshold
        elif op == ">":
            return value > threshold
        elif op == ">=":
            return value >= threshold
        elif op == "<":
            return value < threshold
        elif op == "<=":
            return value <= threshold
        return False
    
    # Process filter criteria for different fields
    if "filter_status" in filter_params:
        status = filter_params["filter_status"]
        filter_conditions.append(Trip.status.ilike(f"%{status}%"))
    
    if "filter_completed_by" in filter_params:
        completed_by = filter_params["filter_completed_by"]
        filter_conditions.append(Trip.completed_by == completed_by)
    
    if "filter_route_quality" in filter_params:
        route_quality = filter_params["filter_route_quality"]
        filter_conditions.append(Trip.route_quality == route_quality)
    
    if "filter_lack_of_accuracy" in filter_params:
        lack_value = filter_params["filter_lack_of_accuracy"]
        
        if lack_value == "yes":
            filter_conditions.append(Trip.lack_of_accuracy == True)
        elif lack_value == "no":
            filter_conditions.append(Trip.lack_of_accuracy == False)
        elif lack_value == "not_set":
            filter_conditions.append(Trip.lack_of_accuracy == None)
    
    if "filter_distance_op" in filter_params and "filter_distance_value" in filter_params:
        try:
            op = normalize_op(filter_params["filter_distance_op"])
            value = float(filter_params["filter_distance_value"])
            
            if op == "==":
                filter_conditions.append(Trip.calculated_distance == value)
            elif op == "!=":
                filter_conditions.append(Trip.calculated_distance != value)
            elif op == ">":
                filter_conditions.append(Trip.calculated_distance > value)
            elif op == ">=":
                filter_conditions.append(Trip.calculated_distance >= value)
            elif op == "<":
                filter_conditions.append(Trip.calculated_distance < value)
            elif op == "<=":
                filter_conditions.append(Trip.calculated_distance <= value)
        except ValueError:
            pass
    
    if "filter_trip_time_op" in filter_params and "filter_trip_time_value" in filter_params:
        try:
            op = normalize_op(filter_params["filter_trip_time_op"])
            value = float(filter_params["filter_trip_time_value"])
            
            if op == "==":
                filter_conditions.append(Trip.trip_time == value)
            elif op == "!=":
                filter_conditions.append(Trip.trip_time != value)
            elif op == ">":
                filter_conditions.append(Trip.trip_time > value)
            elif op == ">=":
                filter_conditions.append(Trip.trip_time >= value)
            elif op == "<":
                filter_conditions.append(Trip.trip_time < value)
            elif op == "<=":
                filter_conditions.append(Trip.trip_time <= value)
        except ValueError:
            pass
    
    if "filter_tag" in filter_params:
        tag_name = filter_params["filter_tag"]
        if tag_name:
            # Join with Tag model to filter by tag name
            query = query.join(Trip.tags).filter(Tag.name == tag_name)
    
    # Apply all filter conditions
    if filter_conditions:
        query = query.filter(and_(*filter_conditions))
    
    # Execute the query to get trips
    trips = query.all()
    
    # Get all tags for the filter dropdown
    all_tags = db_session.query(Tag).all()
    saved_filters = get_saved_filters()
    
    return render_template(
        "trips.html",
        trips=trips,
        all_tags=all_tags,
        filter_params=filter_params,
        saved_filters=saved_filters
    )

@app.route("/export_trips")
def export_trips():
    """
    Export trips to Excel file.
    """
    trips = db_session.query(Trip).all()
    
    # Create Excel workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Trips Data"
    
    # Add header row
    headers = [
        "Trip ID", 
        "Manual Distance",
        "Calculated Distance",
        "Route Quality",
        "Status",
        "Trip Time (min)",
        "Completed By",
        "Coordinate Count",
        "Lack of Accuracy",
        "Tags",
        "Short Segments Count",
        "Medium Segments Count",
        "Long Segments Count",
        "Short Segments Distance",
        "Medium Segments Distance",
        "Long Segments Distance",
        "Max Segment Distance",
        "Avg Segment Distance"
    ]
    ws.append(headers)
    
    # Add data rows
    for trip in trips:
        # Collect all tags as comma-separated string
        tags_text = ", ".join(tag.name for tag in trip.tags) if trip.tags else ""
        
        lack_of_accuracy_text = ""
        if trip.lack_of_accuracy is True:
            lack_of_accuracy_text = "Yes"
        elif trip.lack_of_accuracy is False:
            lack_of_accuracy_text = "No"
        
        row = [
            trip.trip_id,
            trip.manual_distance,
            trip.calculated_distance,
            trip.route_quality,
            trip.status,
            trip.trip_time,
            trip.completed_by,
            trip.coordinate_count,
            lack_of_accuracy_text,
            tags_text,
            trip.short_segments_count,
            trip.medium_segments_count,
            trip.long_segments_count,
            trip.short_segments_distance,
            trip.medium_segments_distance,
            trip.long_segments_distance,
            trip.max_segment_distance,
            trip.avg_segment_distance
        ]
        ws.append(row)
    
    # Create a BytesIO object to save the workbook to
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"trips_data_{timestamp}.xlsx"
    
    return send_file(
        output, 
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename
    ) 