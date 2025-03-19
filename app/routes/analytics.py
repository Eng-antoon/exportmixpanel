from flask import render_template, request, jsonify, redirect, url_for, flash, session as flask_session
from collections import defaultdict, Counter
from datetime import datetime
from app import app, db_session
from db.models import Trip, Tag
from app.utils.helpers import normalize_carrier, load_excel_data

@app.route("/")
def analytics():
    """
    Main analytics dashboard route.
    """
    # Get counts of trips by status
    trips = db_session.query(Trip).all()
    status_counts = Counter()
    completion_counts = {"admin": 0, "driver": 0, "other": 0}
    route_quality_counts = Counter()
    
    total_trips = len(trips)
    completed_trips = 0
    
    for trip in trips:
        status = trip.status
        status_counts[status] = status_counts.get(status, 0) + 1
        
        if status == "completed":
            completed_trips += 1
            completed_by = trip.completed_by
            if completed_by == "admin":
                completion_counts["admin"] += 1
            elif completed_by == "driver":
                completion_counts["driver"] += 1
            else:
                completion_counts["other"] += 1
                
        # Track route quality
        quality = trip.route_quality
        if quality:
            route_quality_counts[quality] += 1
            
    # Calculate percentages for the doughnut chart
    completion_percentages = {}
    if completed_trips > 0:
        completion_percentages = {
            "admin": round(100 * completion_counts["admin"] / completed_trips, 1),
            "driver": round(100 * completion_counts["driver"] / completed_trips, 1),
            "other": round(100 * completion_counts["other"] / completed_trips, 1)
        }
        
    # Calculate route quality percentages
    route_quality_percentages = {}
    if total_trips > 0:
        for quality, count in route_quality_counts.items():
            route_quality_percentages[quality] = round(100 * count / total_trips, 1)
            
    # Analyze tags
    tags_data = defaultdict(int)
    for trip in trips:
        for tag in trip.tags:
            tags_data[tag.name] += 1
            
    # Sort tags by count
    sorted_tags = sorted(tags_data.items(), key=lambda x: x[1], reverse=True)
    
    # Generate data for lack_of_accuracy chart
    lack_accuracy_data = {"Yes": 0, "No": 0, "Not Set": 0}
    
    for trip in trips:
        if trip.lack_of_accuracy is True:
            lack_accuracy_data["Yes"] += 1
        elif trip.lack_of_accuracy is False:
            lack_accuracy_data["No"] += 1
        else:
            lack_accuracy_data["Not Set"] += 1
            
    lack_accuracy_percentages = {}
    if total_trips > 0:
        for key, count in lack_accuracy_data.items():
            lack_accuracy_percentages[key] = round(100 * count / total_trips, 1)
            
    # Try to load mobile device data from the Excel export
    mobile_data = []
    carrier_counts = Counter()
    brand_counts = Counter()
    device_name_counts = Counter()
    android_version_counts = Counter()
    
    try:
        mobile_data = load_excel_data("data/data.xlsx")
        
        for row in mobile_data:
            # Normalize carrier names and count them
            carrier = row.get("carrier", "")
            if carrier:
                normalized_carrier = normalize_carrier(carrier)
                carrier_counts[normalized_carrier] += 1
                
            # Count by brand
            brand = row.get("brand", "")
            if brand:
                brand_counts[brand] += 1
                
            # Count by device name
            device_name = row.get("Device Name", "")
            if device_name:
                device_name_counts[device_name] += 1
                
            # Count by Android version
            android_version = row.get("Android Version", "")
            if android_version:
                android_version_counts[android_version] += 1
                
    except Exception as e:
        print(f"Error loading mobile data: {e}")
        
    # Sort and limit data for charts
    top_carriers = sorted(carrier_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    top_brands = sorted(brand_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    top_devices = sorted(device_name_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    top_android_versions = sorted(android_version_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
    # Get date range from session or set defaults
    from_date = flask_session.get("from_date", "")
    to_date = flask_session.get("to_date", "")
    
    # Get saved filters
    saved_filters = flask_session.get("saved_filters", {})
    
    return render_template(
        "analytics.html",
        status_counts=status_counts,
        completion_counts=completion_counts,
        completion_percentages=completion_percentages,
        route_quality_counts=route_quality_counts,
        route_quality_percentages=route_quality_percentages,
        tags=sorted_tags,
        lack_accuracy_data=lack_accuracy_data,
        lack_accuracy_percentages=lack_accuracy_percentages,
        top_carriers=top_carriers,
        top_brands=top_brands,
        top_devices=top_devices,
        top_android_versions=top_android_versions,
        from_date=from_date,
        to_date=to_date,
        saved_filters=saved_filters
    )

@app.route('/update_date_range', methods=['POST'])
def update_date_range():
    """
    Update the session's date range for filtering.
    """
    from_date = request.form.get('from_date', '')
    to_date = request.form.get('to_date', '')
    
    # Store in session
    flask_session["from_date"] = from_date
    flask_session["to_date"] = to_date
    
    # Redirect back to appropriate page
    referer = request.headers.get('Referer', '')
    if 'trip_insights' in referer:
        return redirect(url_for('trip_insights'))
    return redirect(url_for('analytics'))

@app.route("/save_filter", methods=["POST"])
def save_filter():
    """
    Save a filter configuration to the session.
    """
    filter_name = request.form.get("filter_name")
    filter_data = {}
    
    # Extract filter parameters from form
    for key, value in request.form.items():
        if key.startswith("filter_") and key != "filter_name" and value:
            filter_data[key] = value
            
    # Get current filters or initialize empty dict
    saved_filters = flask_session.get("saved_filters", {})
    
    # Add or update this filter
    saved_filters[filter_name] = filter_data
    
    # Save back to session
    flask_session["saved_filters"] = saved_filters
    
    flash(f"Filter '{filter_name}' saved successfully!")
    
    return redirect(url_for("trips"))

@app.route("/apply_filter/<filter_name>")
def apply_filter(filter_name):
    """
    Apply a saved filter.
    """
    saved_filters = flask_session.get("saved_filters", {})
    
    if filter_name in saved_filters:
        # Redirect to trips with filter parameters
        filter_params = saved_filters[filter_name]
        query_params = "&".join([f"{k}={v}" for k, v in filter_params.items()])
        
        return redirect(f"/trips?{query_params}")
    else:
        flash(f"Filter '{filter_name}' not found!")
        return redirect(url_for("trips"))

@app.route("/trip_insights")
def trip_insights():
    """
    Advanced trip insights dashboard.
    """
    # Get all trips
    trips = db_session.query(Trip).all()
    
    # Segment distribution analysis
    short_segments_total = sum(t.short_segments_count or 0 for t in trips)
    medium_segments_total = sum(t.medium_segments_count or 0 for t in trips)
    long_segments_total = sum(t.long_segments_count or 0 for t in trips)
    
    segment_count_distribution = {
        "Short (<1km)": short_segments_total,
        "Medium (1-5km)": medium_segments_total,
        "Long (>5km)": long_segments_total
    }
    
    # Segment distance distribution
    short_distance_total = sum(t.short_segments_distance or 0 for t in trips)
    medium_distance_total = sum(t.medium_segments_distance or 0 for t in trips) 
    long_distance_total = sum(t.long_segments_distance or 0 for t in trips)
    
    segment_distance_distribution = {
        "Short (<1km)": round(short_distance_total, 2),
        "Medium (1-5km)": round(medium_distance_total, 2),
        "Long (>5km)": round(long_distance_total, 2)
    }
    
    # Maximum segment distance analysis
    max_distances = [t.max_segment_distance for t in trips if t.max_segment_distance is not None]
    max_distance_ranges = {
        "0-1km": 0,
        "1-5km": 0,
        "5-10km": 0,
        "10-20km": 0,
        "20km+": 0
    }
    
    for dist in max_distances:
        if dist < 1:
            max_distance_ranges["0-1km"] += 1
        elif dist < 5:
            max_distance_ranges["1-5km"] += 1
        elif dist < 10:
            max_distance_ranges["5-10km"] += 1
        elif dist < 20:
            max_distance_ranges["10-20km"] += 1
        else:
            max_distance_ranges["20km+"] += 1
            
    # Average segment distance analysis
    avg_distances = [t.avg_segment_distance for t in trips if t.avg_segment_distance is not None]
    avg_distance_ranges = {
        "0-0.1km": 0,
        "0.1-0.5km": 0,
        "0.5-1km": 0,
        "1-2km": 0,
        "2km+": 0
    }
    
    for dist in avg_distances:
        if dist < 0.1:
            avg_distance_ranges["0-0.1km"] += 1
        elif dist < 0.5:
            avg_distance_ranges["0.1-0.5km"] += 1
        elif dist < 1:
            avg_distance_ranges["0.5-1km"] += 1
        elif dist < 2:
            avg_distance_ranges["1-2km"] += 1
        else:
            avg_distance_ranges["2km+"] += 1
            
    # Trip time analysis
    trip_times = [t.trip_time for t in trips if t.trip_time is not None]
    trip_time_ranges = {
        "0-10min": 0,
        "10-30min": 0,
        "30-60min": 0,
        "1-2hr": 0,
        "2hr+": 0
    }
    
    for time in trip_times:
        if time < 10:
            trip_time_ranges["0-10min"] += 1
        elif time < 30:
            trip_time_ranges["10-30min"] += 1
        elif time < 60:
            trip_time_ranges["30-60min"] += 1
        elif time < 120:
            trip_time_ranges["1-2hr"] += 1
        else:
            trip_time_ranges["2hr+"] += 1
            
    # Get date range from session
    from_date = flask_session.get("from_date", "")
    to_date = flask_session.get("to_date", "")
    
    return render_template(
        "trip_insights.html",
        segment_count_distribution=segment_count_distribution,
        segment_distance_distribution=segment_distance_distribution,
        max_distance_ranges=max_distance_ranges,
        avg_distance_ranges=avg_distance_ranges,
        trip_time_ranges=trip_time_ranges,
        from_date=from_date,
        to_date=to_date
    ) 