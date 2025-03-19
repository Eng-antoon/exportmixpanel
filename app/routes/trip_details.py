from flask import render_template, request, jsonify, redirect, url_for, flash
from app import app, db_session
from db.models import Trip, Tag
from app.api.client import fetch_trip_coordinates
from app.models.operations import update_trip_route_quality, add_tag_to_trip, remove_tag_from_trip, get_trip_tags, create_tag, delete_tag
from db.config import API_TOKEN

@app.route("/trip/<int:trip_id>")
def trip_detail(trip_id):
    """
    Display detailed information for a specific trip.
    """
    trip = db_session.query(Trip).filter(Trip.trip_id == trip_id).first()
    
    if not trip:
        flash("Trip not found", "error")
        return redirect(url_for("trips"))
    
    tags = get_trip_tags(trip_id)
    all_tags = db_session.query(Tag).all()
    
    # Get segment distribution labels and data for chart
    if trip.short_segments_count is not None and trip.medium_segments_count is not None and trip.long_segments_count is not None:
        segment_labels = ["Short (<1km)", "Medium (1-5km)", "Long (>5km)"]
        segment_counts = [trip.short_segments_count, trip.medium_segments_count, trip.long_segments_count]
        segment_distances = [
            trip.short_segments_distance or 0,
            trip.medium_segments_distance or 0,
            trip.long_segments_distance or 0
        ]
    else:
        segment_labels = []
        segment_counts = []
        segment_distances = []
    
    return render_template(
        "trip_detail.html",
        trip=trip,
        tags=tags,
        all_tags=all_tags,
        segment_labels=segment_labels,
        segment_counts=segment_counts,
        segment_distances=segment_distances
    )

@app.route("/update_route_quality", methods=["POST"])
def update_route_quality():
    """
    Update the route quality for a trip.
    """
    trip_id = request.form.get("trip_id")
    quality = request.form.get("quality")
    
    if not trip_id or not quality:
        return jsonify({"success": False, "message": "Missing required parameters"})
    
    success = update_trip_route_quality(int(trip_id), quality)
    
    if success:
        return jsonify({"success": True, "message": "Route quality updated successfully"})
    else:
        return jsonify({"success": False, "message": "Failed to update route quality"})

@app.route("/update_trip_tags", methods=["POST"])
def update_trip_tags():
    """
    Add or remove tags from a trip.
    """
    trip_id = request.form.get("trip_id")
    tag_name = request.form.get("tag_name")
    action = request.form.get("action")  # 'add' or 'remove'
    
    if not trip_id or not tag_name or not action:
        return jsonify({"success": False, "message": "Missing required parameters"})
    
    trip_id = int(trip_id)
    
    if action == "add":
        success = add_tag_to_trip(trip_id, tag_name)
        if success:
            return jsonify({"success": True, "message": f"Tag '{tag_name}' added successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to add tag"})
    elif action == "remove":
        success = remove_tag_from_trip(trip_id, tag_name)
        if success:
            return jsonify({"success": True, "message": f"Tag '{tag_name}' removed successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to remove tag"})
    else:
        return jsonify({"success": False, "message": "Invalid action"})

@app.route("/get_tags", methods=["GET"])
def get_tags():
    """
    Get all available tags.
    """
    tags = db_session.query(Tag).all()
    tag_list = [{"id": tag.id, "name": tag.name} for tag in tags]
    return jsonify({"success": True, "tags": tag_list})

@app.route("/create_tag", methods=["POST"])
def create_tag_route():
    """
    Create a new tag.
    """
    tag_name = request.form.get("tag_name")
    
    if not tag_name:
        return jsonify({"success": False, "message": "Tag name is required"})
    
    tag = create_tag(tag_name)
    
    if tag:
        return jsonify({
            "success": True, 
            "message": f"Tag '{tag_name}' created successfully",
            "tag": {"id": tag.id, "name": tag.name}
        })
    else:
        return jsonify({"success": False, "message": f"Tag '{tag_name}' already exists or could not be created"})

@app.route("/delete_tag", methods=["POST"])
def delete_tag_route():
    """
    Delete a tag.
    """
    tag_name = request.form.get("tag_name")
    
    if not tag_name:
        return jsonify({"success": False, "message": "Tag name is required"})
    
    success = delete_tag(tag_name)
    
    if success:
        return jsonify({"success": True, "message": f"Tag '{tag_name}' deleted successfully"})
    else:
        return jsonify({"success": False, "message": f"Failed to delete tag '{tag_name}'"})

@app.route('/trip_coordinates/<int:trip_id>')
def trip_coordinates(trip_id):
    """
    Get coordinates for a specific trip.
    """
    coordinates = fetch_trip_coordinates(trip_id)
    
    if not coordinates:
        return jsonify({
            "success": False,
            "message": "No coordinates found for this trip",
            "data": []
        })
    
    # Format coordinates for leaflet
    leaflet_coords = []
    for coord in coordinates:
        try:
            lat = float(coord[1])  # API returns as [lon, lat]
            lon = float(coord[0])
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                leaflet_coords.append([lat, lon])
        except (ValueError, IndexError):
            continue
    
    return jsonify({
        "success": True,
        "message": f"Found {len(leaflet_coords)} valid coordinates",
        "data": leaflet_coords
    }) 