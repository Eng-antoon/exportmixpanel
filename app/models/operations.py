from app import db_session
from db.models import Trip, Tag, Base
from app.api.client import fetch_trip_from_api, fetch_coordinates_count, fetch_trip_coordinates
from app.utils.trip_analysis import determine_completed_by, calculate_trip_time
from app.utils.helpers import analyze_trip_segments
from app import engine
from sqlalchemy import and_, or_, func
import math

def migrate_db():
    """
    Create database tables from models if they don't exist.
    """
    try:
        print("Creating database tables from models...")
        Base.metadata.create_all(bind=engine)
        print("Database tables created successfully")
    except Exception as e:
        print(f"Error during database migration: {e}")

def update_trip_db(trip_id, force_update=False):
    """
    Update trip information in the database.
    
    Args:
        trip_id: ID of the trip to update
        force_update: Whether to force update even if the trip exists
        
    Returns:
        Dictionary with update status
    """
    result = {"success": False, "message": ""}
    
    try:
        # Check if the trip exists in our database
        trip = db_session.query(Trip).filter(Trip.trip_id == trip_id).first()
        
        # If trip exists and we're not forcing an update, return early
        if trip and not force_update:
            result["success"] = True
            result["message"] = "Trip already exists in database"
            return result
            
        # Fetch trip data from the API
        trip_data = fetch_trip_from_api(trip_id)
        if not trip_data:
            result["message"] = "Failed to fetch trip data from API"
            return result
            
        # Helper function to safely extract values
        def is_valid(value):
            return value is not None and value != ""
        
        # Extract trip details
        api_distance = None
        try:
            # The API sometimes returns distance as a string with "km" suffix
            raw_distance = trip_data.get("distance", "")
            if isinstance(raw_distance, str) and "km" in raw_distance:
                api_distance = float(raw_distance.replace("km", "").strip())
            elif is_valid(raw_distance):
                api_distance = float(raw_distance)
        except (ValueError, TypeError):
            pass
            
        status = trip_data.get("status", "").lower()
        
        # Calculate additional trip metrics
        coordinates_count = fetch_coordinates_count(trip_id)
        
        # Analyze trip activity if available
        activity = trip_data.get("activity", [])
        completed_by = determine_completed_by(activity)
        trip_time = calculate_trip_time(activity)
        
        # If trip doesn't exist, create a new one
        if not trip:
            trip = Trip(trip_id=trip_id)
            db_session.add(trip)
        
        # Update trip fields
        trip.calculated_distance = api_distance
        trip.status = status
        trip.coordinate_count = coordinates_count
        trip.completed_by = completed_by
        trip.trip_time = trip_time
        
        # If we have coordinates and the trip is completed, analyze the segments
        if coordinates_count > 0 and status == "completed":
            coordinates = fetch_trip_coordinates(trip_id)
            if coordinates:
                segment_analysis = analyze_trip_segments(coordinates)
                trip.short_segments_count = segment_analysis["short_segments_count"]
                trip.medium_segments_count = segment_analysis["medium_segments_count"] 
                trip.long_segments_count = segment_analysis["long_segments_count"]
                trip.short_segments_distance = segment_analysis["short_segments_distance"]
                trip.medium_segments_distance = segment_analysis["medium_segments_distance"]
                trip.long_segments_distance = segment_analysis["long_segments_distance"]
                trip.max_segment_distance = segment_analysis["max_segment_distance"]
                trip.avg_segment_distance = segment_analysis["avg_segment_distance"]
        
        db_session.commit()
        result["success"] = True
        result["message"] = "Trip updated successfully"
        
    except Exception as e:
        db_session.rollback()
        result["message"] = f"Error updating trip: {str(e)}"
        print(f"Error updating trip {trip_id}: {str(e)}")
    
    return result

def get_all_trips():
    """
    Get all trips from the database.
    
    Returns:
        List of Trip objects
    """
    return db_session.query(Trip).all()

def get_trip_by_id(trip_id):
    """
    Get a specific trip by ID.
    
    Args:
        trip_id: ID of the trip to fetch
        
    Returns:
        Trip object or None if not found
    """
    return db_session.query(Trip).filter(Trip.trip_id == trip_id).first()

def get_trip_tags(trip_id):
    """
    Get all tags for a specific trip.
    
    Args:
        trip_id: ID of the trip to get tags for
        
    Returns:
        List of Tag objects
    """
    trip = db_session.query(Trip).filter(Trip.trip_id == trip_id).first()
    if trip:
        return trip.tags
    return []

def add_tag_to_trip(trip_id, tag_name):
    """
    Add a tag to a trip. Create the tag if it doesn't exist.
    
    Args:
        trip_id: ID of the trip to tag
        tag_name: Name of the tag to add
        
    Returns:
        True if successful, False otherwise
    """
    try:
        trip = db_session.query(Trip).filter(Trip.trip_id == trip_id).first()
        if not trip:
            return False
            
        # Get or create the tag
        tag = db_session.query(Tag).filter(func.lower(Tag.name) == func.lower(tag_name)).first()
        if not tag:
            tag = Tag(name=tag_name)
            db_session.add(tag)
            
        # Add tag to trip if not already there
        if tag not in trip.tags:
            trip.tags.append(tag)
            
            # Special handling for 'lack of accuracy' tag
            if tag_name.lower() == "lack of accuracy":
                trip.lack_of_accuracy = True
            
        db_session.commit()
        return True
        
    except Exception as e:
        db_session.rollback()
        print(f"Error adding tag to trip: {e}")
        return False

def remove_tag_from_trip(trip_id, tag_name):
    """
    Remove a tag from a trip.
    
    Args:
        trip_id: ID of the trip to remove tag from
        tag_name: Name of the tag to remove
        
    Returns:
        True if successful, False otherwise
    """
    try:
        trip = db_session.query(Trip).filter(Trip.trip_id == trip_id).first()
        if not trip:
            return False
            
        tag = db_session.query(Tag).filter(func.lower(Tag.name) == func.lower(tag_name)).first()
        if tag and tag in trip.tags:
            trip.tags.remove(tag)
            
            # Special handling for 'lack of accuracy' tag
            if tag_name.lower() == "lack of accuracy":
                trip.lack_of_accuracy = False
            
        db_session.commit()
        return True
        
    except Exception as e:
        db_session.rollback()
        print(f"Error removing tag from trip: {e}")
        return False

def get_all_tags():
    """
    Get all tags from the database.
    
    Returns:
        List of Tag objects
    """
    return db_session.query(Tag).all()

def create_tag(tag_name):
    """
    Create a new tag.
    
    Args:
        tag_name: Name of the tag to create
        
    Returns:
        Tag object if created, None if tag already exists or on error
    """
    try:
        # Check if tag already exists
        existing = db_session.query(Tag).filter(func.lower(Tag.name) == func.lower(tag_name)).first()
        if existing:
            return None
            
        tag = Tag(name=tag_name)
        db_session.add(tag)
        db_session.commit()
        return tag
        
    except Exception as e:
        db_session.rollback()
        print(f"Error creating tag: {e}")
        return None

def delete_tag(tag_name):
    """
    Delete a tag.
    
    Args:
        tag_name: Name of the tag to delete
        
    Returns:
        True if successful, False otherwise
    """
    try:
        tag = db_session.query(Tag).filter(func.lower(Tag.name) == func.lower(tag_name)).first()
        if tag:
            db_session.delete(tag)
            db_session.commit()
            return True
        return False
        
    except Exception as e:
        db_session.rollback()
        print(f"Error deleting tag: {e}")
        return False

def update_trip_route_quality(trip_id, quality):
    """
    Update the route quality of a trip.
    
    Args:
        trip_id: ID of the trip to update
        quality: New route quality value
        
    Returns:
        True if successful, False otherwise
    """
    try:
        trip = db_session.query(Trip).filter(Trip.trip_id == trip_id).first()
        if trip:
            trip.route_quality = quality
            db_session.commit()
            return True
        return False
        
    except Exception as e:
        db_session.rollback()
        print(f"Error updating route quality: {e}")
        return False 