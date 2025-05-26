import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from db.models import Trip

# Load mixpanel data
print("Loading mixpanel data...")
try:
    df = pd.read_excel("mixpanel_export.xlsx")
    print(f"Total rows in mixpanel_export.xlsx: {len(df)}")
    
    # Check if 'event' column exists and contains 'trip_details_route'
    if 'event' in df.columns:
        print(f"Number of trip_details_route events: {len(df[df['event'] == 'trip_details_route'])}")
    else:
        print("'event' column not found in mixpanel_export.xlsx")
    
    # Check if 'tripId' column exists
    if 'tripId' in df.columns:
        print(f"Number of unique trip IDs in mixpanel data: {df['tripId'].nunique()}")
        print(f"Sample trip IDs: {df['tripId'].head(5).tolist()}")
    else:
        print("'tripId' column not found in mixpanel_export.xlsx")
        print(f"Available columns: {df.columns.tolist()}")
except Exception as e:
    print(f"Error loading mixpanel data: {e}")

# Load trip data from database
print("\nLoading trip data from database...")
try:
    # Create database connection
    engine = create_engine('sqlite:///my_dashboard.db')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Get all trips
    trips = session.query(Trip).all()
    print(f"Total trips in database: {len(trips)}")
    
    # Get filtered trips (calculated_distance <= 600)
    filtered_trips = []
    for trip in trips:
        try:
            cd = float(trip.calculated_distance or 0)
            if cd <= 600:
                filtered_trips.append(trip)
        except:
            continue
    
    print(f"Filtered trips (calc_distance <= 600): {len(filtered_trips)}")
    
    # Check trip_id formats
    if len(trips) > 0:
        print(f"Sample trip_id values: {[trip.trip_id for trip in trips[:5]]}")
        print(f"Sample trip_id types: {[type(trip.trip_id) for trip in trips[:5]]}")
    
    # Show trips with locations_trip_points
    trips_with_points = [trip for trip in trips if trip.locations_trip_points is not None and trip.locations_trip_points > 0]
    print(f"Trips with location points: {len(trips_with_points)}")
    
    # Clean up
    session.close()
except Exception as e:
    print(f"Error loading trip data: {e}")

# Check for matching IDs
print("\nChecking for matching IDs between mixpanel and database...")
try:
    # Convert mixpanel tripId to strings for comparison
    if 'tripId' in df.columns:
        mixpanel_trip_ids = set(str(tid) for tid in df['tripId'].unique())
        
        # Convert database trip_id to strings
        db_trip_ids = set(str(trip.trip_id) for trip in trips)
        
        # Find matches
        matching_ids = mixpanel_trip_ids.intersection(db_trip_ids)
        
        print(f"Number of unique trip IDs in mixpanel: {len(mixpanel_trip_ids)}")
        print(f"Number of unique trip IDs in database: {len(db_trip_ids)}")
        print(f"Number of matching trip IDs: {len(matching_ids)}")
        
        if len(matching_ids) > 0:
            print(f"Sample matching IDs: {list(matching_ids)[:5]}")
        else:
            print("No matching IDs found!")
            
            # Show sample IDs from both sources
            print(f"Sample mixpanel IDs: {list(mixpanel_trip_ids)[:5]}")
            print(f"Sample database IDs: {list(db_trip_ids)[:5]}")
except Exception as e:
    print(f"Error checking matching IDs: {e}") 