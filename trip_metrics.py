#!/usr/bin/env python3
"""
Trip Metrics Module

This module provides functions to fetch and store trip metrics data from Metabase question 5717.
"""

import logging
import json
import sqlite3
from datetime import datetime
from metabase_client import MetabaseClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from db.config import DB_URI

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create SQLAlchemy engine
engine = create_engine(DB_URI)

def create_trip_metrics_table():
    """
    Create the trip_metrics table if it doesn't exist.
    """
    try:
        with engine.connect() as connection:
            # Check if the table exists
            result = connection.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='trip_metrics'"))
            if not result.fetchone():
                logger.info("Creating trip_metrics table")
                connection.execute(text("""
                    CREATE TABLE trip_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trip_id INTEGER,
                        battery_level REAL,
                        charging_status TEXT,
                        optimization_status BOOLEAN,
                        power_saving_mode BOOLEAN,
                        connection_status TEXT,
                        connection_sub_type TEXT,
                        connection_type TEXT,
                        gps BOOLEAN,
                        accuracy REAL,
                        altitude REAL,
                        latitude REAL,
                        longitude REAL,
                        logged_at REAL,
                        location_permission TEXT,
                        notification_permission BOOLEAN,
                        number_of_trip_logs INTEGER,
                        metrics TEXT,
                        created_at TEXT,
                        updated_at TEXT,
                        received_at TEXT
                    )
                """))
                
                # Create index on trip_id for faster lookups
                connection.execute(text("CREATE INDEX idx_trip_metrics_trip_id ON trip_metrics(trip_id)"))
                
                # Create index on logged_at for faster lookups
                connection.execute(text("CREATE INDEX idx_trip_metrics_logged_at ON trip_metrics(logged_at)"))
                
                # Create composite index on trip_id and logged_at for faster duplicate checks
                connection.execute(text("CREATE UNIQUE INDEX idx_trip_metrics_trip_id_logged_at ON trip_metrics(trip_id, logged_at)"))
                
                logger.info("trip_metrics table created successfully")
            else:
                logger.info("trip_metrics table already exists")
                
                # Check if the composite index exists
                result = connection.execute(text("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_trip_metrics_trip_id_logged_at'"))
                if not result.fetchone():
                    logger.info("Creating composite index on trip_id and logged_at")
                    try:
                        # Try to create the unique index, but it might fail if there are duplicates
                        connection.execute(text("CREATE UNIQUE INDEX idx_trip_metrics_trip_id_logged_at ON trip_metrics(trip_id, logged_at)"))
                    except Exception as e:
                        logger.warning(f"Could not create unique index due to duplicates: {str(e)}")
                        logger.info("Will clean up duplicates first")
                        clean_up_duplicates()
                        # Try again after cleanup
                        try:
                            connection.execute(text("CREATE UNIQUE INDEX idx_trip_metrics_trip_id_logged_at ON trip_metrics(trip_id, logged_at)"))
                            logger.info("Successfully created unique index after duplicate cleanup")
                        except Exception as e:
                            logger.error(f"Still could not create unique index after cleanup: {str(e)}")
    except Exception as e:
        logger.error(f"Error creating trip_metrics table: {str(e)}")
        raise

def clean_up_duplicates():
    """
    Clean up duplicate records in the trip_metrics table.
    This function keeps only one record for each trip_id and logged_at combination.
    """
    logger.info("Starting duplicate cleanup process")
    try:
        # Create a temporary table with unique records
        with engine.begin() as connection:
            connection.execute(text("""
                CREATE TEMPORARY TABLE temp_trip_metrics AS
                SELECT MIN(id) as id
                FROM trip_metrics
                GROUP BY trip_id, logged_at
            """))
            
            # Get count of records to keep
            result = connection.execute(text("SELECT COUNT(*) FROM temp_trip_metrics"))
            unique_count = result.scalar()
            
            # Get total count before deletion
            result = connection.execute(text("SELECT COUNT(*) FROM trip_metrics"))
            total_count = result.scalar()
            
            duplicates_count = total_count - unique_count
            logger.info(f"Found {duplicates_count} duplicate records out of {total_count} total records")
            
            if duplicates_count > 0:
                # Delete all records that are not in the temporary table
                connection.execute(text("""
                    DELETE FROM trip_metrics
                    WHERE id NOT IN (SELECT id FROM temp_trip_metrics)
                """))
                
                logger.info(f"Successfully deleted {duplicates_count} duplicate records")
            
            # Drop the temporary table
            connection.execute(text("DROP TABLE temp_trip_metrics"))
        
        # VACUUM must be executed outside of a transaction
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text("VACUUM"))
            
        logger.info("Duplicate cleanup completed successfully")
        return duplicates_count
    except Exception as e:
        logger.error(f"Error cleaning up duplicates: {str(e)}")
        raise

def fetch_trip_metrics_from_metabase(question_id=5717, max_retries=3):
    """
    Fetch trip metrics data from Metabase question 5717.
    
    Args:
        question_id (int): Metabase question ID
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        list: List of dictionaries containing trip metrics data
    """
    client = MetabaseClient()
    logger.info(f"Fetching trip metrics data from Metabase question {question_id}")
    
    # Use the export endpoint to fetch potentially large datasets
    response_data = client.get_question_data_export(question_id, parameters=None, format="json", max_retries=max_retries)
    
    if not response_data:
        logger.error(f"No data received from Metabase question {question_id}")
        return []
    
    # Log a sample of the response data for debugging
    if isinstance(response_data, list) and len(response_data) > 0:
        sample_record = response_data[0]
        logger.info(f"Sample record from Metabase: {json.dumps(sample_record, indent=2)}")
        logger.info(f"Sample record keys: {list(sample_record.keys())}")
        
        # If there's trip_id 304030, log a sample of that
        for idx, record in enumerate(response_data[:100]):  # Check first 100 records
            if record.get('trip_id') == 304030:
                logger.info(f"Found sample record for trip_id 304030: {json.dumps(record, indent=2)}")
                break
    
    # Process the response into a more usable format
    try:
        # Case 1: If the response is already a list of dictionaries (direct format)
        if isinstance(response_data, list):
            logger.info(f"Successfully processed {len(response_data)} trip metrics from Metabase (direct format)")
            return response_data
        
        # Case 2: If the response has the standard "data" with "rows" and "cols" structure
        if isinstance(response_data, dict) and "data" in response_data:
            if "rows" in response_data["data"] and "cols" in response_data["data"]:
                rows = response_data["data"]["rows"]
                cols = response_data["data"]["cols"]
                
                # Create a list of dictionaries, each representing a trip metric
                results = []
                
                for row in rows:
                    metric = {}
                    for i, col in enumerate(cols):
                        col_name = col.get("display_name") or col.get("name", f"column_{i}")
                        metric[col_name] = row[i]
                    
                    results.append(metric)
                
                logger.info(f"Successfully processed {len(results)} trip metrics from Metabase (standard format)")
                return results
                
        # Log the actual response format for debugging
        logger.error(f"Unexpected data format from Metabase: {str(response_data)[:200]}...")
        return []
        
    except Exception as e:
        logger.error(f"Error processing Metabase response: {str(e)}")
        logger.exception("Detailed error:")
        return []

def store_trip_metrics(metrics_data):
    """
    Store trip metrics data in the database.
    
    Args:
        metrics_data (list): List of dictionaries containing trip metrics data
        
    Returns:
        int: Number of records inserted/updated
    """
    if not metrics_data:
        logger.warning("No metrics data to store")
        return 0
    
    try:
        # Create table if it doesn't exist
        create_trip_metrics_table()
        
        # Clean up any existing duplicates before inserting new data
        clean_up_duplicates()
        
        count = 0
        skipped_count = 0
        batch_size = 5000  # Process records in batches for better performance
        
        # Log a sample record for debugging
        if len(metrics_data) > 0:
            sample_record = metrics_data[0]
            logger.info(f"Sample record for storage: {json.dumps(sample_record, indent=2)[:500]}...")
            logger.info(f"Sample record keys: {list(sample_record.keys())}")
        
        # Create a dictionary to track existing records
        existing_records = {}
        
        # Fetch existing records for duplicate checking
        logger.info("Fetching existing records for duplicate checking...")
        with engine.connect() as connection:
            result = connection.execute(text("SELECT trip_id, logged_at FROM trip_metrics"))
            for row in result:
                # Use a tuple of trip_id and logged_at as the key
                key = (row[0], row[1])
                existing_records[key] = True
        
        logger.info(f"Found {len(existing_records)} existing records for duplicate checking")
        
        # Process records in batches
        total_records = len(metrics_data)
        logger.info(f"Processing {total_records} records in batches of {batch_size}")
        
        for i in range(0, total_records, batch_size):
            batch = metrics_data[i:i+batch_size]
            batch_records = []
            
            for metric in batch:
                # Skip records without trip_id
                if "trip_id" not in metric or metric["trip_id"] is None:
                    skipped_count += 1
                    if skipped_count <= 5:
                        logger.warning(f"Skipping record without trip_id: {json.dumps(metric, indent=2)[:200]}...")
                    continue
                
                # Extract values from the record - handling Metabase's special field naming
                battery_level = metric.get("battery_level")
                if battery_level is None:
                    battery_level = metric.get("metrics → battery → battery_level")
                    
                charging_status = metric.get("charging_status")
                if charging_status is None:
                    charging_status = metric.get("metrics → battery → charging_status")
                    
                optimization_status = metric.get("optimization_status")
                if optimization_status is None:
                    optimization_status = metric.get("metrics → battery → optimization_status")
                    
                power_saving_mode = metric.get("power_saving_mode")
                if power_saving_mode is None:
                    power_saving_mode = metric.get("metrics → battery → power_saving_mode")
                    
                connection_status = metric.get("connection_status")
                if connection_status is None:
                    connection_status = metric.get("metrics → connection → connection_status")
                    
                connection_sub_type = metric.get("connection_sub_type")
                if connection_sub_type is None:
                    connection_sub_type = metric.get("metrics → connection → connection_sub_type")
                    
                connection_type = metric.get("connection_type")
                if connection_type is None:
                    connection_type = metric.get("metrics → connection → connection_type") or metric.get("metrics → connection_type")
                    
                gps = metric.get("gps")
                if gps is None:
                    gps = metric.get("metrics → gps")
                    
                accuracy = metric.get("accuracy")
                if accuracy is None:
                    accuracy = metric.get("metrics → location → accuracy")
                    
                altitude = metric.get("altitude")
                if altitude is None:
                    altitude = metric.get("metrics → location → altitude")
                    
                latitude = metric.get("latitude")
                if latitude is None:
                    latitude = metric.get("metrics → location → latitude")
                    
                longitude = metric.get("longitude")
                if longitude is None:
                    longitude = metric.get("metrics → location → longitude")
                    
                # Get logged_at from multiple possible fields
                logged_at = metric.get("logged_at") 
                if logged_at is None:
                    logged_at = metric.get("metrics → location → logged_at")
                    
                location_permission = metric.get("location_permission")
                if location_permission is None:
                    location_permission = metric.get("metrics → location_permission")
                    
                notification_permission = metric.get("notification_permission")
                if notification_permission is None:
                    notification_permission = metric.get("metrics → notification_permission")
                    
                number_of_trip_logs = metric.get("number_of_trip_logs")
                if number_of_trip_logs is None:
                    number_of_trip_logs = metric.get("metrics → number_of_trip_logs")
                
                # Fallback to parsing metrics JSON if it exists
                if all(x is None for x in [battery_level, charging_status, optimization_status, power_saving_mode, 
                                           connection_status, connection_sub_type, connection_type, 
                                           accuracy, altitude, latitude, longitude, logged_at]) and "metrics" in metric:
                    try:
                        # Try to parse as JSON
                        if isinstance(metric["metrics"], str):
                            parsed_metrics = json.loads(metric["metrics"])
                            if isinstance(parsed_metrics, dict):
                                # Extract data from the JSON structure
                                battery = parsed_metrics.get("battery", {}) or {}
                                connection = parsed_metrics.get("connection", {}) or {}
                                location = parsed_metrics.get("location", {}) or {}
                                
                                battery_level = battery_level or battery.get("battery_level")
                                charging_status = charging_status or battery.get("charging_status")
                                optimization_status = optimization_status or battery.get("optimization_status")
                                power_saving_mode = power_saving_mode or battery.get("power_saving_mode")
                                
                                connection_status = connection_status or connection.get("connection_status")
                                connection_sub_type = connection_sub_type or connection.get("connection_sub_type")
                                connection_type = connection_type or connection.get("connection_type")
                                
                                accuracy = accuracy or location.get("accuracy")
                                altitude = altitude or location.get("altitude")
                                latitude = latitude or location.get("latitude")
                                longitude = longitude or location.get("longitude")
                                logged_at = logged_at or location.get("logged_at")
                                
                                gps = gps or parsed_metrics.get("gps")
                                location_permission = location_permission or parsed_metrics.get("location_permission")
                                notification_permission = notification_permission or parsed_metrics.get("notification_permission")
                                number_of_trip_logs = number_of_trip_logs or parsed_metrics.get("number_of_trip_logs")
                    except Exception as e:
                        logger.warning(f"Error parsing metrics JSON: {str(e)}")
                
                # Construct metrics JSON with all available data
                metrics_json = {
                    "battery": {
                        "battery_level": battery_level,
                        "charging_status": charging_status,
                        "optimization_status": optimization_status,
                        "power_saving_mode": power_saving_mode
                    },
                    "connection": {
                        "connection_status": connection_status,
                        "connection_sub_type": connection_sub_type,
                        "connection_type": connection_type
                    },
                    "location": {
                        "accuracy": accuracy,
                        "altitude": altitude,
                        "latitude": latitude,
                        "longitude": longitude,
                        "logged_at": logged_at
                    },
                    "gps": gps,
                    "location_permission": location_permission,
                    "notification_permission": notification_permission,
                    "number_of_trip_logs": number_of_trip_logs
                }
                
                # Convert metrics JSON to string
                metrics_json_str = json.dumps(metrics_json)
                
                # Convert datetime objects to strings
                for field in ["created_at", "updated_at", "received_at"]:
                    if field in metric and isinstance(metric[field], datetime):
                        metric[field] = metric[field].isoformat()
                
                # Skip if we don't have enough data to uniquely identify the record
                if metric.get("trip_id") is None or logged_at is None:
                    skipped_count += 1
                    continue
                
                # Check if this record already exists in our dictionary
                key = (metric.get("trip_id"), logged_at)
                if key in existing_records:
                    continue  # Skip this record as it already exists
                
                # Add to our batch for insertion
                batch_records.append({
                    "trip_id": metric.get("trip_id"),
                    "battery_level": battery_level,
                    "charging_status": charging_status,
                    "optimization_status": optimization_status,
                    "power_saving_mode": power_saving_mode,
                    "connection_status": connection_status,
                    "connection_sub_type": connection_sub_type,
                    "connection_type": connection_type,
                    "gps": gps,
                    "accuracy": accuracy,
                    "altitude": altitude,
                    "latitude": latitude,
                    "longitude": longitude,
                    "logged_at": logged_at,
                    "location_permission": location_permission,
                    "notification_permission": notification_permission,
                    "number_of_trip_logs": number_of_trip_logs,
                    "metrics": metrics_json_str,
                    "created_at": metric.get("created_at"),
                    "updated_at": metric.get("updated_at"),
                    "received_at": metric.get("received_at")
                })
                
                # Also add to our dictionary to avoid duplicates within the same batch
                existing_records[key] = True
            
            # Batch insert records
            if batch_records:
                with engine.begin() as connection:
                    # Use INSERT OR IGNORE to skip duplicates that might have been added by another process
                    connection.execute(text("""
                        INSERT OR IGNORE INTO trip_metrics (
                            trip_id, battery_level, charging_status, optimization_status,
                            power_saving_mode, connection_status, connection_sub_type,
                            connection_type, gps, accuracy, altitude, latitude, longitude,
                            logged_at, location_permission, notification_permission,
                            number_of_trip_logs, metrics, created_at, updated_at, received_at
                        ) VALUES (
                            :trip_id, :battery_level, :charging_status, :optimization_status,
                            :power_saving_mode, :connection_status, :connection_sub_type,
                            :connection_type, :gps, :accuracy, :altitude, :latitude, :longitude,
                            :logged_at, :location_permission, :notification_permission,
                            :number_of_trip_logs, :metrics, :created_at, :updated_at, :received_at
                        )
                    """), batch_records)
                
                count += len(batch_records)
                logger.info(f"Inserted batch of {len(batch_records)} records. Total processed: {count}/{total_records}")
        
        logger.info(f"Successfully stored {count} trip metrics records, skipped {skipped_count} records")
        return count
    
    except Exception as e:
        logger.error(f"Error storing trip metrics: {str(e)}")
        raise

def import_trip_metrics():
    """
    Main function to fetch and store trip metrics data.
    
    Returns:
        dict: Result of the import operation
    """
    try:
        start_time = datetime.now()
        client = MetabaseClient()
        
        # First check if the question exists and get its metadata
        question_id = 5717
        question_metadata = client.get_question_metadata(question_id)
        
        if question_metadata:
            logger.info(f"Question {question_id} found. Type: {question_metadata.get('type', 'unknown')}, Name: {question_metadata.get('name', 'unknown')}")
        else:
            logger.error(f"Could not retrieve metadata for question {question_id}. The question might not exist or you might not have access.")
            return {
                "status": "error",
                "message": f"Could not access Metabase question {question_id}. Please check if it exists and you have access.",
                "records": 0,
                "duration": str(datetime.now() - start_time)
            }
        
        # First try using the export endpoint
        metrics_data = fetch_trip_metrics_from_metabase(question_id)
        
        # If export fails, try the regular API endpoint as fallback
        if not metrics_data:
            logger.info("Attempting to fetch data using regular API endpoint as fallback")
            response_data = client.get_question_data(question_id, parameters=None, max_retries=3)
            
            if response_data and "data" in response_data and "rows" in response_data["data"]:
                rows = response_data["data"]["rows"]
                cols = response_data["data"]["cols"]
                
                # Create a list of dictionaries, each representing a trip metric
                metrics_data = []
                
                for row in rows:
                    metric = {}
                    for i, col in enumerate(cols):
                        col_name = col.get("display_name") or col.get("name", f"column_{i}")
                        metric[col_name] = row[i]
                    
                    metrics_data.append(metric)
                
                logger.info(f"Successfully retrieved {len(metrics_data)} trip metrics using regular API endpoint")
        
        if not metrics_data:
            return {
                "status": "error",
                "message": "No trip metrics data fetched from Metabase",
                "records": 0,
                "duration": str(datetime.now() - start_time)
            }
        
        records_count = store_trip_metrics(metrics_data)
        duration = datetime.now() - start_time
        
        return {
            "status": "success",
            "message": f"Successfully imported {records_count} trip metrics records",
            "records": records_count,
            "duration": str(duration)
        }
    
    except Exception as e:
        logger.error(f"Error importing trip metrics: {str(e)}")
        return {
            "status": "error",
            "message": f"Error importing trip metrics: {str(e)}",
            "records": 0,
            "duration": str(datetime.now() - start_time) if 'start_time' in locals() else "N/A"
        }

if __name__ == "__main__":
    # If run directly, import trip metrics data
    result = import_trip_metrics()
    print(json.dumps(result, indent=2)) 