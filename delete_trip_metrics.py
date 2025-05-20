#!/usr/bin/env python3
"""
Delete Trip Metrics Data

This script deletes all data from the trip_metrics table to allow testing the import feature.
"""

import logging
import sqlite3
import os
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

# Extract database path from DB_URI
db_path = DB_URI.replace('sqlite:///', '')
logger.info(f"Database path: {db_path}")

def delete_trip_metrics_data():
    """
    Delete all data from the trip_metrics table using SQLAlchemy.
    
    Returns:
        dict: Result of the operation
    """
    try:
        # Create a transaction
        with engine.begin() as connection:
            # Check if the table exists
            result = connection.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='trip_metrics'"))
            if not result.fetchone():
                logger.warning("trip_metrics table does not exist")
                return {
                    "status": "warning",
                    "message": "Trip metrics table does not exist. Nothing to delete."
                }
            
            # Get the count before deletion
            result = connection.execute(text("SELECT COUNT(*) FROM trip_metrics"))
            count_before = result.fetchone()[0]
            
            # Delete all records - use explicit transaction
            connection.execute(text("DELETE FROM trip_metrics"))
            
            # Reset SQLite sequence if needed
            connection.execute(text("DELETE FROM sqlite_sequence WHERE name='trip_metrics'"))
            
            # Verify deletion
            result = connection.execute(text("SELECT COUNT(*) FROM trip_metrics"))
            count_after = result.fetchone()[0]
            
            # Log the operation
            logger.info(f"Deleted {count_before} records from trip_metrics table. Remaining: {count_after}")
            
            return {
                "status": "success",
                "message": f"Successfully deleted {count_before} records from trip_metrics table.",
                "deleted_count": count_before,
                "remaining_count": count_after
            }
    except Exception as e:
        logger.error(f"Error deleting trip metrics data: {str(e)}")
        return {
            "status": "error",
            "message": f"Error deleting trip metrics data: {str(e)}"
        }

def delete_trip_metrics_data_sqlite():
    """
    Delete all data from the trip_metrics table using direct SQLite connection.
    
    Returns:
        dict: Result of the operation
    """
    try:
        # Ensure the database file exists
        if not os.path.exists(db_path):
            logger.error(f"Database file not found: {db_path}")
            return {
                "status": "error",
                "message": f"Database file not found: {db_path}"
            }
        
        # Connect directly to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trip_metrics'")
        if not cursor.fetchone():
            logger.warning("trip_metrics table does not exist")
            conn.close()
            return {
                "status": "warning",
                "message": "Trip metrics table does not exist. Nothing to delete."
            }
        
        # Get the count before deletion
        cursor.execute("SELECT COUNT(*) FROM trip_metrics")
        count_before = cursor.fetchone()[0]
        
        # Delete all records
        cursor.execute("DELETE FROM trip_metrics")
        
        # Reset SQLite sequence
        cursor.execute("DELETE FROM sqlite_sequence WHERE name='trip_metrics'")
        
        # Commit the transaction
        conn.commit()
        
        # Verify deletion
        cursor.execute("SELECT COUNT(*) FROM trip_metrics")
        count_after = cursor.fetchone()[0]
        
        # Close the connection
        conn.close()
        
        # Log the operation
        logger.info(f"[SQLite] Deleted {count_before} records from trip_metrics table. Remaining: {count_after}")
        
        return {
            "status": "success",
            "message": f"Successfully deleted {count_before} records from trip_metrics table using SQLite.",
            "deleted_count": count_before,
            "remaining_count": count_after
        }
    except Exception as e:
        logger.error(f"Error deleting trip metrics data using SQLite: {str(e)}")
        return {
            "status": "error",
            "message": f"Error deleting trip metrics data using SQLite: {str(e)}"
        }

if __name__ == "__main__":
    # Execute the deletion using both methods
    print("Method 1: Using SQLAlchemy")
    result1 = delete_trip_metrics_data()
    print(f"Status: {result1['status']}")
    print(f"Message: {result1['message']}")
    if result1['status'] == "success":
        print(f"Deleted: {result1['deleted_count']} records")
        print(f"Remaining: {result1['remaining_count']} records")
    
    print("\nMethod 2: Using direct SQLite connection")
    result2 = delete_trip_metrics_data_sqlite()
    print(f"Status: {result2['status']}")
    print(f"Message: {result2['message']}")
    if result2['status'] == "success":
        print(f"Deleted: {result2['deleted_count']} records")
        print(f"Remaining: {result2['remaining_count']} records") 