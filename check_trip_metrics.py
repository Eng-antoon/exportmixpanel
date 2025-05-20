#!/usr/bin/env python3
"""
Check Trip Metrics Data

This script checks the actual record count in the trip_metrics table.
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

def check_trip_metrics_sqlalchemy():
    """
    Check trip_metrics table using SQLAlchemy.
    """
    try:
        with engine.connect() as connection:
            # Check if the table exists
            result = connection.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='trip_metrics'"))
            if not result.fetchone():
                logger.warning("trip_metrics table does not exist")
                return "Table does not exist"
            
            # Get record count
            result = connection.execute(text("SELECT COUNT(*) FROM trip_metrics"))
            count = result.fetchone()[0]
            
            # Get sample records
            result = connection.execute(text("SELECT * FROM trip_metrics LIMIT 5"))
            sample_records = result.fetchall()
            
            return {
                "count": count,
                "sample_records": sample_records if sample_records else []
            }
    except Exception as e:
        logger.error(f"Error checking trip metrics with SQLAlchemy: {str(e)}")
        return f"Error: {str(e)}"

def check_trip_metrics_sqlite():
    """
    Check trip_metrics table using direct SQLite connection.
    """
    try:
        # Ensure the database file exists
        if not os.path.exists(db_path):
            logger.error(f"Database file not found: {db_path}")
            return f"Database file not found: {db_path}"
        
        # Connect directly to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trip_metrics'")
        if not cursor.fetchone():
            logger.warning("trip_metrics table does not exist")
            conn.close()
            return "Table does not exist"
        
        # Get record count
        cursor.execute("SELECT COUNT(*) FROM trip_metrics")
        count = cursor.fetchone()[0]
        
        # Get sample records
        cursor.execute("SELECT * FROM trip_metrics LIMIT 5")
        sample_records = cursor.fetchall()
        
        # Close the connection
        conn.close()
        
        return {
            "count": count,
            "sample_records": sample_records if sample_records else []
        }
    except Exception as e:
        logger.error(f"Error checking trip metrics with SQLite: {str(e)}")
        return f"Error: {str(e)}"

if __name__ == "__main__":
    print("Checking trip_metrics table with SQLAlchemy:")
    result1 = check_trip_metrics_sqlalchemy()
    if isinstance(result1, dict):
        print(f"Record count: {result1['count']}")
        print(f"Sample records: {len(result1['sample_records'])}")
    else:
        print(result1)
    
    print("\nChecking trip_metrics table with SQLite:")
    result2 = check_trip_metrics_sqlite()
    if isinstance(result2, dict):
        print(f"Record count: {result2['count']}")
        print(f"Sample records: {len(result2['sample_records'])}")
    else:
        print(result2) 