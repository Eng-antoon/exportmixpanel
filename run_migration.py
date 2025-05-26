#!/usr/bin/env python3
"""
Run migration script to ensure all database columns are created.
This script will explicitly run the migrate_db function to add any missing columns.
"""

import os
import sys
import logging
from sqlalchemy import create_engine, text, inspect
from db.config import DB_URI
from db.models import Base

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create SQLAlchemy engine
engine = create_engine(DB_URI)

def migrate_db():
    try:
        print("Creating database tables from models...")
        Base.metadata.create_all(bind=engine)
        print("Database tables created successfully")
        
        # Add missing columns if they don't exist
        connection = engine.connect()
        inspector = inspect(engine)
        
        # First, verify the trips table exists
        tables = inspector.get_table_names()
        if 'trips' not in tables:
            print("Trips table does not exist yet - it will be created with all columns")
            connection.close()
            return
        
        existing_columns = [column['name'] for column in inspector.get_columns('trips')]
        print(f"Existing columns: {existing_columns}")
        
        columns_to_add = {
            'pickup_success_rate': 'FLOAT',
            'dropoff_success_rate': 'FLOAT',
            'total_points_success_rate': 'FLOAT',
            'locations_trip_points': 'INTEGER',
            'driver_trip_points': 'INTEGER',
            'autoending': 'BOOLEAN',
            'driver_app_interactions_per_trip': 'FLOAT',
            'driver_app_interaction_rate': 'FLOAT',
            'trip_points_interaction_ratio': 'FLOAT'
        }
        
        # Add each missing column
        for column_name, column_type in columns_to_add.items():
            if column_name not in existing_columns:
                try:
                    print(f"Adding {column_name} column to trips table")
                    connection.execute(text(f"ALTER TABLE trips ADD COLUMN {column_name} {column_type}"))
                    connection.commit()
                    print(f"Successfully added {column_name} column")
                except Exception as e:
                    print(f"Error adding {column_name} column: {e}")
                    connection.rollback()
            else:
                print(f"Column {column_name} already exists")
        
        connection.close()
        print("Database migration completed")
    except Exception as e:
        logger.error(f"Migration error: {e}")
        print(f"Error during database migration: {e}")

if __name__ == "__main__":
    print("Running database migration...")
    migrate_db()
    print("Migration completed.") 