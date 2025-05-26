import sqlite3
import os

# Define the database path
db_path = os.path.join('data', 'trips.db')

def migrate_database():
    """
    Add missing columns for driver app interaction metrics to the trips table
    """
    print("Starting database migration to add driver app interaction metrics columns...")
    
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check if the columns already exist
        cursor.execute("PRAGMA table_info(trips)")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Add driver_app_interactions_per_trip column if it doesn't exist
        if 'driver_app_interactions_per_trip' not in columns:
            print("Adding driver_app_interactions_per_trip column...")
            cursor.execute('ALTER TABLE trips ADD COLUMN driver_app_interactions_per_trip FLOAT')
        else:
            print("Column driver_app_interactions_per_trip already exists.")
        
        # Add driver_app_interaction_rate column if it doesn't exist
        if 'driver_app_interaction_rate' not in columns:
            print("Adding driver_app_interaction_rate column...")
            cursor.execute('ALTER TABLE trips ADD COLUMN driver_app_interaction_rate FLOAT')
        else:
            print("Column driver_app_interaction_rate already exists.")
        
        # Add trip_points_interaction_ratio column if it doesn't exist
        if 'trip_points_interaction_ratio' not in columns:
            print("Adding trip_points_interaction_ratio column...")
            cursor.execute('ALTER TABLE trips ADD COLUMN trip_points_interaction_ratio FLOAT')
        else:
            print("Column trip_points_interaction_ratio already exists.")
        
        # Commit the changes
        conn.commit()
        print("Database migration completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error during migration: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database() 