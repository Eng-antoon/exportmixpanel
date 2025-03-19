import sqlite3
import os

def migrate_data():
    print("Starting data migration from backup database...")
    
    # Check if backup file exists
    backup_file = "my_dashboard.db.bak"
    if not os.path.exists(backup_file):
        print(f"Backup file {backup_file} not found.")
        return False
    
    # Connect to both databases
    try:
        src_conn = sqlite3.connect(backup_file)
        src_cursor = src_conn.cursor()
        
        dest_conn = sqlite3.connect("my_dashboard.db")
        dest_cursor = dest_conn.cursor()
        
        # Get all trips from the source database
        src_cursor.execute("SELECT * FROM trips")
        trips = src_cursor.fetchall()
        print(f"Found {len(trips)} trips in the backup database.")
        
        # Get column names from source table
        src_cursor.execute("PRAGMA table_info(trips)")
        src_columns = [row[1] for row in src_cursor.fetchall()]
        print(f"Source columns: {src_columns}")
        
        # Get column names from destination table
        dest_cursor.execute("PRAGMA table_info(trips)")
        dest_columns = [row[1] for row in dest_cursor.fetchall()]
        print(f"Destination columns: {dest_columns}")
        
        # Find common columns for insert
        common_columns = [col for col in src_columns if col in dest_columns]
        print(f"Common columns for migration: {common_columns}")
        
        # Prepare the insert statement
        placeholders = ", ".join(["?"] * len(common_columns))
        insert_sql = f"INSERT INTO trips ({', '.join(common_columns)}) VALUES ({placeholders})"
        
        # Insert each trip into the new database
        for trip in trips:
            # Extract data for common columns only
            values = [trip[src_columns.index(col)] for col in common_columns]
            try:
                dest_cursor.execute(insert_sql, values)
            except sqlite3.IntegrityError as e:
                print(f"Error inserting trip: {e}")
                continue
        
        # Commit the changes
        dest_conn.commit()
        print(f"Successfully migrated {dest_cursor.rowcount if dest_cursor.rowcount >= 0 else 'all'} trips.")
        
        # Check for tags table in source
        src_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tags'")
        if src_cursor.fetchone():
            # Migrate tags
            src_cursor.execute("SELECT * FROM tags")
            tags = src_cursor.fetchall()
            print(f"Found {len(tags)} tags in the backup database.")
            
            # Get column names from source tags table
            src_cursor.execute("PRAGMA table_info(tags)")
            src_tag_columns = [row[1] for row in src_cursor.fetchall()]
            
            # Get column names from destination tags table
            dest_cursor.execute("PRAGMA table_info(tags)")
            dest_tag_columns = [row[1] for row in dest_cursor.fetchall()]
            
            # Find common columns for insert
            common_tag_columns = [col for col in src_tag_columns if col in dest_tag_columns]
            
            if common_tag_columns:
                # Prepare the insert statement for tags
                tag_placeholders = ", ".join(["?"] * len(common_tag_columns))
                tag_insert_sql = f"INSERT INTO tags ({', '.join(common_tag_columns)}) VALUES ({tag_placeholders})"
                
                # Insert each tag into the new database
                for tag in tags:
                    # Extract data for common columns only
                    tag_values = [tag[src_tag_columns.index(col)] for col in common_tag_columns]
                    try:
                        dest_cursor.execute(tag_insert_sql, tag_values)
                    except sqlite3.IntegrityError as e:
                        print(f"Error inserting tag: {e}")
                        continue
                
                # Commit the changes
                dest_conn.commit()
                print(f"Successfully migrated {len(tags)} tags.")
        
        # Check for trip_tags table in source
        src_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trip_tags'")
        if src_cursor.fetchone():
            # Migrate trip_tags relationships
            src_cursor.execute("SELECT * FROM trip_tags")
            trip_tags = src_cursor.fetchall()
            print(f"Found {len(trip_tags)} trip_tag relationships in the backup database.")
            
            # Get column names from source trip_tags table
            src_cursor.execute("PRAGMA table_info(trip_tags)")
            src_tt_columns = [row[1] for row in src_cursor.fetchall()]
            
            # Get column names from destination trip_tags table
            dest_cursor.execute("PRAGMA table_info(trip_tags)")
            dest_tt_columns = [row[1] for row in dest_cursor.fetchall()]
            
            # Find common columns for insert
            common_tt_columns = [col for col in src_tt_columns if col in dest_tt_columns]
            
            if common_tt_columns:
                # Prepare the insert statement for trip_tags
                tt_placeholders = ", ".join(["?"] * len(common_tt_columns))
                tt_insert_sql = f"INSERT INTO trip_tags ({', '.join(common_tt_columns)}) VALUES ({tt_placeholders})"
                
                # Insert each relationship into the new database
                for rel in trip_tags:
                    # Extract data for common columns only
                    rel_values = [rel[src_tt_columns.index(col)] for col in common_tt_columns]
                    try:
                        dest_cursor.execute(tt_insert_sql, rel_values)
                    except sqlite3.IntegrityError as e:
                        print(f"Error inserting trip_tag relation: {e}")
                        continue
                
                # Commit the changes
                dest_conn.commit()
                print(f"Successfully migrated {len(trip_tags)} trip_tag relationships.")
        
        print("Data migration completed successfully.")
        return True
    
    except Exception as e:
        print(f"Error during data migration: {e}")
        return False
    
    finally:
        if 'src_conn' in locals():
            src_conn.close()
        if 'dest_conn' in locals():
            dest_conn.close()

if __name__ == "__main__":
    migrate_data() 