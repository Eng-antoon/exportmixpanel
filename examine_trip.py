import device_metrics
import json
import logging
import sqlite3
from db.config import DB_URI

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Trip ID to analyze
TRIP_ID = 304030

# Get data from the device_metrics module
print(f"Analyzing data for trip ID {TRIP_ID}")
data = device_metrics.get_device_metrics_by_trip(TRIP_ID)

print(f"Data status: {data['status']}")
print(f"Message: {data.get('message', 'No message')}")
print(f"Total count: {data.get('total_count', 0)}")
print(f"Sampled count: {data.get('sampled_count', 0)}")

# Check if metrics is a list (raw data) or dictionary (processed data)
metrics = data['metrics']
print(f"Type of metrics: {type(metrics)}")

if isinstance(metrics, list):
    print(f"Found {len(metrics)} raw metrics records")
    
    # Initialize counters
    connection_type_count = {"Connected": 0, "Disconnected": 0, "Unknown": 0}
    
    # Analyze each record
    for i, record in enumerate(metrics):
        # Print first 5 records for inspection
        if i < 5:
            print(f"\nRecord {i+1}:")
            print(f"  connection_status: {record.get('connection_status')}")
            
            # Check if metrics is a JSON string or dict
            metrics_json = {}
            if isinstance(record.get('metrics'), str):
                try:
                    metrics_json = json.loads(record.get('metrics', '{}'))
                except json.JSONDecodeError:
                    metrics_json = {}
            else:
                metrics_json = record.get('metrics', {})
            
            # Get connection data from nested structure
            connection = metrics_json.get('connection', {})
            print(f"  Nested connection data: {connection}")
            conn_status = record.get("connection_status") or connection.get("connection_status")
            print(f"  Final connection_status used: {conn_status}")
        
        # Count connection types
        metrics_json = {}
        if isinstance(record.get('metrics'), str):
            try:
                metrics_json = json.loads(record.get('metrics', '{}'))
            except json.JSONDecodeError:
                metrics_json = {}
        else:
            metrics_json = record.get('metrics', {})
        
        connection = metrics_json.get('connection', {})
        conn_status = record.get("connection_status") or connection.get("connection_status")
        conn_type = connection.get("connection_type")
        
        # Using the same logic as in our updated device_metrics.py
        if conn_type == "Connected":
            connection_type_count["Connected"] += 1
        elif conn_type == "Disconnected":
            connection_type_count["Disconnected"] += 1
        elif conn_status == "Connected" or conn_status == "MOBILE" or conn_status == "WIFI":
            connection_type_count["Connected"] += 1
        elif conn_status == "Disconnected" or conn_status is None or conn_status == "":
            connection_type_count["Disconnected"] += 1
        else:
            connection_type_count["Unknown"] += 1
            # If we have an unknown status, print it for debugging
            if i < 20:  # Limit to first 20 unknown records to avoid too much output
                print(f"\nUnknown connection status in record {i+1}:")
                print(f"  connection_status: {conn_status}")
                print(f"  connection_type: {conn_type}")
                print(f"  record connection_status: {record.get('connection_status')}")
                print(f"  nested connection_status: {connection.get('connection_status')}")
    
    # Print connection type counts
    print("\nConnection type counts:")
    connection_total = sum(connection_type_count.values())
    print(f"Total records counted: {connection_total}")
    
    for status, count in connection_type_count.items():
        percentage = round(count * 100 / connection_total, 2) if connection_total > 0 else 0
        print(f"  {status}: {count} ({percentage}%)")
else:
    # If metrics is already processed data
    print("Processed metrics data found")
    connection_type_data = metrics.get('connection_type', {})
    print(f"Connection type data: {connection_type_data}")

# Now query the database directly to verify
print("\n--- Querying database directly ---")
db_path = DB_URI.replace('sqlite:///', '')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get total count
cursor.execute("SELECT COUNT(*) FROM trip_metrics WHERE trip_id = ?", (TRIP_ID,))
total_count = cursor.fetchone()[0]
print(f"Total records in database for trip {TRIP_ID}: {total_count}")

# Get connection_type distribution
cursor.execute("""
    SELECT 
        json_extract(metrics, '$.connection.connection_type') as connection_type,
        COUNT(*) as count
    FROM trip_metrics 
    WHERE trip_id = ?
    GROUP BY json_extract(metrics, '$.connection.connection_type')
""", (TRIP_ID,))
connection_type_counts = cursor.fetchall()
print("\nConnection type distribution from database:")
for conn_type, count in connection_type_counts:
    percentage = round(count * 100 / total_count, 2) if total_count > 0 else 0
    print(f"  {conn_type or 'None'}: {count} ({percentage}%)")

# Get connection_status distribution
cursor.execute("""
    SELECT 
        json_extract(metrics, '$.connection.connection_status') as connection_status,
        COUNT(*) as count
    FROM trip_metrics 
    WHERE trip_id = ?
    GROUP BY json_extract(metrics, '$.connection.connection_status')
""", (TRIP_ID,))
connection_status_counts = cursor.fetchall()
print("\nConnection status distribution from database:")
for conn_status, count in connection_status_counts:
    percentage = round(count * 100 / total_count, 2) if total_count > 0 else 0
    print(f"  {conn_status or 'None'}: {count} ({percentage}%)")

conn.close() 