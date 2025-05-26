#!/usr/bin/env python3

from sqlalchemy import create_engine, text
from db.config import DB_URI
import json

# Create SQLAlchemy engine
engine = create_engine(DB_URI)
trip_id = 314156

with engine.connect() as conn:
    # Check connection status distribution
    result = conn.execute(text("""
        SELECT json_extract(metrics, '$.connection.connection_status'), COUNT(*) 
        FROM trip_metrics 
        WHERE trip_id = :trip_id 
        GROUP BY json_extract(metrics, '$.connection.connection_status')
    """), {"trip_id": trip_id})
    
    print("Connection Status Distribution:")
    for row in result:
        print(f"{row[0]}: {row[1]}")
    
    # Check connection type distribution
    result = conn.execute(text("""
        SELECT json_extract(metrics, '$.connection.connection_type'), COUNT(*) 
        FROM trip_metrics 
        WHERE trip_id = :trip_id 
        GROUP BY json_extract(metrics, '$.connection.connection_type')
    """), {"trip_id": trip_id})
    
    print("\nConnection Type Distribution:")
    for row in result:
        print(f"{row[0]}: {row[1]}")
    
    # Check location permission distribution
    result = conn.execute(text("""
        SELECT json_extract(metrics, '$.location_permission'), COUNT(*) 
        FROM trip_metrics 
        WHERE trip_id = :trip_id 
        GROUP BY json_extract(metrics, '$.location_permission')
    """), {"trip_id": trip_id})
    
    print("\nLocation Permission Distribution:")
    for row in result:
        print(f"{row[0]}: {row[1]}")
    
    # Check power saving mode distribution with type information
    result = conn.execute(text("""
        SELECT json_extract(metrics, '$.battery.power_saving_mode'), 
               typeof(json_extract(metrics, '$.battery.power_saving_mode')),
               COUNT(*) 
        FROM trip_metrics 
        WHERE trip_id = :trip_id 
        GROUP BY json_extract(metrics, '$.battery.power_saving_mode'), typeof(json_extract(metrics, '$.battery.power_saving_mode'))
    """), {"trip_id": trip_id})
    
    print("\nPower Saving Mode Distribution (with type):")
    for row in result:
        print(f"{row[0]} (type: {row[1]}): {row[2]}")
    
    # Check raw metrics for a sample record
    result = conn.execute(text("""
        SELECT metrics
        FROM trip_metrics 
        WHERE trip_id = :trip_id 
        LIMIT 1
    """), {"trip_id": trip_id})
    
    row = result.fetchone()
    if row:
        metrics_json = json.loads(row[0])
        print("\nSample Record Metrics Structure:")
        print(json.dumps(metrics_json, indent=2))
    
    # Compare the queries
    print("\nDevice Metrics Query Result:")
    result = conn.execute(text("""
        SELECT 
            COUNT(*) as log_count,
            SUM(CASE WHEN json_extract(metrics, '$.connection.connection_status') IS NULL OR json_extract(metrics, '$.connection.connection_status') = 'Disconnected' THEN 1 ELSE 0 END) as disconnected_count,
            COUNT(*) as connection_total,
            SUM(CASE WHEN json_extract(metrics, '$.connection.connection_type') = 'Connected' THEN 1 ELSE 0 END) as connected_count
        FROM trip_metrics
        WHERE trip_id = :trip_id
    """), {"trip_id": trip_id})
    
    row = result.fetchone()
    print(f"Log Count: {row.log_count}")
    print(f"Disconnected Count: {row.disconnected_count}")
    print(f"Connection Total: {row.connection_total}")
    print(f"Disconnected Percentage: {(row.disconnected_count / row.connection_total * 100) if row.connection_total > 0 else 0}%")
    print(f"Connected Count: {row.connected_count}")
    print(f"Connected Percentage: {(row.connected_count / row.connection_total * 100) if row.connection_total > 0 else 0}%") 