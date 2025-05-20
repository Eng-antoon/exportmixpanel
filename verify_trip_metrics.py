#!/usr/bin/env python3
"""
Verify Trip Metrics Data

This script verifies that all chart data for a specific trip has the correct total count.
"""

import logging
import json
import sqlite3
from db.config import DB_URI
import device_metrics

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Trip ID to verify
TRIP_ID = 304030

def verify_trip_metrics(trip_id):
    """
    Verify that all chart data for a specific trip has the correct total count.
    
    Args:
        trip_id (int): Trip ID to verify
    """
    logger.info(f"Verifying trip metrics for trip ID {trip_id}")
    
    # Get data from the database
    db_path = DB_URI.replace('sqlite:///', '')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM trip_metrics WHERE trip_id = ?", (trip_id,))
    total_count = cursor.fetchone()[0]
    logger.info(f"Total records in database for trip {trip_id}: {total_count}")
    
    # Get connection_type distribution
    cursor.execute("""
        SELECT 
            json_extract(metrics, '$.connection.connection_type') as connection_type,
            COUNT(*) as count
        FROM trip_metrics 
        WHERE trip_id = ?
        GROUP BY json_extract(metrics, '$.connection.connection_type')
    """, (trip_id,))
    connection_type_counts = cursor.fetchall()
    logger.info("Connection type distribution from database:")
    db_connection_type_total = 0
    for conn_type, count in connection_type_counts:
        percentage = round(count * 100 / total_count, 2) if total_count > 0 else 0
        logger.info(f"  {conn_type or 'None'}: {count} ({percentage}%)")
        db_connection_type_total += count
    
    # Get connection_status distribution
    cursor.execute("""
        SELECT 
            json_extract(metrics, '$.connection.connection_status') as connection_status,
            COUNT(*) as count
        FROM trip_metrics 
        WHERE trip_id = ?
        GROUP BY json_extract(metrics, '$.connection.connection_status')
    """, (trip_id,))
    connection_status_counts = cursor.fetchall()
    logger.info("Connection status distribution from database:")
    db_connection_status_total = 0
    for conn_status, count in connection_status_counts:
        percentage = round(count * 100 / total_count, 2) if total_count > 0 else 0
        logger.info(f"  {conn_status or 'None'}: {count} ({percentage}%)")
        db_connection_status_total += count
    
    # Get optimization_status distribution
    cursor.execute("""
        SELECT 
            json_extract(metrics, '$.battery.optimization_status') as optimization_status,
            COUNT(*) as count
        FROM trip_metrics 
        WHERE trip_id = ?
        GROUP BY json_extract(metrics, '$.battery.optimization_status')
    """, (trip_id,))
    optimization_status_counts = cursor.fetchall()
    logger.info("Optimization status distribution from database:")
    db_optimization_status_total = 0
    for status, count in optimization_status_counts:
        percentage = round(count * 100 / total_count, 2) if total_count > 0 else 0
        logger.info(f"  {status or 'None'}: {count} ({percentage}%)")
        db_optimization_status_total += count
    
    # Get power_saving_mode distribution
    cursor.execute("""
        SELECT 
            json_extract(metrics, '$.battery.power_saving_mode') as power_saving_mode,
            COUNT(*) as count
        FROM trip_metrics 
        WHERE trip_id = ?
        GROUP BY json_extract(metrics, '$.battery.power_saving_mode')
    """, (trip_id,))
    power_saving_mode_counts = cursor.fetchall()
    logger.info("Power saving mode distribution from database:")
    db_power_saving_mode_total = 0
    for mode, count in power_saving_mode_counts:
        percentage = round(count * 100 / total_count, 2) if total_count > 0 else 0
        logger.info(f"  {mode or 'None'}: {count} ({percentage}%)")
        db_power_saving_mode_total += count
    
    # Get location_permission distribution
    cursor.execute("""
        SELECT 
            json_extract(metrics, '$.location_permission') as location_permission,
            COUNT(*) as count
        FROM trip_metrics 
        WHERE trip_id = ?
        GROUP BY json_extract(metrics, '$.location_permission')
    """, (trip_id,))
    location_permission_counts = cursor.fetchall()
    logger.info("Location permission distribution from database:")
    db_location_permission_total = 0
    for permission, count in location_permission_counts:
        percentage = round(count * 100 / total_count, 2) if total_count > 0 else 0
        logger.info(f"  {permission or 'None'}: {count} ({percentage}%)")
        db_location_permission_total += count
    
    # Get gps distribution
    cursor.execute("""
        SELECT 
            json_extract(metrics, '$.gps') as gps,
            COUNT(*) as count
        FROM trip_metrics 
        WHERE trip_id = ?
        GROUP BY json_extract(metrics, '$.gps')
    """, (trip_id,))
    gps_counts = cursor.fetchall()
    logger.info("GPS distribution from database:")
    db_gps_total = 0
    for gps, count in gps_counts:
        percentage = round(count * 100 / total_count, 2) if total_count > 0 else 0
        logger.info(f"  {gps or 'None'}: {count} ({percentage}%)")
        db_gps_total += count
    
    conn.close()
    
    # Now get data from the API
    logger.info("\nGetting data from device_metrics API...")
    data = device_metrics.get_device_metrics_by_trip(trip_id)
    
    if data["status"] == "success":
        metrics = data["metrics"]
        
        # Check connection_type
        api_connection_type_total = sum(item["count"] for item in metrics.get("connection_type", {}).values())
        logger.info(f"API connection_type total: {api_connection_type_total}")
        
        # Check optimization_status
        api_optimization_status_total = sum(item["count"] for item in metrics.get("optimization_status", {}).values())
        logger.info(f"API optimization_status total: {api_optimization_status_total}")
        
        # Check power_saving_mode
        api_power_saving_mode_total = sum(item["count"] for item in metrics.get("power_saving_mode", {}).values())
        logger.info(f"API power_saving_mode total: {api_power_saving_mode_total}")
        
        # Check location_permission
        api_location_permission_total = sum(item["count"] for item in metrics.get("location_permission", {}).values())
        logger.info(f"API location_permission total: {api_location_permission_total}")
        
        # Check gps_status
        api_gps_status_total = sum(item["count"] for item in metrics.get("gps_status", {}).values())
        logger.info(f"API gps_status total: {api_gps_status_total}")
        
        # Verify totals
        logger.info("\nVerifying totals:")
        logger.info(f"Database total records: {total_count}")
        logger.info(f"Database connection_type total: {db_connection_type_total} (Match: {db_connection_type_total == total_count})")
        logger.info(f"Database connection_status total: {db_connection_status_total} (Match: {db_connection_status_total == total_count})")
        logger.info(f"Database optimization_status total: {db_optimization_status_total} (Match: {db_optimization_status_total == total_count})")
        logger.info(f"Database power_saving_mode total: {db_power_saving_mode_total} (Match: {db_power_saving_mode_total == total_count})")
        logger.info(f"Database location_permission total: {db_location_permission_total} (Match: {db_location_permission_total == total_count})")
        logger.info(f"Database gps total: {db_gps_total} (Match: {db_gps_total == total_count})")
        
        logger.info(f"API connection_type total: {api_connection_type_total} (Match: {api_connection_type_total == total_count})")
        logger.info(f"API optimization_status total: {api_optimization_status_total} (Match: {api_optimization_status_total == total_count})")
        logger.info(f"API power_saving_mode total: {api_power_saving_mode_total} (Match: {api_power_saving_mode_total == total_count})")
        logger.info(f"API location_permission total: {api_location_permission_total} (Match: {api_location_permission_total == total_count})")
        logger.info(f"API gps_status total: {api_gps_status_total} (Match: {api_gps_status_total == total_count})")
    else:
        logger.error(f"Error getting data from API: {data.get('message')}")

if __name__ == "__main__":
    verify_trip_metrics(TRIP_ID) 