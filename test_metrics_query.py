#!/usr/bin/env python3

from sqlalchemy import create_engine, text
from db.config import DB_URI

# Create SQLAlchemy engine
engine = create_engine(DB_URI)
trip_id = 313917

with engine.connect() as conn:
    # Test the updated query
    metrics_result = conn.execute(text("""
        SELECT 
            COUNT(*) as log_count,
            -- Use connection_type field instead of connection_status
            SUM(CASE WHEN json_extract(metrics, '$.connection.connection_type') = 'Disconnected' THEN 1 ELSE 0 END) as disconnected_count,
            COUNT(*) as connection_total,
            
            -- Check connection sub types
            SUM(CASE WHEN json_extract(metrics, '$.connection.connection_sub_type') = 'LTE' THEN 1 ELSE 0 END) as lte_count,
            SUM(CASE WHEN json_extract(metrics, '$.connection.connection_sub_type') = 'WIFI' THEN 1 ELSE 0 END) as wifi_count,
            SUM(CASE WHEN json_extract(metrics, '$.connection.connection_sub_type') = '5G' THEN 1 ELSE 0 END) as five_g_count,
            SUM(CASE WHEN json_extract(metrics, '$.connection.connection_sub_type') = '4G' THEN 1 ELSE 0 END) as four_g_count,
            SUM(CASE WHEN json_extract(metrics, '$.connection.connection_sub_type') = '3G' THEN 1 ELSE 0 END) as three_g_count,
            SUM(CASE WHEN json_extract(metrics, '$.connection.connection_sub_type') = '2G' THEN 1 ELSE 0 END) as two_g_count,
            COUNT(CASE WHEN json_extract(metrics, '$.connection.connection_sub_type') IS NOT NULL THEN 1 ELSE NULL END) as connection_sub_total,
            
            -- Check charging statuses
            SUM(CASE WHEN json_extract(metrics, '$.battery.charging_status') = 'DISCHARGING' THEN 1 ELSE 0 END) as discharging_count,
            SUM(CASE WHEN json_extract(metrics, '$.battery.charging_status') = 'CHARGING' THEN 1 ELSE 0 END) as charging_count,
            SUM(CASE WHEN json_extract(metrics, '$.battery.charging_status') = 'FULL' THEN 1 ELSE 0 END) as full_count,
            SUM(CASE WHEN json_extract(metrics, '$.battery.charging_status') = 'UNKNOWN' THEN 1 ELSE 0 END) as unknown_charging_count,
            COUNT(CASE WHEN json_extract(metrics, '$.battery.charging_status') IS NOT NULL THEN 1 ELSE NULL END) as charging_total,
            
            SUM(CASE WHEN json_extract(metrics, '$.gps') = 'false' OR json_extract(metrics, '$.gps') = '0' OR json_extract(metrics, '$.gps') = 0 THEN 1 ELSE 0 END) as gps_false_count,
            COUNT(CASE WHEN json_extract(metrics, '$.gps') IS NOT NULL THEN 1 ELSE NULL END) as gps_total,
            SUM(CASE WHEN json_extract(metrics, '$.location_permission') = 'FOREGROUND_FINE' OR json_extract(metrics, '$.location_permission') = 'FOREGROUND' THEN 1 ELSE 0 END) as foreground_fine_count,
            COUNT(CASE WHEN json_extract(metrics, '$.location_permission') IS NOT NULL THEN 1 ELSE NULL END) as permission_total,
            SUM(CASE WHEN json_extract(metrics, '$.battery.power_saving_mode') = 'false' OR json_extract(metrics, '$.battery.power_saving_mode') = '0' OR json_extract(metrics, '$.battery.power_saving_mode') = 0 THEN 1 ELSE 0 END) as power_saving_false_count,
            COUNT(CASE WHEN json_extract(metrics, '$.battery.power_saving_mode') IS NOT NULL THEN 1 ELSE NULL END) as power_saving_total,
            
            -- Get raw values for debugging
            json_extract(metrics, '$.connection.connection_sub_type') as sample_connection_sub_type,
            json_extract(metrics, '$.battery.charging_status') as sample_charging_status
        FROM trip_metrics
        WHERE trip_id = :trip_id
    """), {"trip_id": trip_id}).fetchone()
    
    print(f"Log Count: {metrics_result.log_count}")
    print(f"Connection Type (Disconnected): {metrics_result.disconnected_count} / {metrics_result.connection_total} = {(metrics_result.disconnected_count / metrics_result.connection_total * 100) if metrics_result.connection_total > 0 else 0:.2f}%")
    
    # Connection sub type details
    print("\nConnection Sub Type Details:")
    print(f"LTE: {metrics_result.lte_count}")
    print(f"WIFI: {metrics_result.wifi_count}")
    print(f"5G: {metrics_result.five_g_count}")
    print(f"4G: {metrics_result.four_g_count}")
    print(f"3G: {metrics_result.three_g_count}")
    print(f"2G: {metrics_result.two_g_count}")
    print(f"Total with sub type: {metrics_result.connection_sub_total}")
    print(f"Sample connection sub type: {metrics_result.sample_connection_sub_type}")
    
    # Calculate LTE percentage correctly
    if metrics_result.connection_total > 0:
        lte_percentage = (metrics_result.lte_count / metrics_result.connection_total) * 100
    else:
        lte_percentage = 0
    print(f"Connection Sub Type (LTE): {metrics_result.lte_count} / {metrics_result.connection_total} = {lte_percentage:.2f}%")
    
    # Charging status details
    print("\nCharging Status Details:")
    print(f"DISCHARGING: {metrics_result.discharging_count}")
    print(f"CHARGING: {metrics_result.charging_count}")
    print(f"FULL: {metrics_result.full_count}")
    print(f"UNKNOWN: {metrics_result.unknown_charging_count}")
    print(f"Total with charging status: {metrics_result.charging_total}")
    print(f"Sample charging status: {metrics_result.sample_charging_status}")
    
    # Calculate discharging percentage correctly
    if metrics_result.charging_total > 0:
        discharging_percentage = (metrics_result.discharging_count / metrics_result.charging_total) * 100
    else:
        discharging_percentage = 0
    print(f"Charging Status (Discharging): {metrics_result.discharging_count} / {metrics_result.charging_total} = {discharging_percentage:.2f}%")
    
    # Calculate unknown as discharging
    if metrics_result.charging_total > 0:
        unknown_as_discharging_percentage = ((metrics_result.discharging_count + metrics_result.unknown_charging_count) / metrics_result.charging_total) * 100
    else:
        unknown_as_discharging_percentage = 0
    print(f"Charging Status (Discharging + Unknown): {metrics_result.discharging_count + metrics_result.unknown_charging_count} / {metrics_result.charging_total} = {unknown_as_discharging_percentage:.2f}%")
    
    print(f"GPS Status (false): {metrics_result.gps_false_count} / {metrics_result.gps_total} = {(metrics_result.gps_false_count / metrics_result.gps_total * 100) if metrics_result.gps_total > 0 else 0:.2f}%")
    print(f"Location Permission (Foreground Fine): {metrics_result.foreground_fine_count} / {metrics_result.permission_total} = {(metrics_result.foreground_fine_count / metrics_result.permission_total * 100) if metrics_result.permission_total > 0 else 0:.2f}%")
    print(f"Power Saving Mode (False): {metrics_result.power_saving_false_count} / {metrics_result.power_saving_total} = {(metrics_result.power_saving_false_count / metrics_result.power_saving_total * 100) if metrics_result.power_saving_total > 0 else 0:.2f}%") 