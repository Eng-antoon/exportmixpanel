import device_metrics
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Trip ID to analyze
TRIP_ID = 304030

# Get data for the trip
print(f"Analyzing optimization status data for trip ID {TRIP_ID}")
data = device_metrics.get_device_metrics_by_trip(TRIP_ID)

# Check raw metrics data
raw_metrics = data.get('raw_metrics', [])
if raw_metrics:
    print(f"Found {len(raw_metrics)} raw metrics records")
    
    # Initialize counters
    optimization_status_count = {"true": 0, "false": 0, "unknown": 0}
    
    # Analyze each record
    for i, record in enumerate(raw_metrics):
        # Extract optimization status values
        
        # Get metrics JSON
        metrics_json = {}
        if isinstance(record.get('metrics'), str):
            try:
                metrics_json = json.loads(record.get('metrics', '{}'))
            except json.JSONDecodeError:
                metrics_json = {}
        else:
            metrics_json = record.get('metrics', {})
        
        # Extract battery data
        battery = metrics_json.get("battery", {}) if metrics_json else {}
        
        # Get optimization status from record first, then from nested battery
        opt_status = record.get("optimization_status")
        if opt_status is None and battery:
            opt_status = battery.get("optimization_status")
        
        # Print first 5 records for inspection
        if i < 5:
            print(f"\nRecord {i+1}:")
            print(f"  Direct optimization_status: {record.get('optimization_status')}")
            print(f"  Battery data: {battery}")
            print(f"  Nested optimization_status: {battery.get('optimization_status')}")
            print(f"  Final optimization_status used: {opt_status}")
            
            # Print the raw battery.power_saving_mode value too
            print(f"  power_saving_mode: {battery.get('power_saving_mode')}")
        
        # Count optimization status
        if opt_status == "true" or opt_status is True or opt_status == 1 or opt_status == "1":
            optimization_status_count["true"] += 1
        elif opt_status == "false" or opt_status is False or opt_status == 0 or opt_status == "0":
            optimization_status_count["false"] += 1
        else:
            optimization_status_count["unknown"] += 1
    
    # Print optimization status counts
    print("\nOptimization status counts:")
    total = sum(optimization_status_count.values())
    print(f"Total records counted: {total}")
    
    for status, count in optimization_status_count.items():
        percentage = round(count * 100 / total, 2) if total > 0 else 0
        print(f"  {status}: {count} ({percentage}%)")
    
    # Check power saving mode as well
    power_saving_count = {"true": 0, "false": 0, "unknown": 0}
    
    for record in raw_metrics:
        metrics_json = record.get('metrics', {})
        if isinstance(metrics_json, str):
            try:
                metrics_json = json.loads(metrics_json)
            except:
                metrics_json = {}
        
        battery = metrics_json.get("battery", {}) if metrics_json else {}
        power_saving = record.get("power_saving_mode")
        if power_saving is None and battery:
            power_saving = battery.get("power_saving_mode")
        
        if power_saving == "true" or power_saving is True or power_saving == 1 or power_saving == "1":
            power_saving_count["true"] += 1
        elif power_saving == "false" or power_saving is False or power_saving == 0 or power_saving == "0":
            power_saving_count["false"] += 1
        else:
            power_saving_count["unknown"] += 1
    
    print("\nPower saving mode counts:")
    total = sum(power_saving_count.values())
    print(f"Total records counted: {total}")
    
    for status, count in power_saving_count.items():
        percentage = round(count * 100 / total, 2) if total > 0 else 0
        print(f"  {status}: {count} ({percentage}%)")
else:
    print("No raw metrics data found")