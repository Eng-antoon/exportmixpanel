from flask import Blueprint, render_template, request, jsonify, current_app

device_bp = Blueprint('device_bp', __name__)

@device_bp.route("/device_metrics")
def device_metrics_page():
    # Delayed import for metabase client from app
    from app import metabase
    """
    Show device metrics dashboard.
    This will load the page without data initially, 
    requiring the user to enter a trip ID or select to view all data.
    """
    return render_template("device_metrics.html", 
                          metrics={"status": "no_data", "message": "Please enter a Trip ID to load data or click 'Show All Data'"},
                          metabase_connected=metabase.session_token is not None)

@device_bp.route("/api/device_metrics", methods=["GET"])
def get_device_metrics_api():
    # Delayed import for device_metrics module and app.logger (via current_app)
    from app import device_metrics
    
    trip_id_str = request.args.get('trip_id') # Keep as string for now
    
    if trip_id_str:
        try:
            trip_id = int(trip_id_str) # Validate and convert to int
            current_app.logger.info(f"Fetching device metrics for trip ID {trip_id}")
            metrics_data = device_metrics.get_device_metrics_by_trip(trip_id) # Renamed from metrics
            
            if not metrics_data or not isinstance(metrics_data, dict):
                current_app.logger.error(f"Invalid metrics response format for trip ID {trip_id}")
                # Return a default empty structure for charts
                return jsonify({
                    "status": "error", "message": "Invalid metrics data format from server",
                    "metrics": {"optimization_status": {"true": {"count": 0, "percentage": 0}, "false": {"count": 0, "percentage": 0}},
                                "connection_type": {"Connected": {"count": 0, "percentage": 0}, "Disconnected": {"count": 0, "percentage": 0}},
                                "location_permission": {"FOREGROUND": {"count": 0, "percentage": 0}, "BACKGROUND": {"count": 0, "percentage": 0}},
                                "total_records": 0}
                })
            
            # Determine records_count safely
            actual_metrics_content = metrics_data.get('metrics', {}) # Default to empty dict
            if isinstance(actual_metrics_content, list):
                records_count = len(actual_metrics_content)
            elif isinstance(actual_metrics_content, dict) and 'total_records' in actual_metrics_content:
                records_count = actual_metrics_content['total_records']
            else:
                records_count = 0
            
            current_app.logger.info(f"Retrieved metrics for trip ID {trip_id}: {metrics_data.get('status', 'unknown')}, record count: {records_count}")
            
            summary_metrics = {} # Renamed
            if metrics_data.get("status") == "success" and actual_metrics_content:
                if isinstance(actual_metrics_content, dict): # Already summarized
                    summary_metrics = metrics_data # Use the whole dict as it contains status and metrics key
                elif records_count > 0 and isinstance(actual_metrics_content, list): # Raw data list
                    summary_metrics = device_metrics.get_device_metrics_summary_from_data(actual_metrics_content)
                    # Ensure the summary_metrics from raw data also has the 'status' and 'metrics' structure
                    summary_metrics = {"status": "success", "metrics": summary_metrics} 
                else:
                    summary_metrics = {"status": "error", "message": "No valid metrics data found", "metrics": {}}
            else:
                current_app.logger.warning(f"No valid data found for trip ID {trip_id}: {metrics_data.get('message', 'Unknown error')}")
                summary_metrics = {"status": "error", "message": metrics_data.get("message", f"No valid data found for trip ID {trip_id}"), "metrics": {}}

            # Ensure a consistent structure for the frontend, especially for chart data
            # Ensure 'metrics' key exists and is a dictionary
            if 'metrics' not in summary_metrics or not isinstance(summary_metrics['metrics'], dict):
                summary_metrics['metrics'] = {}

            for key in ['optimization_status', 'connection_type', 'location_permission', 'power_saving_mode', 'charging_status', 'gps_status']:
                if key not in summary_metrics['metrics']:
                    summary_metrics['metrics'][key] = {} # Initialize as empty dict for chart keys
            
            # Ensure boolean keys for optimization_status
            if 'optimization_status' in summary_metrics['metrics']:
                opt_data = summary_metrics['metrics']['optimization_status']
                if 'true' not in opt_data: opt_data['true'] = {"count": 0, "percentage": 0}
                if 'false' not in opt_data: opt_data['false'] = {"count": 0, "percentage": 0}

            # Ensure keys for connection_type
            if 'connection_type' in summary_metrics['metrics']:
                conn_data = summary_metrics['metrics']['connection_type']
                if 'Connected' not in conn_data: conn_data['Connected'] = {"count": 0, "percentage": 0}
                if 'Disconnected' not in conn_data: conn_data['Disconnected'] = {"count": 0, "percentage": 0}
                if 'Unknown' not in conn_data: conn_data['Unknown'] = {"count": 0, "percentage": 0}
            
            # Ensure keys for location_permission
            if 'location_permission' in summary_metrics['metrics']:
                loc_data = summary_metrics['metrics']['location_permission']
                if 'FOREGROUND' not in loc_data: loc_data['FOREGROUND'] = {"count": 0, "percentage": 0}
                if 'BACKGROUND' not in loc_data: loc_data['BACKGROUND'] = {"count": 0, "percentage": 0}

            return jsonify(summary_metrics)

        except ValueError: # Handles int(trip_id_str) failure
            current_app.logger.error(f"Invalid trip ID format: {trip_id_str}")
            return jsonify({"status": "error", "message": "Invalid trip ID. Please enter a numeric value."})
        except Exception as e:
            current_app.logger.error(f"Error processing device metrics for trip {trip_id_str}: {str(e)}")
            # Return a default empty structure for charts
            return jsonify({
                "status": "error", "message": f"Error processing device metrics: {str(e)}",
                "metrics": {"optimization_status": {"true": {"count": 0, "percentage": 0}, "false": {"count": 0, "percentage": 0}},
                            "connection_type": {"Connected": {"count": 0, "percentage": 0}, "Disconnected": {"count": 0, "percentage": 0}},
                            "location_permission": {"FOREGROUND": {"count": 0, "percentage": 0}, "BACKGROUND": {"count": 0, "percentage": 0}},
                            "total_records": 0}
            })
    else:
        # Get all metrics if no trip_id is specified
        current_app.logger.info("Fetching metrics for all trips")
        from app import device_metrics # Delayed import
        metrics_summary_all = device_metrics.get_device_metrics_summary() # Renamed

        if not metrics_summary_all or not isinstance(metrics_summary_all, dict):
            current_app.logger.error("Invalid metrics response format for all trips")
            # Return default structure
            return jsonify({
                "status": "error", "message": "Invalid metrics data format from server (all trips)",
                 "metrics": {"optimization_status": {"true": {"count": 0, "percentage": 0}, "false": {"count": 0, "percentage": 0}}, # Changed to 0
                            "connection_type": {"Connected": {"count": 0, "percentage": 0}, "Disconnected": {"count": 0, "percentage": 0}}, # Changed to 0
                            "location_permission": {"FOREGROUND": {"count": 0, "percentage": 0}, "BACKGROUND": {"count": 0, "percentage": 0}}, # Changed to 0
                            "total_records": 0}
            })

        if 'metrics' not in metrics_summary_all or not isinstance(metrics_summary_all['metrics'], dict):
            metrics_summary_all['metrics'] = {}
            
        for key in ['optimization_status', 'connection_type', 'location_permission', 'power_saving_mode', 'charging_status', 'gps_status']:
            if key not in metrics_summary_all['metrics']:
                metrics_summary_all['metrics'][key] = {}
        
        # Ensure boolean keys for optimization_status
        if 'optimization_status' in metrics_summary_all['metrics']:
            opt_data = metrics_summary_all['metrics']['optimization_status']
            if 'true' not in opt_data: opt_data['true'] = {"count": 0, "percentage": 0}
            if 'false' not in opt_data: opt_data['false'] = {"count": 0, "percentage": 0}
        
        # Ensure keys for connection_type
        if 'connection_type' in metrics_summary_all['metrics']:
            conn_data = metrics_summary_all['metrics']['connection_type']
            if 'Connected' not in conn_data: conn_data['Connected'] = {"count": 0, "percentage": 0}
            if 'Disconnected' not in conn_data: conn_data['Disconnected'] = {"count": 0, "percentage": 0}
            if 'Unknown' not in conn_data: conn_data['Unknown'] = {"count": 0, "percentage": 0}

        # Ensure keys for location_permission
        if 'location_permission' in metrics_summary_all['metrics']:
            loc_data = metrics_summary_all['metrics']['location_permission']
            if 'FOREGROUND' not in loc_data: loc_data['FOREGROUND'] = {"count": 0, "percentage": 0}
            if 'BACKGROUND' not in loc_data: loc_data['BACKGROUND'] = {"count": 0, "percentage": 0}
            
        return jsonify(metrics_summary_all)
