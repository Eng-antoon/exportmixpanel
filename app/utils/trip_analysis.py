from datetime import datetime

def determine_completed_by(activity_list):
    """
    Inspects an activity list to find the latest event where the status changes to 'completed'
    and returns the corresponding user_type (admin or driver), or None if not found.
    
    Args:
        activity_list: List of activity events from the API.
        
    Returns:
        String representing who completed the trip (admin/driver) or None if not found.
    """
    best_candidate = None
    best_time = None
    for event in activity_list:
        changes = event.get("changes", {})
        status_change = changes.get("status")
        if status_change and isinstance(status_change, list) and len(status_change) >= 2:
            if str(status_change[-1]).lower() == "completed":
                created_str = event.get("created_at", "").replace(" UTC", "")
                event_time = None
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]:
                    try:
                        event_time = datetime.strptime(created_str, fmt)
                        break
                    except ValueError:
                        continue
                if event_time:
                    if best_time is None or event_time > best_time:
                        best_time = event_time
                        best_candidate = event
    if best_candidate:
        return best_candidate.get("user_type", None)
    return None

def calculate_trip_time(activity_list):
    """
    Calculate the total trip time in minutes from a list of trip activity events.
    
    Args:
        activity_list: List of activity events from the API.
        
    Returns:
        Trip time in minutes or None if cannot be calculated.
    """
    if not activity_list:
        return None
    
    # Find the earliest enroute event and the latest completed event
    enroute_time = None
    completed_time = None
    
    for event in activity_list:
        created_str = event.get("created_at", "").replace(" UTC", "")
        event_time = None
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]:
            try:
                event_time = datetime.strptime(created_str, fmt)
                break
            except ValueError:
                continue
                
        if not event_time:
            continue
            
        changes = event.get("changes", {})
        status_change = changes.get("status")
        
        if status_change and isinstance(status_change, list) and len(status_change) >= 2:
            if str(status_change[-1]).lower() == "enroute" and (enroute_time is None or event_time < enroute_time):
                enroute_time = event_time
            elif str(status_change[-1]).lower() == "completed" and (completed_time is None or event_time > completed_time):
                completed_time = event_time
    
    if enroute_time and completed_time and completed_time > enroute_time:
        # Calculate time difference in minutes
        time_diff = (completed_time - enroute_time).total_seconds() / 60
        return round(time_diff, 2)
        
    return None 