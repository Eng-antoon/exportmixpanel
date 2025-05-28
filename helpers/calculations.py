import math
from datetime import datetime # Needed for determine_completed_by and calculate_trip_time

# Copied from app.py
CARRIER_GROUPS = {
    "Vodafone": ["vodafone", "voda fone", "tegi ne3eesh"],
    "Orange": ["orange", "orangeeg", "orange eg"],
    "Etisalat": ["etisalat", "e& etisalat", "e&"],
    "We": ["we"]
}

def normalize_carrier(carrier_name):
    if not carrier_name:
        return ""
    lower = carrier_name.lower().strip()
    for group, variants in CARRIER_GROUPS.items():
        for variant in variants:
            if variant in lower:
                return group
    return carrier_name.title()

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

def calculate_expected_trip_quality(
    logs_count, 
    lack_of_accuracy, 
    medium_segments_count, 
    long_segments_count, 
    short_dist_total, 
    medium_dist_total, 
    long_dist_total,
    calculated_distance
):
    epsilon = 1e-2
    if (short_dist_total + medium_dist_total + long_dist_total) <= 0 or logs_count <= 1:
        return "No Logs Trip"
    if logs_count < 5 and medium_segments_count == 0 and long_segments_count == 0:
        return "No Logs Trip"
    if logs_count < 50 and (medium_segments_count >= 1 or long_segments_count >= 1):
        return "Trip Points Only Exist"
    if logs_count < 50 and (medium_segments_count == 0 or long_segments_count == 0): # Corrected condition based on original
        return "Low Quality Trip"
    
    logs_factor = min(logs_count / 500.0, 1.0)
    ratio = short_dist_total / (medium_dist_total + long_dist_total + epsilon)
    
    if ratio >= 5: segment_factor = 1.0
    elif ratio <= 0.5: segment_factor = 0.0
    else: segment_factor = (ratio - 0.5) / 4.5
    
    quality_score = 0.5 * logs_factor + 0.5 * segment_factor
    if lack_of_accuracy: quality_score *= 0.8

    if quality_score >= 0.8 and (medium_dist_total + long_dist_total) <= 0.05 * (calculated_distance or 0): # Ensure calculated_distance is not None
        return "High Quality Trip"
    elif quality_score >= 0.8: #This was an elif in original, but makes more sense as general moderate for high scores
        return "Moderate Quality Trip"
    # Original logic had an else for Low Quality Trip, which is fine.
    # Re-evaluating the final condition from original:
    # if quality_score >= 0.8 and (medium_dist_total + long_dist_total) <= 0.05*calculated_distance:
    #     return "High Quality Trip"
    # elif quality_score >= 0.8: # This means Q >= 0.8 but the medium/long dist condition for High was false
    #     return "Moderate Quality Trip"
    # else: # This means Q < 0.8
    #     return "Low Quality Trip"
    # The above seems correct. The current implementation matches this.
    # The original had:
    #    if quality_score >= 0.8 and (medium_dist_total + long_dist_total) <= 0.05*calculated_distance:
    #        return "High Quality Trip"
    #    elif quality_score >= 0.8:  <-- This implies previous was false
    #        return "Moderate Quality Trip"
    #    else: # Q < 0.8
    #        return "Low Quality Trip"
    # This is correct.
    # My previous version had a slight deviation in the final condition's structure but achieved the same.
    # Restoring to be closer to original structure for clarity:
    if quality_score >= 0.8:
        if (medium_dist_total + long_dist_total) <= 0.05 * (calculated_distance or epsilon): # Use epsilon if calc_dist is 0 or None
             return "High Quality Trip"
        else:
             return "Moderate Quality Trip" # If score is high but too many med/long segments
    else: # quality_score < 0.8
        return "Low Quality Trip"


def analyze_trip_segments(coordinates):
    # Uses haversine_distance
    if not coordinates or len(coordinates) < 2:
        return {
            "short_segments_count": 0, "medium_segments_count": 0, "long_segments_count": 0,
            "short_segments_distance": 0, "medium_segments_distance": 0, "long_segments_distance": 0,
            "max_segment_distance": 0, "avg_segment_distance": 0
        }
    coords = [[float(point[1]), float(point[0])] for point in coordinates]
    # ... (rest of the function logic from app.py)
    short_segments_count = 0; medium_segments_count = 0; long_segments_count = 0
    short_segments_distance = 0.0; medium_segments_distance = 0.0; long_segments_distance = 0.0
    max_segment_distance = 0.0; total_distance = 0.0; segment_count = 0
    
    for i in range(len(coords) - 1):
        lat1, lon1 = coords[i]
        lat2, lon2 = coords[i+1]
        distance = haversine_distance(lat1, lon1, lat2, lon2)
        segment_count += 1
        total_distance += distance
        
        if distance < 1: short_segments_count += 1; short_segments_distance += distance
        elif distance <= 5: medium_segments_count += 1; medium_segments_distance += distance
        else: long_segments_count += 1; long_segments_distance += distance
            
        if distance > max_segment_distance: max_segment_distance = distance
            
    avg_segment_distance = total_distance / segment_count if segment_count > 0 else 0
    
    return {
        "short_segments_count": short_segments_count,
        "medium_segments_count": medium_segments_count,
        "long_segments_count": long_segments_count,
        "short_segments_distance": round(short_segments_distance, 2),
        "medium_segments_distance": round(medium_segments_distance, 2),
        "long_segments_distance": round(long_segments_distance, 2),
        "max_segment_distance": round(max_segment_distance, 2),
        "avg_segment_distance": round(avg_segment_distance, 2)
    }

def determine_completed_by(activity_list):
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
                    except ValueError: continue
                if event_time and (best_time is None or event_time > best_time):
                    best_time = event_time
                    best_candidate = event
    if best_candidate: return best_candidate.get("user_type", None)
    return None

def calculate_trip_time(activity_list):
    arrival_time = None
    completion_time = None
    for event in activity_list: # Find first arrival
        changes = event.get("changes", {})
        status_change = changes.get("status")
        if status_change and isinstance(status_change, list) and len(status_change) >= 2:
            if str(status_change[0]).lower() == "pending" and str(status_change[1]).lower() == "arrived":
                created_str = event.get("created_at", "").replace(" UTC", "")
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]:
                    try: arrival_time = datetime.strptime(created_str, fmt); break
                    except ValueError: continue
                if arrival_time: break 
    for event in activity_list: # Find completion
        changes = event.get("changes", {})
        status_change = changes.get("status")
        if status_change and isinstance(status_change, list) and len(status_change) >= 2:
            if str(status_change[1]).lower() == "completed":
                created_str = event.get("created_at", "").replace(" UTC", "")
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]:
                    try: completion_time = datetime.strptime(created_str, fmt); break
                    except ValueError: continue
    if arrival_time and completion_time:
        time_diff = completion_time - arrival_time
        return round(time_diff.total_seconds() / 3600.0, 2)
    return None
