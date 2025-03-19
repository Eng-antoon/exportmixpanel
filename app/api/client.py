import requests
from db.config import BASE_API_URL, API_TOKEN
from app.utils.helpers import fetch_api_token, fetch_api_token_alternative

def fetch_coordinates_count(trip_id, token=API_TOKEN):
    """
    Fetch the count of coordinates for a trip.
    
    Args:
        trip_id: ID of the trip
        token: Authentication token
        
    Returns:
        Count of coordinates or 0 if not available
    """
    url = f"{BASE_API_URL}/admin/trips/{trip_id}/coordinates"
    headers = {"Authorization": token}
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            coordinates = resp.json().get("coordinates", [])
            return len(coordinates)
        else:
            print(f"Error {resp.status_code} fetching coordinates for trip {trip_id}")
            return 0
    except Exception as e:
        print(f"Exception fetching coordinates: {e}")
        return 0

def fetch_trip_from_api(trip_id, token=API_TOKEN):
    """
    Fetch trip details from the API.
    
    Args:
        trip_id: ID of the trip to fetch
        token: Authentication token
        
    Returns:
        Dictionary of trip data or None on failure
    """
    url = f"{BASE_API_URL}/admin/trips/{trip_id}"
    headers = {"Authorization": token}
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 401:  # Unauthorized
            # Try to get a new token
            new_token = fetch_api_token() or fetch_api_token_alternative()
            if new_token:
                # Retry with new token
                headers = {"Authorization": new_token}
                resp2 = requests.get(url, headers=headers)
                if resp2.status_code == 200:
                    return resp2.json()
        
        print(f"Error {resp.status_code} fetching trip {trip_id}")
        return None
    except Exception as e:
        print(f"Exception fetching trip: {e}")
        return None

def fetch_trip_coordinates(trip_id, token=API_TOKEN):
    """
    Fetch coordinates for a trip.
    
    Args:
        trip_id: ID of the trip
        token: Authentication token
        
    Returns:
        List of coordinates or empty list if not available
    """
    url = f"{BASE_API_URL}/admin/trips/{trip_id}/coordinates"
    headers = {"Authorization": token}
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp.json().get("coordinates", [])
        elif resp.status_code == 401:  # Unauthorized
            # Try to get a new token
            new_token = fetch_api_token() or fetch_api_token_alternative()
            if new_token:
                # Retry with new token
                headers = {"Authorization": new_token}
                resp2 = requests.get(url, headers=headers)
                if resp2.status_code == 200:
                    return resp2.json().get("coordinates", [])
        
        print(f"Error {resp.status_code} fetching coordinates for trip {trip_id}")
        return []
    except Exception as e:
        print(f"Exception fetching coordinates: {e}")
        return [] 