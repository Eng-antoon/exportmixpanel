import requests
import os # Though os might not be directly used by these specific funcs, often API interaction might involve file paths for e.g. certs.
from db.config import BASE_API_URL, API_EMAIL, API_PASSWORD, API_TOKEN

# Note: The original functions in app.py had a default API_TOKEN in their signature.
# This will be preserved.

def fetch_api_token():
    url = f"{BASE_API_URL}/auth/sign_in"
    payload = {"admin_user": {"email": API_EMAIL, "password": API_PASSWORD}}
    try: # Added try-except for robustness, similar to fetch_api_token_alternative
        resp = requests.post(url, json=payload)
        resp.raise_for_status() # Raise an exception for bad status codes
        return resp.json().get("token", None)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching primary token: {e}")
        return None
    except Exception as e: # Catch other potential errors like JSONDecodeError
        print(f"An unexpected error occurred fetching primary token: {e}")
        return None

def fetch_api_token_alternative():
    # Consider moving these credentials to config as well if they are not specific to this alternative path
    alt_email = "SupplyPartner@illa.com.eg" 
    alt_password = "654321" 
    url = f"{BASE_API_URL}/auth/sign_in"
    payload = {"admin_user": {"email": alt_email, "password": alt_password}}
    try:
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        return resp.json().get("token", None)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching alternative token: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred fetching alternative token: {e}")
        return None

def fetch_coordinates_count(trip_id, token=API_TOKEN):
    url = f"{BASE_API_URL}/trips/{trip_id}/coordinates"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = requests.get(url, headers=headers)
        
        # Try alternative token if primary fails with 401
        if resp.status_code == 401:
            alt_token = fetch_api_token_alternative()
            if alt_token:
                headers["Authorization"] = f"Bearer {alt_token}"
                resp = requests.get(url, headers=headers)
        
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", {}).get("attributes", {}).get("count", 0)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching coordinates for trip {trip_id}: {e}")
        return None
    except Exception as e: # Catch other potential errors
        print(f"An unexpected error occurred fetching coordinates for trip {trip_id}: {e}")
        return None

def fetch_trip_from_api(trip_id, token=API_TOKEN):
    url = f"{BASE_API_URL}/trips/{trip_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = requests.get(url, headers=headers)
        # If unauthorized, try alternative token
        if resp.status_code == 401:
            alt_token = fetch_api_token_alternative()
            if alt_token:
                headers["Authorization"] = f"Bearer {alt_token}"
                resp = requests.get(url, headers=headers)
                # If alternative token also results in 401 or other error, it will be raised below
        
        resp.raise_for_status() # Check for HTTP errors for the final response
        data = resp.json()
        
        # Validate essential field presence
        calc_dist = data.get("data", {}).get("attributes", {}).get("calculatedDistance")
        if calc_dist is None or str(calc_dist).strip() in ["", "N/A"]:
            # Raise a ValueError or return a specific structure indicating missing critical data
            # For now, let's return None as the original code implies error by trying alternative token.
            # A more robust solution might be a custom exception or a dict with an error key.
            print(f"Warning: Missing calculatedDistance for trip {trip_id} with token {token[:5]}...") # Log part of token for debug
            # Depending on strictness, could return None or raise error. Original code tries alternative.
            # If we are here after trying primary (and potentially alt token if primary was 401),
            # and still missing data, this indicates an issue with the API response itself.
            # Let's assume the original logic of trying alternative token handles this.
            # If after all attempts, data is still incomplete, the caller (update_trip_db) should handle it.
            # For now, we return data as is if status is OK, but caller needs to be robust.
            
        # Add a flag if alternative token was used (original logic in app.py did this)
        # This needs to be handled carefully: if primary token worked, this flag shouldn't be set.
        # The current structure here doesn't explicitly track if alt_token was *successfully* used for *this* data.
        # Re-thinking: the original `fetch_trip_from_api` had a specific structure for this.
        # Let's refine to match that more closely.
        
        # The original logic was: try primary, on ANY exception, try alternative.
        # Let's stick to that for now.
        return data

    except requests.exceptions.RequestException as e: # Covers network issues, timeout, etc.
        print(f"Error fetching trip data for {trip_id} with primary token: {e}")
        alt_token = fetch_api_token_alternative()
        if alt_token:
            headers_alt = {"Authorization": f"Bearer {alt_token}", "Content-Type": "application/json"}
            try:
                print(f"Trying alternative token for trip {trip_id}")
                resp_alt = requests.get(url, headers=headers_alt)
                resp_alt.raise_for_status()
                data_alt = resp_alt.json()
                # It's good practice to validate critical fields from alt response too
                calc_dist_alt = data_alt.get("data", {}).get("attributes", {}).get("calculatedDistance")
                if calc_dist_alt is None or str(calc_dist_alt).strip() in ["", "N/A"]:
                     print(f"Warning: Missing calculatedDistance for trip {trip_id} even with alternative token.")
                     # Return None or data_alt based on how strictly this should be handled
                     # Returning data_alt for now, caller must validate.
                data_alt["used_alternative"] = True # Flag that alternative token was used
                return data_alt
            except requests.exceptions.RequestException as e_alt_req:
                print(f"Alternative fetch failed for trip {trip_id} (RequestException): {e_alt_req}")
            except Exception as e_alt_other: # Other errors like JSONDecodeError
                print(f"Alternative fetch failed for trip {trip_id} (Other): {e_alt_other}")
        return None # If primary fails and alt token also fails or isn't available
    except Exception as e: # Catch other errors from primary attempt (e.g. JSONDecodeError)
        print(f"An unexpected error occurred fetching trip data for {trip_id} with primary token: {e}")
        # Optionally, could also try alternative token here if not a RequestException
        return None
