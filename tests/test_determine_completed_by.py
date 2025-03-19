import sys
import os
sys.path.insert(0, os.path.abspath(os.getcwd()))

import datetime
import pytest

from app.utils.trip_analysis import determine_completed_by

def test_admin_candidate():
    activity_list = [{
        "user_id": 73,
        "user_type": "admin",
        "user_name": "karim.ragab@illa.com.eg",
        "created_at": "2025-03-13 16:55:43 UTC",
        "changes": {
            "updated_at": ["2025-03-13 16:55:13 UTC", "2025-03-13 16:55:43 UTC"],
            "status": ["arrived", "completed"]
        }
    }]
    assert determine_completed_by(activity_list) == "admin"

def test_driver_candidate():
    activity_list = [{
        "user_id": 17259,
        "user_type": "driver",
        "user_name": "",
        "created_at": "2025-03-13 17:43:51 UTC",
        "changes": {
            "updated_at": ["2025-03-13 17:43:51 UTC", "2025-03-13 17:43:51 UTC"],
            "status": ["moving", "completed"]
        }
    }]
    assert determine_completed_by(activity_list) == "driver"

def test_non_status_change_event():
    activity_list = [{
        "user_id": 73,
        "user_type": "admin",
        "user_name": "karim.ragab@illa.com.eg",
        "created_at": "2025-03-13 18:10:50 UTC",
        "changes": {
            "manual_distance": [None, "75.0"],
            "updated_at": ["2025-03-13 17:43:51 UTC", "2025-03-13 18:10:50 UTC"]
        }
    }]
    assert determine_completed_by(activity_list) is None

def test_multiple_events():
    # Two events, the later one (by created_at) should be selected
    activity_list = [
        {
            "user_id": 100,
            "user_type": "driver",
            "user_name": "",
            "created_at": "2025-03-13 15:00:00 UTC",
            "changes": {
                "status": ["pending", "completed"]
            }
        },
        {
            "user_id": 101,
            "user_type": "admin",
            "user_name": "admin.user@example.com",
            "created_at": "2025-03-13 18:00:00 UTC",
            "changes": {
                "status": ["arrived", "completed"]
            }
        }
    ]
    assert determine_completed_by(activity_list) == "admin" 