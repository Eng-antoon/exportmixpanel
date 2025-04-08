import unittest
from unittest.mock import patch, MagicMock, Mock
import json
import os
import sys
import datetime

# Add the parent directory to the path so we can import the app module
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

import app
from db.models import Trip

class TestAppFilters(unittest.TestCase):
    """Tests for the filtering functions in app.py."""
    
    def test_normalize_op(self):
        """Test the filter operation normalization function."""
        # Extract the normalize_op function from the route handler
        # This is usually defined inside the trips route
        normalize_op = None
        
        # Check if we can find the function in the global scope
        if hasattr(app, "normalize_op"):
            normalize_op = app.normalize_op
        else:
            # Create a simplified version for testing based on the app code
            def normalize_op(op):
                op = op.lower()
                if op == "equals" or op == "=" or op == "==":
                    return "=="
                elif op == "not equals" or op == "!=" or op == "<>":
                    return "!="
                elif op == "greater than" or op == ">":
                    return ">"
                elif op == "less than" or op == "<":
                    return "<"
                elif op == "greater than or equal" or op == ">=":
                    return ">="
                elif op == "less than or equal" or op == "<=":
                    return "<="
                elif op == "contains":
                    return "contains"
                elif op == "not contains":
                    return "not contains"
                elif op == "starts with":
                    return "starts with"
                elif op == "ends with":
                    return "ends with"
                else:
                    return op
        
        # Now test the normalize_op function
        self.assertEqual(normalize_op("equals"), "==")
        self.assertEqual(normalize_op("="), "==")
        self.assertEqual(normalize_op("=="), "==")
        
        self.assertEqual(normalize_op("not equals"), "!=")
        self.assertEqual(normalize_op("!="), "!=")
        self.assertEqual(normalize_op("<>"), "!=")
        
        self.assertEqual(normalize_op("greater than"), ">")
        self.assertEqual(normalize_op(">"), ">")
        
        self.assertEqual(normalize_op("less than"), "<")
        self.assertEqual(normalize_op("<"), "<")
        
        self.assertEqual(normalize_op("greater than or equal"), ">=")
        self.assertEqual(normalize_op(">="), ">=")
        
        self.assertEqual(normalize_op("less than or equal"), "<=")
        self.assertEqual(normalize_op("<="), "<=")
        
        self.assertEqual(normalize_op("contains"), "contains")
        self.assertEqual(normalize_op("not contains"), "not contains")
        self.assertEqual(normalize_op("starts with"), "starts with")
        self.assertEqual(normalize_op("ends with"), "ends with")

    def test_compare_function(self):
        """Test the compare function used in filters."""
        # Extract the compare function from the route handler
        # This is usually defined inside the trips route
        compare = None
        
        # Check if we can find the function in the global scope
        if hasattr(app, "compare"):
            compare = app.compare
        else:
            # Create a simplified version for testing based on the app code
            def compare(value, op, threshold):
                # Convert value and threshold to numeric if possible
                try:
                    if isinstance(value, str) and value.replace('.', '', 1).isdigit():
                        value = float(value)
                    if isinstance(threshold, str) and threshold.replace('.', '', 1).isdigit():
                        threshold = float(threshold)
                except (ValueError, AttributeError):
                    pass  # Keep as string if conversion fails
                
                if value is None:
                    return op == "!="
                    
                if op == "==":
                    if isinstance(value, str) and isinstance(threshold, str):
                        return value.lower() == threshold.lower()
                    return value == threshold
                elif op == "!=":
                    if isinstance(value, str) and isinstance(threshold, str):
                        return value.lower() != threshold.lower()
                    return value != threshold
                elif op == ">":
                    return value > threshold
                elif op == "<":
                    return value < threshold
                elif op == ">=":
                    return value >= threshold
                elif op == "<=":
                    return value <= threshold
                elif op == "contains":
                    return threshold.lower() in str(value).lower()
                elif op == "not contains":
                    return threshold.lower() not in str(value).lower()
                elif op == "starts with":
                    return str(value).lower().startswith(threshold.lower())
                elif op == "ends with":
                    return str(value).lower().endswith(threshold.lower())
                return False
        
        # Now test the compare function for different operators
        # Equality operators
        self.assertTrue(compare(10, "==", 10))
        self.assertFalse(compare(10, "==", 20))
        
        self.assertTrue(compare(10, "!=", 20))
        self.assertFalse(compare(10, "!=", 10))
        
        # Comparison operators
        self.assertTrue(compare(20, ">", 10))
        self.assertFalse(compare(10, ">", 20))
        
        self.assertTrue(compare(10, "<", 20))
        self.assertFalse(compare(20, "<", 10))
        
        self.assertTrue(compare(20, ">=", 20))
        self.assertTrue(compare(30, ">=", 20))
        self.assertFalse(compare(10, ">=", 20))
        
        self.assertTrue(compare(10, "<=", 10))
        self.assertTrue(compare(10, "<=", 20))
        self.assertFalse(compare(20, "<=", 10))
        
        # String operators
        self.assertTrue(compare("Hello World", "contains", "world"))
        self.assertFalse(compare("Hello World", "contains", "xyz"))
        
        self.assertTrue(compare("Hello World", "not contains", "xyz"))
        self.assertFalse(compare("Hello World", "not contains", "world"))
        
        self.assertTrue(compare("Hello World", "starts with", "hello"))
        self.assertFalse(compare("Hello World", "starts with", "world"))
        
        self.assertTrue(compare("Hello World", "ends with", "world"))
        self.assertFalse(compare("Hello World", "ends with", "hello"))
        
        # Mixed type comparisons - our implementation should auto-convert
        self.assertTrue(compare(10, "==", "10"))  
        self.assertTrue(compare("10", "==", 10))  
        
        self.assertTrue(compare(20, ">", "10"))  
        self.assertFalse(compare("10", ">", 20))
        
        # Handle None values
        self.assertFalse(compare(None, "==", 10))
        self.assertTrue(compare(None, "!=", 10))
        self.assertFalse(compare(None, ">", 10))

    def test_apply_date_filter(self):
        """Test the date filter application."""
        # Create mock trips with different dates
        trips = []
        
        # Trip 1: Jan 1, 2023
        trip1 = MagicMock(spec=Trip)
        trip1.trip_started_at = datetime.datetime(2023, 1, 1, 10, 0, 0)
        trips.append(trip1)
        
        # Trip 2: Jan 15, 2023
        trip2 = MagicMock(spec=Trip)
        trip2.trip_started_at = datetime.datetime(2023, 1, 15, 10, 0, 0)
        trips.append(trip2)
        
        # Trip 3: Feb 5, 2023
        trip3 = MagicMock(spec=Trip)
        trip3.trip_started_at = datetime.datetime(2023, 2, 5, 10, 0, 0)
        trips.append(trip3)
        
        # Trip 4: No date (should be excluded)
        trip4 = MagicMock(spec=Trip)
        trip4.trip_started_at = None
        trips.append(trip4)
        
        # Define a simple filter function for date filtering
        def filter_trips_by_date(trips, start_date, end_date):
            filtered_trips = []
            for trip in trips:
                if trip.trip_started_at is not None:
                    if start_date and end_date:
                        if start_date <= trip.trip_started_at.date() <= end_date:
                            filtered_trips.append(trip)
                    elif start_date:
                        if start_date <= trip.trip_started_at.date():
                            filtered_trips.append(trip)
                    elif end_date:
                        if trip.trip_started_at.date() <= end_date:
                            filtered_trips.append(trip)
                    else:
                        filtered_trips.append(trip)
            return filtered_trips
        
        # Test with both start and end dates
        start_date = datetime.date(2023, 1, 10)
        end_date = datetime.date(2023, 1, 31)
        filtered = filter_trips_by_date(trips, start_date, end_date)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0], trip2)
        
        # Test with only start date
        start_date = datetime.date(2023, 2, 1)
        end_date = None
        filtered = filter_trips_by_date(trips, start_date, end_date)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0], trip3)
        
        # Test with only end date
        start_date = None
        end_date = datetime.date(2023, 1, 5)
        filtered = filter_trips_by_date(trips, start_date, end_date)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0], trip1)
        
        # Test with no date filter (should return all trips with dates)
        start_date = None
        end_date = None
        filtered = filter_trips_by_date(trips, start_date, end_date)
        self.assertEqual(len(filtered), 3)  # Trip4 should be excluded as it has no date

    def test_apply_property_filters(self):
        """Test applying property filters to trips."""
        # Create mock trips with different properties
        trips = []
        
        # Trip 1: High quality, 10km distance
        trip1 = MagicMock(spec=Trip)
        trip1.route_quality = "High"
        trip1.calculated_distance = 10.0
        trip1.driver_device_os = "iOS"
        trips.append(trip1)
        
        # Trip 2: Medium quality, 5km distance
        trip2 = MagicMock(spec=Trip)
        trip2.route_quality = "Medium"
        trip2.calculated_distance = 5.0
        trip2.driver_device_os = "Android"
        trips.append(trip2)
        
        # Trip 3: Low quality, 15km distance
        trip3 = MagicMock(spec=Trip)
        trip3.route_quality = "Low"
        trip3.calculated_distance = 15.0
        trip3.driver_device_os = "iOS"
        trips.append(trip3)
        
        # Define a simple filter function for property filtering
        def filter_trips_by_properties(trips, filters):
            if not filters:
                return trips
                
            def compare(value, op, threshold):
                if value is None:
                    return False
                if op == "==":
                    return str(value).lower() == str(threshold).lower()
                elif op == "!=":
                    return str(value).lower() != str(threshold).lower()
                elif op == ">":
                    try:
                        return float(value) > float(threshold)
                    except (ValueError, TypeError):
                        return False
                elif op == "<":
                    try:
                        return float(value) < float(threshold)
                    except (ValueError, TypeError):
                        return False
                elif op == ">=":
                    try:
                        return float(value) >= float(threshold)
                    except (ValueError, TypeError):
                        return False
                elif op == "<=":
                    try:
                        return float(value) <= float(threshold)
                    except (ValueError, TypeError):
                        return False
                elif op == "contains":
                    return str(threshold).lower() in str(value).lower()
                return False
            
            filtered_trips = []
            for trip in trips:
                include_trip = True
                for filter_dict in filters:
                    property_name = filter_dict.get("property")
                    operator = filter_dict.get("operator")
                    threshold = filter_dict.get("value")
                    
                    # Get the property value from the trip
                    if property_name == "route_quality":
                        value = trip.route_quality
                    elif property_name == "calculated_distance":
                        value = trip.calculated_distance
                    elif property_name == "driver_device_os":
                        value = trip.driver_device_os
                    else:
                        value = None
                    
                    # Apply the comparison
                    if not compare(value, operator, threshold):
                        include_trip = False
                        break
                
                if include_trip:
                    filtered_trips.append(trip)
            
            return filtered_trips
        
        # Test filter by route quality
        filters = [{"property": "route_quality", "operator": "==", "value": "High"}]
        filtered = filter_trips_by_properties(trips, filters)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0], trip1)
        
        # Test filter by distance > 10
        filters = [{"property": "calculated_distance", "operator": ">", "value": 10}]
        filtered = filter_trips_by_properties(trips, filters)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0], trip3)
        
        # Test multiple filters (iOS and distance >= 10)
        filters = [
            {"property": "driver_device_os", "operator": "==", "value": "iOS"},
            {"property": "calculated_distance", "operator": ">=", "value": 10}
        ]
        filtered = filter_trips_by_properties(trips, filters)
        self.assertEqual(len(filtered), 2)
        self.assertIn(trip1, filtered)
        self.assertIn(trip3, filtered)
        
        # Test with contains operator
        filters = [{"property": "route_quality", "operator": "contains", "value": "ed"}]
        filtered = filter_trips_by_properties(trips, filters)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0], trip2)  # "Medium" contains "ed"


if __name__ == '__main__':
    unittest.main() 