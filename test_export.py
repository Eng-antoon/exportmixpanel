#!/usr/bin/env python3
"""
Test script for the export API

This script tests the export API functionality by fetching trip points for a specific trip ID.
"""

import logging
import json
from trip_points_helper import MetabaseClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_fetch_trip_points(trip_id=310763):
    """Test fetching trip points for a specific trip ID using the export API"""
    client = MetabaseClient()
    
    # First try with standard API
    logger.info(f"Fetching trip {trip_id} with standard API...")
    
    # This will check if there are trip points in the first 2000 rows
    standard_points = client.get_trip_points(5651, trip_id)
    logger.info(f"Standard API found {len(standard_points)} points for trip {trip_id}")
    
    # Then try with export API directly
    logger.info(f"Fetching trip {trip_id} with export API...")
    export_points = client.get_all_trip_points(5651, trip_id)
    logger.info(f"Export API found {len(export_points)} points for trip {trip_id}")
    
    # Print details of the points found
    if export_points:
        logger.info("Details of the first point:")
        logger.info(json.dumps(export_points[0], indent=2))
    
    return len(standard_points), len(export_points)

if __name__ == "__main__":
    std_count, export_count = test_fetch_trip_points()
    
    if export_count > std_count:
        print(f"✅ Success! Export API found {export_count - std_count} more points than standard API")
    elif export_count == std_count and std_count > 0:
        print("✅ Both APIs found the same number of points")
    elif export_count == 0 and std_count == 0:
        print("❌ Neither API found any points for this trip")
    else:
        print(f"⚠️ Unexpected result: Standard API found {std_count} points, Export API found {export_count} points") 