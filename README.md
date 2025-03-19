# Mixpanel Export & Analytics Tool

A comprehensive solution for exporting, consolidating, and analyzing event data from Mixpanel. This project is designed for robust data handling, enriching data with mobile device specifications, and providing an interactive web dashboard for trip analytics.

## Introduction

This project provides a complete toolchain that includes:

- Exporting event data from Mixpanel using its Export API with configurable date ranges and timestamp conversions.
- Consolidating and cleaning the exported data by removing duplicates and merging it with detailed mobile device specifications.
- Storing and managing trip data using a well-defined SQLAlchemy data schema.
- Providing an interactive web dashboard built with Flask to visualize, filter, and manage trip data.
- Command-line utilities for streamlined data export and consolidation processes.

## Features

### Mixpanel Data Export
- Utilizes Mixpanel's Export API to fetch event data (specifically for trip details) for a specified date range.
- Processes exported JSON data: flattens nested data, cleans column headers by stripping unnecessary characters, and converts Unix timestamps to human-readable dates.
- Saves the exported data as an Excel file (`mixpanel_export.xlsx`).

### Data Consolidation
- Reads the exported Excel file and sorts the trip records by timestamp (newest first).
- Removes duplicate entries based on `tripId`, ensuring each trip has a single, most recent record.
- Merges the cleaned trip data with mobile device specifications from an embedded CSV.

### Data Schema & Storage
- Implements a SQLAlchemy model (`Trip`) to define the structure of trip-related data.
- **Trip Model Details:**
  - `id`: Auto-incrementing primary key.
  - `trip_id`: Unique identifier from the ILLA system.
  - `manual_distance`: Optional float for manually recorded distance.
  - `calculated_distance`: Optional float for automatically calculated distance.
  - `route_quality`: String describing the quality of the route.
  - `status`: Status indicator of the trip.
  - `trip_time`: Float representing the duration of the trip.
  - `completed_by`: String indicating who or what completed the trip.
  - `coordinate_count`: Integer representing the number of coordinates recorded.
  - `lack_of_accuracy`: Boolean flag indicating whether a lack of accuracy tag was found (default is None).

### Mobile Device Specifications
- The project includes a detailed CSV embedded within the consolidation script containing specifications for various mobile devices.
- **CSV includes details such as:**
  - Original Model, Brand, Device Name, Release Year, Android Version
  - Hardware features: Fingerprint Sensor, Accelerometer, Gyro, Proximity Sensor, Compass, Barometer
  - Additional specs: Chipset, RAM, Storage, Battery Capacity (mAh)

### Interactive Web Dashboard
- Built with Flask to provide a user-friendly interface for viewing and managing trip analytics.
- Offers features for applying filters, updating trip records, and visualizing trends.

### Database Setup & Migration
- Includes scripts (`db/create_db.py`) and configuration (`db/config.py`) for initializing and migrating the database as the data schema evolves.

### Command-Line Utilities
- **Data Export:** `exportmix.py` allows users to export Mixpanel data for a specified date range.
- **Data Consolidation:** `consolidatemixpanel.py` processes and consolidates the exported data, enriching it with device specifications, and outputs a cleaned Excel file (`data/data.xlsx`).

## Project Structure

- `app.py`: Main Flask application for the interactive web dashboard.
- `exportmix.py`: Script to export event data from Mixpanel, process timestamps, and save data to `mixpanel_export.xlsx`.
- `consolidatemixpanel.py`: Consolidates exported data, removes duplicate trips based on timestamp, and merges with embedded mobile device specifications.
- `db/models.py`: Contains the SQLAlchemy model (`Trip`) defining the database schema.
- `db/create_db.py`: Script to initialize and migrate the database schema.
- `db/config.py`: Database configuration settings.
- `mixpanel.py`: A placeholder/dummy module for future Mixpanel functionality enhancements.
- `templates/`: HTML templates for the Flask web interface.
- `static/`: Static assets (CSS, JavaScript, images) for the web dashboard.
- `data/`: Stores exported and consolidated data files (`mixpanel_export.xlsx` and `data.xlsx`).
- `tests/`: Automated tests for validating various parts of the project.
- `requirements.txt`: Lists all Python dependencies.

## Installation

1. Clone this repository.
2. (Optional) Create and activate a virtual environment.
3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
4. Configure Mixpanel API details in `exportmix.py` (update `API_SECRET` if necessary).
5. Initialize the database by running the script in `db/create_db.py` if not already set up.

## Usage

### Web Application

Run the Flask web dashboard:
```
python app.py
```
- View trip analytics, update records, and apply filters through the dashboard.

### Data Export

Export event data from Mixpanel by specifying a date range:
```
python exportmix.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```
- The data is saved as `mixpanel_export.xlsx`.

### Data Consolidation

Consolidate and enrich the exported data:
```
python consolidatemixpanel.py
```
- Duplicate trip entries are filtered (keeping only the most recent), and data is merged with mobile device specifications.
- The final consolidated dataset is saved as `data/data.xlsx`.

### Testing

Run the tests to ensure everything is working as expected:
```
pytest
```

## License

[Insert License Information Here]

## Future Enhancements

- Expand the functionality in `mixpanel.py` to support dynamic Mixpanel queries and more advanced data processing.
- Enhance the web dashboard with additional analytics and user management features.
- Improve error handling, logging, and overall maintainability of the codebase.
- Introduce user authentication for secure access to data management features. 