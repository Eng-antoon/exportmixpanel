# Mixpanel Export and Analysis Tool

A Flask-based application for exporting data from Mixpanel, analyzing trips, and displaying insights through an interactive web interface.

## Project Structure

The project has been refactored into a modular structure:

```
exportmixpanel/
├── app/
│   ├── api/               # API client functionality
│   ├── data_processing/   # Data processing modules
│   ├── models/            # Database models and operations
│   ├── routes/            # Route handlers for web interface
│   ├── utils/             # Utility functions
│   ├── __init__.py        # App initialization
│   └── templates/         # HTML templates
├── db/
│   └── models.py          # Database model definitions
├── data/                  # Directory for data files
├── exportmix.py           # Command-line tool for Mixpanel export
├── consolidatemixpanel.py # Command-line tool for data consolidation
└── run.py                 # Application entry point
```

## Features

- Export data from Mixpanel API
- Consolidate and merge data with mobile device specifications
- View trips with filtering capabilities
- Analyze trip data with dashboards and visualizations
- Export trip data to Excel
- Manage trip tags and route quality
- View detailed trip information and coordinates

## Prerequisites

- Python 3.8+
- Flask
- SQLAlchemy
- Pandas
- Requests
- Openpyxl

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/exportmixpanel.git
   cd exportmixpanel
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Running the Web Application

```
python run.py
```

The application will be available at `http://localhost:5000`.

### Exporting Data from Mixpanel

```
python exportmix.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

This will export data from Mixpanel for the specified date range and save it to `mixpanel_export.xlsx`.

### Consolidating Exported Data

```
python consolidatemixpanel.py --input-file mixpanel_export.xlsx --output-dir data --output-file data.xlsx
```

This will consolidate the exported data, merge it with mobile device specifications, and save it to the specified output file.

## API Endpoints

- `/api/update_db` - Update database with trip information
- `/api/update_db_async` - Start asynchronous update for a trip
- `/api/update_all_db_async` - Start asynchronous update for all trips
- `/api/update_progress` - Get progress of an asynchronous update

## Web Interface

- `/` - Home page with analytics dashboard
- `/trips` - View and filter trips
- `/trip/<trip_id>` - View detailed information for a specific trip
- `/trip_insights` - Advanced trip analysis dashboard
- `/export_trips` - Export filtered trips to Excel

## Database

The application uses SQLAlchemy with SQLite by default. Database tables:

- `Trip` - Store trip information
- `Tag` - Store tag information with many-to-many relationship to trips

## Development

To extend the application:

1. Add new routes in the appropriate files under `app/routes/`
2. Add new database operations in `app/models/operations.py`
3. Enhance the frontend by modifying templates in `app/templates/`

## License

This project is licensed under the MIT License - see the LICENSE file for details. 