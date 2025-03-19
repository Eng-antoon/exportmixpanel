from app import app
from app.models.operations import migrate_db
import sys

if __name__ == "__main__":
    # Run database migrations
    print("Running database migration...")
    migrate_db()
    print("Database migration completed")
    
    # Check for arguments
    debug_mode = "--debug" in sys.argv
    
    # Start the Flask application
    app.run(debug=debug_mode, host="0.0.0.0", port=5000) 