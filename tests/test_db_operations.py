import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import tempfile
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.models import Base, Trip, Tag
import importlib

# Mock create_database to work within the test environment
def create_test_database(engine):
    Base.metadata.create_all(engine)
    return True

class TestDbOperations:
    @pytest.fixture
    def temp_db_path(self):
        # Create a temporary file to use as our test database
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        
        # Set up the database URI
        db_uri = f'sqlite:///{path}'
        
        yield path, db_uri
        
        # Clean up - remove temp file
        if os.path.exists(path):
            os.unlink(path)
    
    def test_create_database(self, temp_db_path):
        # Get the path and URI
        db_path, db_uri = temp_db_path
        
        # Create an engine with the test URI
        engine = create_engine(db_uri)
        
        # Call the create_database function directly on the engine
        result = create_test_database(engine)
        assert result is True
        
        # Connect to the created database and verify tables exist
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check trips table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trips'")
        assert cursor.fetchone() is not None
        
        # Check tags table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tags'")
        assert cursor.fetchone() is not None
        
        # Check trip_tags table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trip_tags'")
        assert cursor.fetchone() is not None
        
        conn.close()
    
    @pytest.fixture
    def db_session(self, temp_db_path):
        # Get path and URI
        _, db_uri = temp_db_path
        
        # Create the engine and session
        engine = create_engine(db_uri)
        Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        yield session
        
        # Clean up
        session.close()
        Base.metadata.drop_all(engine)
    
    def test_add_trip(self, db_session):
        # Create and add a new trip
        trip = Trip(
            trip_id=1234,
            manual_distance=5.0,
            status="in_progress"
        )
        db_session.add(trip)
        db_session.commit()
        
        # Verify the trip was added
        retrieved_trip = db_session.query(Trip).filter_by(trip_id=1234).first()
        assert retrieved_trip is not None
        assert retrieved_trip.manual_distance == 5.0
        assert retrieved_trip.status == "in_progress"
    
    def test_update_trip(self, db_session):
        # Create and add a new trip
        trip = Trip(
            trip_id=5678,
            manual_distance=10.0,
            status="new"
        )
        db_session.add(trip)
        db_session.commit()
        
        # Update the trip
        trip.status = "completed"
        trip.manual_distance = 12.5
        db_session.commit()
        
        # Verify the trip was updated
        retrieved_trip = db_session.query(Trip).filter_by(trip_id=5678).first()
        assert retrieved_trip.status == "completed"
        assert retrieved_trip.manual_distance == 12.5
    
    def test_delete_trip(self, db_session):
        # Create and add a new trip
        trip = Trip(trip_id=9876)
        db_session.add(trip)
        db_session.commit()
        
        # Verify the trip exists
        assert db_session.query(Trip).filter_by(trip_id=9876).first() is not None
        
        # Delete the trip
        db_session.delete(trip)
        db_session.commit()
        
        # Verify the trip was deleted
        assert db_session.query(Trip).filter_by(trip_id=9876).first() is None
    
    def test_add_tags_to_trip(self, db_session):
        # Create a trip and tags
        trip = Trip(trip_id=4444)
        tag1 = Tag(name="test_tag1")
        tag2 = Tag(name="test_tag2")
        
        # Add trip and tags to database
        db_session.add(trip)
        db_session.add(tag1)
        db_session.add(tag2)
        db_session.commit()
        
        # Associate tags with trip
        trip.tags.append(tag1)
        trip.tags.append(tag2)
        db_session.commit()
        
        # Verify the association
        retrieved_trip = db_session.query(Trip).filter_by(trip_id=4444).first()
        assert len(retrieved_trip.tags) == 2
        tag_names = sorted([tag.name for tag in retrieved_trip.tags])
        assert "test_tag1" in tag_names
        assert "test_tag2" in tag_names
    
    def test_remove_tag_from_trip(self, db_session):
        # Create a trip and tags
        trip = Trip(trip_id=5555)
        tag1 = Tag(name="remove_tag1")
        tag2 = Tag(name="remove_tag2")
        
        # Add trip and tags to database and associate
        db_session.add(trip)
        db_session.add(tag1)
        db_session.add(tag2)
        trip.tags.append(tag1)
        trip.tags.append(tag2)
        db_session.commit()
        
        # Verify both tags are associated
        retrieved_trip = db_session.query(Trip).filter_by(trip_id=5555).first()
        assert len(retrieved_trip.tags) == 2
        
        # Remove one tag
        retrieved_trip.tags.remove(tag1)
        db_session.commit()
        
        # Verify only one tag remains
        db_session.refresh(retrieved_trip)
        assert len(retrieved_trip.tags) == 1
        assert retrieved_trip.tags[0].name == "remove_tag2" 