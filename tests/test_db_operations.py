import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import tempfile
import sqlite3

# Add the parent directory to the path so we can import the modules
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.models import Base, Trip, Tag, trip_tags
from db.create_db import create_database

class TestDbOperations(unittest.TestCase):
    """Tests for database operations."""
    
    def setUp(self):
        """Set up a test database."""
        # Use an in-memory SQLite database for testing
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        
        # Create a session
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    
    def tearDown(self):
        """Clean up after tests."""
        self.session.close()
        Base.metadata.drop_all(self.engine)
    
    def test_create_database(self):
        """Test the create_database function."""
        # Since this function simply creates tables, we can check if a call doesn't raise an exception
        try:
            # Create a temp file for the SQLite database
            fd, temp_path = tempfile.mkstemp(suffix='.db')
            os.close(fd)
            
            # Patch DB_URI to use the temp file
            with patch('db.create_db.DB_URI', f'sqlite:///{temp_path}'):
                create_database()
            
            # Verify tables were created by checking if we can connect and query
            conn = sqlite3.connect(temp_path)
            cursor = conn.cursor()
            
            # Check if the trips table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trips';")
            self.assertIsNotNone(cursor.fetchone())
            
            # Check if the tags table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tags';")
            self.assertIsNotNone(cursor.fetchone())
            
            # Check if the trip_tags table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trip_tags';")
            self.assertIsNotNone(cursor.fetchone())
            
            conn.close()
            os.unlink(temp_path)
        except Exception as e:
            self.fail(f"create_database raised an exception: {e}")
    
    def test_trip_crud(self):
        """Test CRUD operations on the Trip model."""
        # Create a new trip
        trip = Trip(
            trip_id=12345,
            manual_distance=10.5,
            calculated_distance=11.2,
            route_quality="High"
        )
        self.session.add(trip)
        self.session.commit()
        
        # Read the trip
        retrieved_trip = self.session.query(Trip).filter_by(trip_id=12345).first()
        self.assertEqual(retrieved_trip.trip_id, 12345)
        self.assertEqual(retrieved_trip.route_quality, "High")
        
        # Update the trip
        retrieved_trip.route_quality = "Medium"
        self.session.commit()
        
        # Verify the update
        updated_trip = self.session.query(Trip).filter_by(trip_id=12345).first()
        self.assertEqual(updated_trip.route_quality, "Medium")
        
        # Delete the trip
        self.session.delete(updated_trip)
        self.session.commit()
        
        # Verify the deletion
        deleted_trip = self.session.query(Trip).filter_by(trip_id=12345).first()
        self.assertIsNone(deleted_trip)
    
    def test_tag_crud(self):
        """Test CRUD operations on the Tag model."""
        # Create a new tag
        tag = Tag(name="Test Tag")
        self.session.add(tag)
        self.session.commit()
        
        # Read the tag
        retrieved_tag = self.session.query(Tag).filter_by(name="Test Tag").first()
        self.assertEqual(retrieved_tag.name, "Test Tag")
        
        # Update the tag
        retrieved_tag.name = "Updated Tag"
        self.session.commit()
        
        # Verify the update
        updated_tag = self.session.query(Tag).filter_by(name="Updated Tag").first()
        self.assertEqual(updated_tag.name, "Updated Tag")
        
        # Delete the tag
        self.session.delete(updated_tag)
        self.session.commit()
        
        # Verify the deletion
        deleted_tag = self.session.query(Tag).filter_by(name="Updated Tag").first()
        self.assertIsNone(deleted_tag)
    
    def test_trip_tag_association(self):
        """Test the association between trips and tags."""
        # Create trips and tags
        trip1 = Trip(trip_id=12345)
        trip2 = Trip(trip_id=67890)
        
        tag1 = Tag(name="Tag 1")
        tag2 = Tag(name="Tag 2")
        
        # Associate tags with trips in a specific order
        trip1.tags = [tag1, tag2]  # Use assignment rather than append to ensure a specific order
        trip2.tags = [tag2]
        
        # Add to session and commit
        self.session.add_all([trip1, trip2, tag1, tag2])
        self.session.commit()
        
        # Verify associations
        retrieved_trip1 = self.session.query(Trip).filter_by(trip_id=12345).first()
        self.assertEqual(len(retrieved_trip1.tags), 2)
        
        # Since SQLAlchemy may not guarantee order, verify that both tags are present
        tag_names = [tag.name for tag in retrieved_trip1.tags]
        self.assertIn("Tag 1", tag_names)
        self.assertIn("Tag 2", tag_names)
        
        retrieved_trip2 = self.session.query(Trip).filter_by(trip_id=67890).first()
        self.assertEqual(len(retrieved_trip2.tags), 1)
        self.assertEqual(retrieved_trip2.tags[0].name, "Tag 2")
        
        # Verify reverse associations
        retrieved_tag1 = self.session.query(Tag).filter_by(name="Tag 1").first()
        self.assertEqual(len(retrieved_tag1.trips), 1)
        self.assertEqual(retrieved_tag1.trips[0].trip_id, 12345)
        
        retrieved_tag2 = self.session.query(Tag).filter_by(name="Tag 2").first()
        self.assertEqual(len(retrieved_tag2.trips), 2)
        
        # Remove an association
        trip1.tags.remove(tag1)
        self.session.commit()
        
        # Verify the association was removed
        retrieved_trip1 = self.session.query(Trip).filter_by(trip_id=12345).first()
        self.assertEqual(len(retrieved_trip1.tags), 1)
        self.assertEqual(retrieved_trip1.tags[0].name, "Tag 2")

if __name__ == '__main__':
    unittest.main() 