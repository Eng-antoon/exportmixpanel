import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.models import Base, Trip, Tag, trip_tags

class TestModels(unittest.TestCase):
    """Test suite for database models."""
    
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
    
    def test_trip_create(self):
        """Test that a Trip can be created and retrieved."""
        # Create a trip
        trip = Trip(
            trip_id=12345,
            manual_distance=10.5,
            calculated_distance=11.2,
            route_quality="High",
            status="Completed",
            trip_time=30.5,
            completed_by="Driver",
            coordinate_count=100,
            lack_of_accuracy=False,
            expected_trip_quality="High Quality Trip",
            short_segments_count=5,
            medium_segments_count=3,
            long_segments_count=1,
            short_segments_distance=2.5,
            medium_segments_distance=7.5,
            long_segments_distance=6.0,
            max_segment_distance=6.0,
            avg_segment_distance=0.5
        )
        
        # Add to session and commit
        self.session.add(trip)
        self.session.commit()
        
        # Retrieve the trip
        retrieved_trip = self.session.query(Trip).filter_by(trip_id=12345).first()
        
        # Assert values
        self.assertEqual(retrieved_trip.trip_id, 12345)
        self.assertEqual(retrieved_trip.manual_distance, 10.5)
        self.assertEqual(retrieved_trip.calculated_distance, 11.2)
        self.assertEqual(retrieved_trip.route_quality, "High")
        self.assertEqual(retrieved_trip.status, "Completed")
        self.assertEqual(retrieved_trip.trip_time, 30.5)
        self.assertEqual(retrieved_trip.completed_by, "Driver")
        self.assertEqual(retrieved_trip.coordinate_count, 100)
        self.assertEqual(retrieved_trip.lack_of_accuracy, False)
        self.assertEqual(retrieved_trip.expected_trip_quality, "High Quality Trip")
        self.assertEqual(retrieved_trip.short_segments_count, 5)
        self.assertEqual(retrieved_trip.medium_segments_count, 3)
        self.assertEqual(retrieved_trip.long_segments_count, 1)
        self.assertEqual(retrieved_trip.short_segments_distance, 2.5)
        self.assertEqual(retrieved_trip.medium_segments_distance, 7.5)
        self.assertEqual(retrieved_trip.long_segments_distance, 6.0)
        self.assertEqual(retrieved_trip.max_segment_distance, 6.0)
        self.assertEqual(retrieved_trip.avg_segment_distance, 0.5)
    
    def test_tag_create(self):
        """Test that a Tag can be created and retrieved."""
        # Create a tag
        tag = Tag(name="Test Tag")
        
        # Add to session and commit
        self.session.add(tag)
        self.session.commit()
        
        # Retrieve the tag
        retrieved_tag = self.session.query(Tag).filter_by(name="Test Tag").first()
        
        # Assert values
        self.assertEqual(retrieved_tag.name, "Test Tag")
    
    def test_trip_tag_relationship(self):
        """Test the many-to-many relationship between Trip and Tag."""
        # Create a trip
        trip = Trip(trip_id=12345)
        
        # Create tags
        tag1 = Tag(name="Tag 1")
        tag2 = Tag(name="Tag 2")
        
        # Associate tags with trip
        trip.tags.append(tag1)
        trip.tags.append(tag2)
        
        # Add to session and commit
        self.session.add(trip)
        self.session.add(tag1)
        self.session.add(tag2)
        self.session.commit()
        
        # Retrieve the trip
        retrieved_trip = self.session.query(Trip).filter_by(trip_id=12345).first()
        
        # Assert tags without assuming order
        self.assertEqual(len(retrieved_trip.tags), 2)
        tag_names = [tag.name for tag in retrieved_trip.tags]
        self.assertIn("Tag 1", tag_names)
        self.assertIn("Tag 2", tag_names)
        
        # Verify the reverse relationship
        tag1_trips = self.session.query(Tag).filter_by(name="Tag 1").first().trips
        self.assertEqual(len(tag1_trips), 1)
        self.assertEqual(tag1_trips[0].trip_id, 12345)

if __name__ == '__main__':
    unittest.main() 