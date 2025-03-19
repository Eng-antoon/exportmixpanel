import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.models import Base, Trip, Tag, trip_tags

class TestDbModels:
    @pytest.fixture
    def setup_db(self):
        # Create an in-memory SQLite database for testing
        engine = create_engine('sqlite:///:memory:')
        # Create all tables in the metadata
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session
        # Clean up
        session.close()
        # Drop all tables
        Base.metadata.drop_all(engine)
    
    def test_trip_model(self, setup_db):
        # Create a test trip
        trip = Trip(
            trip_id=12345,
            manual_distance=10.5,
            calculated_distance=11.2,
            route_quality="good",
            status="completed",
            trip_time=15.3,
            completed_by="driver",
            coordinate_count=25,
            lack_of_accuracy=False
        )
        
        # Add to the session
        setup_db.add(trip)
        setup_db.commit()
        
        # Retrieve the trip
        retrieved_trip = setup_db.query(Trip).filter_by(trip_id=12345).first()
        
        # Validate fields
        assert retrieved_trip.trip_id == 12345
        assert retrieved_trip.manual_distance == 10.5
        assert retrieved_trip.calculated_distance == 11.2
        assert retrieved_trip.route_quality == "good"
        assert retrieved_trip.status == "completed"
        assert retrieved_trip.trip_time == 15.3
        assert retrieved_trip.completed_by == "driver"
        assert retrieved_trip.coordinate_count == 25
        assert retrieved_trip.lack_of_accuracy is False
    
    def test_tag_model(self, setup_db):
        # Create a test tag
        tag = Tag(name="test_tag")
        
        # Add to the session
        setup_db.add(tag)
        setup_db.commit()
        
        # Retrieve the tag
        retrieved_tag = setup_db.query(Tag).filter_by(name="test_tag").first()
        
        # Validate fields
        assert retrieved_tag.name == "test_tag"
    
    def test_trip_tag_relationship(self, setup_db):
        # Create a trip and tags
        trip = Trip(trip_id=54321)
        tag1 = Tag(name="tag1")
        tag2 = Tag(name="tag2")
        
        # Associate tags with trip
        trip.tags.append(tag1)
        trip.tags.append(tag2)
        
        # Add to the session
        setup_db.add(trip)
        setup_db.add(tag1)
        setup_db.add(tag2)
        setup_db.commit()
        
        # Retrieve the trip and check its tags
        retrieved_trip = setup_db.query(Trip).filter_by(trip_id=54321).first()
        assert len(retrieved_trip.tags) == 2
        tag_names = sorted([tag.name for tag in retrieved_trip.tags])
        assert tag_names == ["tag1", "tag2"]
        
        # Retrieve a tag and check its trips
        retrieved_tag = setup_db.query(Tag).filter_by(name="tag1").first()
        assert len(retrieved_tag.trips) == 1
        assert retrieved_tag.trips[0].trip_id == 54321
    
    def test_trip_uniqueness_constraint(self, setup_db):
        # Create two trips with the same trip_id
        trip1 = Trip(trip_id=99999)
        trip2 = Trip(trip_id=99999)
        
        # Add first trip
        setup_db.add(trip1)
        setup_db.commit()
        
        # Trying to add second trip with same trip_id should raise an exception
        setup_db.add(trip2)
        with pytest.raises(Exception):
            setup_db.commit()
            
        # Rollback the failed transaction
        setup_db.rollback()

    def test_tag_uniqueness_constraint(self, setup_db):
        # Create two tags with the same name
        tag1 = Tag(name="unique_tag")
        tag2 = Tag(name="unique_tag")
        
        # Add first tag
        setup_db.add(tag1)
        setup_db.commit()
        
        # Trying to add second tag with same name should raise an exception
        setup_db.add(tag2)
        with pytest.raises(Exception):
            setup_db.commit()
            
        # Rollback the failed transaction
        setup_db.rollback() 