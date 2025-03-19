from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

trip_tags = Table(
    'trip_tags',
    Base.metadata,
    Column('trip_id', Integer, ForeignKey('trips.id')),
    Column('tag_id', Integer, ForeignKey('tags.id'))
)

class Trip(Base):
    """
    A simple model storing:
      - trip_id (unique from the ILLA system)
      - manual_distance
      - calculated_distance
      - route_quality
    You can expand as needed.
    """
    __tablename__ = "trips"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(Integer, unique=True, nullable=False)
    manual_distance = Column(Float, nullable=True)
    calculated_distance = Column(Float, nullable=True)
    route_quality = Column(String, nullable=True)
    status = Column(String, nullable=True)
    trip_time = Column(Float, nullable=True)
    completed_by = Column(String, nullable=True)
    coordinate_count = Column(Integer, nullable=True)
    # New field to store lack_of_accuracy tag; True if tag found, False if not, default is None
    lack_of_accuracy = Column(Boolean, nullable=True, default=None)
    # Distance analysis fields
    short_segments_count = Column(Integer, nullable=True)  # Count of segments less than 1 km
    medium_segments_count = Column(Integer, nullable=True)  # Count of segments between 1-5 km
    long_segments_count = Column(Integer, nullable=True)  # Count of segments more than 5 km
    short_segments_distance = Column(Float, nullable=True)  # Total distance of segments less than 1 km
    medium_segments_distance = Column(Float, nullable=True)  # Total distance of segments between 1-5 km
    long_segments_distance = Column(Float, nullable=True)  # Total distance of segments more than 5 km
    max_segment_distance = Column(Float, nullable=True)  # Maximum distance between any two consecutive points
    avg_segment_distance = Column(Float, nullable=True)  # Average distance between consecutive points
    tags = relationship("Tag", secondary=trip_tags, backref="trips")

class Tag(Base):
    __tablename__ = "tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
