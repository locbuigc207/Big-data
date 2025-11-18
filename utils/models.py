from postgresql_client import Base
from sqlalchemy import BigInteger, Column, DateTime, Float, String, func

class LMSEvent(Base):
    __tablename__ = "lms-events"
    event_id = Column(
        String(255),
        primary_key=True
    )
    time = Column(
        DateTime(timezone=True),
        server_default=func.timezone("UTC", func.current_timestamp())
    )
    student_id = Column(BigInteger)
    event_name = Column(String(50))
    description = Column(String(255))
    origin = Column(String(255))
    ip_address = Column(String(255))
