"""
Student Activity Signal Simulator - Kafka Producer
This script simulates real-time student activity events and sends them to Kafka.

Event Types:
- LOGIN: Student login events
- VIDEO_VIEW: Video watching activities
- ASSIGNMENT_SUBMIT: Assignment submissions
- QUIZ_ATTEMPT: Quiz/test attempts
- FORUM_POST: Forum interactions
- ATTENDANCE: Check-in/check-out events
"""

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from typing import Dict, Any, List
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StudentActivitySimulator:
    """Simulates student activities and sends them to Kafka topics"""
    
    def __init__(self, bootstrap_servers: List[str] = ['localhost:9092']):
        """
        Initialize the simulator
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.connect_to_kafka()
        
        # Simulation parameters
        self.num_students = 3000
        self.num_courses = 40
        self.num_teachers = 20
        
        # Event type weights (probability distribution)
        self.event_types = {
            'LOGIN': 0.25,
            'VIDEO_VIEW': 0.30,
            'ASSIGNMENT_SUBMIT': 0.10,
            'QUIZ_ATTEMPT': 0.15,
            'FORUM_POST': 0.10,
            'MATERIAL_DOWNLOAD': 0.10
        }
    
    def connect_to_kafka(self):
        """Establish connection to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip',
                linger_ms=10,  # Batch messages for 10ms
                batch_size=16384  # 16KB batch size
            )
            logger.info(f"Successfully connected to Kafka at {self.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def generate_student_id(self) -> str:
        """Generate a random student ID"""
        return f"STU{random.randint(1, self.num_students):05d}"
    
    def generate_course_id(self) -> str:
        """Generate a random course ID"""
        return f"CS{random.randint(1, self.num_courses):03d}"
    
    def generate_teacher_id(self) -> str:
        """Generate a random teacher ID"""
        return f"TCH{random.randint(1, self.num_teachers):03d}"
    
    def generate_login_event(self) -> Dict[str, Any]:
        """Generate a student login event"""
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'LOGIN',
            'student_id': self.generate_student_id(),
            'timestamp': datetime.now().isoformat(),
            'device_type': random.choice(['Desktop', 'Mobile', 'Tablet']),
            'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            'session_id': str(uuid.uuid4()),
            'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
        }
    
    def generate_video_view_event(self) -> Dict[str, Any]:
        """Generate a video viewing event"""
        duration = random.randint(60, 3600)  # 1 min to 1 hour
        watch_time = random.randint(30, duration)  # Watch time can be less than duration
        
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'VIDEO_VIEW',
            'student_id': self.generate_student_id(),
            'course_id': self.generate_course_id(),
            'timestamp': datetime.now().isoformat(),
            'video_id': f"VID{random.randint(1, 500):04d}",
            'video_title': f"Lecture {random.randint(1, 20)}",
            'duration_seconds': duration,
            'watch_time_seconds': watch_time,
            'completion_rate': round(watch_time / duration * 100, 2),
            'quality': random.choice(['360p', '720p', '1080p']),
            'playback_speed': random.choice([1.0, 1.25, 1.5, 2.0])
        }
    
    def generate_assignment_submit_event(self) -> Dict[str, Any]:
        """Generate an assignment submission event"""
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'ASSIGNMENT_SUBMIT',
            'student_id': self.generate_student_id(),
            'course_id': self.generate_course_id(),
            'timestamp': datetime.now().isoformat(),
            'assignment_id': f"ASG{random.randint(1, 100):03d}",
            'assignment_title': f"Assignment {random.randint(1, 10)}",
            'submission_status': random.choice(['On Time', 'Late', 'Early']),
            'file_count': random.randint(1, 5),
            'file_size_mb': round(random.uniform(0.1, 50.0), 2),
            'attempt_number': random.randint(1, 3),
            'time_spent_minutes': random.randint(30, 180)
        }
    
    def generate_quiz_attempt_event(self) -> Dict[str, Any]:
        """Generate a quiz attempt event"""
        total_questions = random.randint(5, 30)
        correct_answers = random.randint(0, total_questions)
        
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'QUIZ_ATTEMPT',
            'student_id': self.generate_student_id(),
            'course_id': self.generate_course_id(),
            'timestamp': datetime.now().isoformat(),
            'quiz_id': f"QUZ{random.randint(1, 80):03d}",
            'quiz_title': f"Quiz {random.randint(1, 15)}",
            'total_questions': total_questions,
            'correct_answers': correct_answers,
            'score': round(correct_answers / total_questions * 100, 2),
            'time_taken_minutes': random.randint(5, 60),
            'attempt_number': random.randint(1, 3)
        }
    
    def generate_forum_post_event(self) -> Dict[str, Any]:
        """Generate a forum interaction event"""
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'FORUM_POST',
            'student_id': self.generate_student_id(),
            'course_id': self.generate_course_id(),
            'timestamp': datetime.now().isoformat(),
            'thread_id': f"THR{random.randint(1, 200):04d}",
            'post_type': random.choice(['Question', 'Answer', 'Comment', 'Reply']),
            'word_count': random.randint(10, 500),
            'has_attachments': random.choice([True, False]),
            'is_helpful': random.choice([True, False, None])
        }
    
    def generate_material_download_event(self) -> Dict[str, Any]:
        """Generate a material download event"""
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'MATERIAL_DOWNLOAD',
            'student_id': self.generate_student_id(),
            'course_id': self.generate_course_id(),
            'timestamp': datetime.now().isoformat(),
            'material_id': f"MAT{random.randint(1, 300):04d}",
            'material_type': random.choice(['PDF', 'PPTX', 'DOCX', 'ZIP', 'XLSX']),
            'material_name': f"Material_{random.randint(1, 100)}",
            'file_size_mb': round(random.uniform(0.5, 100.0), 2),
            'download_source': random.choice(['Course Page', 'Assignment', 'Announcement'])
        }
    
    def generate_attendance_event(self) -> Dict[str, Any]:
        """Generate an attendance check-in/check-out event"""
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'ATTENDANCE',
            'student_id': self.generate_student_id(),
            'course_id': self.generate_course_id(),
            'timestamp': datetime.now().isoformat(),
            'session_id': f"SES{random.randint(1, 100):04d}",
            'session_type': random.choice(['Lecture', 'Lab', 'Tutorial', 'Seminar']),
            'check_type': random.choice(['CHECK_IN', 'CHECK_OUT']),
            'location': random.choice(['Room A101', 'Room B202', 'Lab C303', 'Online']),
            'teacher_id': self.generate_teacher_id(),
            'is_late': random.choice([True, False]),
            'minutes_late': random.randint(0, 30) if random.random() < 0.2 else 0
        }
    
    def generate_event(self) -> Dict[str, Any]:
        """Generate a random event based on weighted probabilities"""
        event_type = random.choices(
            list(self.event_types.keys()),
            weights=list(self.event_types.values())
        )[0]
        
        event_generators = {
            'LOGIN': self.generate_login_event,
            'VIDEO_VIEW': self.generate_video_view_event,
            'ASSIGNMENT_SUBMIT': self.generate_assignment_submit_event,
            'QUIZ_ATTEMPT': self.generate_quiz_attempt_event,
            'FORUM_POST': self.generate_forum_post_event,
            'MATERIAL_DOWNLOAD': self.generate_material_download_event
        }
        
        return event_generators[event_type]()
    
    def send_event(self, topic: str, event: Dict[str, Any], key: str = None):
        """
        Send an event to Kafka topic
        
        Args:
            topic: Kafka topic name
            event: Event data as dictionary
            key: Optional message key for partitioning
        """
        try:
            # Use student_id as key for consistent partitioning
            if key is None and 'student_id' in event:
                key = event['student_id']
            
            future = self.producer.send(topic, value=event, key=key)
            # Wait for acknowledgment
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Sent {event['event_type']} to {topic} "
                f"[partition={record_metadata.partition}, offset={record_metadata.offset}]"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to send event to {topic}: {e}")
            return False
    
    def simulate_continuous(self, events_per_second: float = 5.0, duration_seconds: int = None):
        """
        Continuously generate and send events
        
        Args:
            events_per_second: Rate of event generation
            duration_seconds: How long to run (None = infinite)
        """
        logger.info(f"Starting continuous simulation at {events_per_second} events/second")
        
        start_time = time.time()
        event_count = 0
        
        try:
            while True:
                # Check duration
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break
                
                # Generate and send student activity event
                activity_event = self.generate_event()
                self.send_event('student-activities', activity_event)
                event_count += 1
                
                # Occasionally generate attendance events (10% chance)
                if random.random() < 0.1:
                    attendance_event = self.generate_attendance_event()
                    self.send_event('student-attendance', attendance_event)
                    event_count += 1
                
                # Log progress every 100 events
                if event_count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed if elapsed > 0 else 0
                    logger.info(f"Sent {event_count} events (rate: {rate:.2f}/sec)")
                
                # Sleep to maintain desired rate
                time.sleep(1.0 / events_per_second)
                
        except KeyboardInterrupt:
            logger.info("Simulation stopped by user")
        finally:
            elapsed = time.time() - start_time
            logger.info(f"Simulation complete: {event_count} events in {elapsed:.2f} seconds")
            self.close()
    
    def simulate_burst(self, num_events: int = 1000):
        """
        Generate a burst of events as fast as possible
        
        Args:
            num_events: Number of events to generate
        """
        logger.info(f"Starting burst simulation: {num_events} events")
        
        start_time = time.time()
        success_count = 0
        
        for i in range(num_events):
            # Generate activity event
            activity_event = self.generate_event()
            if self.send_event('student-activities', activity_event):
                success_count += 1
            
            # Log progress
            if (i + 1) % 100 == 0:
                logger.info(f"Progress: {i + 1}/{num_events} events")
        
        elapsed = time.time() - start_time
        rate = success_count / elapsed if elapsed > 0 else 0
        
        logger.info(
            f"Burst complete: {success_count}/{num_events} events in {elapsed:.2f}s "
            f"(rate: {rate:.2f}/sec)"
        )
        
        self.close()
    
    def close(self):
        """Close the Kafka producer connection"""
        if self.producer:
            logger.info("Flushing remaining messages...")
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")


def main():
    """Main function to run the simulator"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Student Activity Signal Simulator')
    parser.add_argument(
        '--mode',
        choices=['continuous', 'burst'],
        default='continuous',
        help='Simulation mode'
    )
    parser.add_argument(
        '--rate',
        type=float,
        default=5.0,
        help='Events per second (continuous mode)'
    )
    parser.add_argument(
        '--events',
        type=int,
        default=1000,
        help='Number of events (burst mode)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Duration in seconds (continuous mode, None=infinite)'
    )
    parser.add_argument(
        '--kafka',
        default='localhost:9092',
        help='Kafka bootstrap server'
    )
    
    args = parser.parse_args()
    
    # Create simulator
    simulator = StudentActivitySimulator(bootstrap_servers=[args.kafka])
    
    # Run simulation based on mode
    if args.mode == 'continuous':
        simulator.simulate_continuous(
            events_per_second=args.rate,
            duration_seconds=args.duration
        )
    else:
        simulator.simulate_burst(num_events=args.events)


if __name__ == '__main__':
    main()
