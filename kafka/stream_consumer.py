"""
Student Activity Stream Consumer - Kafka Consumer
This script consumes student activity events from Kafka and processes them in real-time.

Features:
- Consumes from multiple topics
- Processes events in real-time
- Calculates engagement metrics
- Generates alerts for at-risk students
- Stores processed data (can be extended to MongoDB/HDFS)
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List
from collections import defaultdict
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StudentActivityConsumer:
    """Consumes and processes student activity events from Kafka"""
    
    def __init__(self, bootstrap_servers: List[str] = ['localhost:9092'],
                 group_id: str = 'student-analytics-group'):
        """
        Initialize the consumer
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer = None
        self.running = False
        
        # Metrics storage (in production, use database)
        self.student_metrics = defaultdict(lambda: {
            'total_activities': 0,
            'login_count': 0,
            'video_views': 0,
            'assignments_submitted': 0,
            'quiz_attempts': 0,
            'forum_posts': 0,
            'total_video_watch_time': 0,
            'avg_quiz_score': 0,
            'last_activity': None
        })
        
        self.attendance_records = defaultdict(list)
        self.processed_count = 0
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def connect_to_kafka(self, topics: List[str]):
        """
        Establish connection to Kafka and subscribe to topics
        
        Args:
            topics: List of topic names to subscribe to
        """
        try:
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='earliest',  # Start from beginning if no offset
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                session_timeout_ms=30000,
                max_poll_records=500,
                max_poll_interval_ms=300000
            )
            logger.info(f"Successfully connected to Kafka at {self.bootstrap_servers}")
            logger.info(f"Subscribed to topics: {topics}")
            logger.info(f"Consumer group: {self.group_id}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def process_login_event(self, event: Dict[str, Any]):
        """Process LOGIN event"""
        student_id = event['student_id']
        self.student_metrics[student_id]['login_count'] += 1
        self.student_metrics[student_id]['last_activity'] = event['timestamp']
        
        logger.debug(f"Login: {student_id} from {event['device_type']}")
    
    def process_video_view_event(self, event: Dict[str, Any]):
        """Process VIDEO_VIEW event"""
        student_id = event['student_id']
        metrics = self.student_metrics[student_id]
        
        metrics['video_views'] += 1
        metrics['total_video_watch_time'] += event['watch_time_seconds']
        metrics['last_activity'] = event['timestamp']
        
        # Alert if completion rate is very low
        if event['completion_rate'] < 20:
            self._generate_alert(
                student_id,
                'LOW_VIDEO_COMPLETION',
                f"Student watched only {event['completion_rate']:.1f}% of video {event['video_id']}"
            )
        
        logger.debug(
            f"Video View: {student_id} watched {event['video_id']} "
            f"({event['completion_rate']:.1f}% complete)"
        )
    
    def process_assignment_submit_event(self, event: Dict[str, Any]):
        """Process ASSIGNMENT_SUBMIT event"""
        student_id = event['student_id']
        metrics = self.student_metrics[student_id]
        
        metrics['assignments_submitted'] += 1
        metrics['last_activity'] = event['timestamp']
        
        # Alert if submission is late
        if event['submission_status'] == 'Late':
            self._generate_alert(
                student_id,
                'LATE_SUBMISSION',
                f"Late submission for {event['assignment_id']}"
            )
        
        logger.debug(
            f"Assignment: {student_id} submitted {event['assignment_id']} "
            f"({event['submission_status']})"
        )
    
    def process_quiz_attempt_event(self, event: Dict[str, Any]):
        """Process QUIZ_ATTEMPT event"""
        student_id = event['student_id']
        metrics = self.student_metrics[student_id]
        
        metrics['quiz_attempts'] += 1
        
        # Update average quiz score
        current_avg = metrics['avg_quiz_score']
        total_attempts = metrics['quiz_attempts']
        metrics['avg_quiz_score'] = (
            (current_avg * (total_attempts - 1) + event['score']) / total_attempts
        )
        metrics['last_activity'] = event['timestamp']
        
        # Alert if score is very low
        if event['score'] < 40:
            self._generate_alert(
                student_id,
                'LOW_QUIZ_SCORE',
                f"Low score ({event['score']:.1f}%) on quiz {event['quiz_id']}"
            )
        
        logger.debug(
            f"Quiz: {student_id} scored {event['score']:.1f}% on {event['quiz_id']}"
        )
    
    def process_forum_post_event(self, event: Dict[str, Any]):
        """Process FORUM_POST event"""
        student_id = event['student_id']
        metrics = self.student_metrics[student_id]
        
        metrics['forum_posts'] += 1
        metrics['last_activity'] = event['timestamp']
        
        logger.debug(
            f"Forum: {student_id} posted {event['post_type']} "
            f"({event['word_count']} words)"
        )
    
    def process_material_download_event(self, event: Dict[str, Any]):
        """Process MATERIAL_DOWNLOAD event"""
        student_id = event['student_id']
        metrics = self.student_metrics[student_id]
        
        metrics['last_activity'] = event['timestamp']
        
        logger.debug(
            f"Download: {student_id} downloaded {event['material_type']} "
            f"({event['file_size_mb']:.2f} MB)"
        )
    
    def process_attendance_event(self, event: Dict[str, Any]):
        """Process ATTENDANCE event"""
        student_id = event['student_id']
        self.attendance_records[student_id].append(event)
        
        # Alert if student is frequently late
        if event['is_late'] and event['minutes_late'] > 15:
            self._generate_alert(
                student_id,
                'LATE_ATTENDANCE',
                f"Student arrived {event['minutes_late']} minutes late to {event['session_id']}"
            )
        
        logger.debug(
            f"Attendance: {student_id} {event['check_type']} at {event['location']} "
            f"({'Late' if event['is_late'] else 'On Time'})"
        )
    
    def _generate_alert(self, student_id: str, alert_type: str, message: str):
        """
        Generate an alert for at-risk students
        
        Args:
            student_id: Student identifier
            alert_type: Type of alert
            message: Alert message
        """
        alert = {
            'alert_id': f"ALT-{datetime.now().timestamp()}",
            'student_id': student_id,
            'alert_type': alert_type,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'severity': 'WARNING' if 'LOW' in alert_type else 'INFO'
        }
        
        logger.warning(f"ALERT: {alert_type} - {message}")
        
        # In production, send this to alerts topic or notification system
        # self.producer.send('alerts', value=alert)
    
    def calculate_engagement_score(self, student_id: str) -> float:
        """
        Calculate engagement score for a student
        
        Args:
            student_id: Student identifier
            
        Returns:
            Engagement score (0-100)
        """
        metrics = self.student_metrics[student_id]
        
        if metrics['total_activities'] == 0:
            return 0.0
        
        # Weighted engagement calculation
        score = 0.0
        score += min(metrics['login_count'] * 2, 20)  # Max 20 points
        score += min(metrics['video_views'] * 3, 25)  # Max 25 points
        score += min(metrics['assignments_submitted'] * 10, 30)  # Max 30 points
        score += min(metrics['quiz_attempts'] * 5, 15)  # Max 15 points
        score += min(metrics['forum_posts'] * 2, 10)  # Max 10 points
        
        return min(score, 100.0)
    
    def process_event(self, event: Dict[str, Any], topic: str):
        """
        Route event to appropriate processor
        
        Args:
            event: Event data
            topic: Topic name
        """
        try:
            event_type = event.get('event_type')
            student_id = event.get('student_id')
            
            if not event_type or not student_id:
                logger.warning(f"Invalid event: missing event_type or student_id")
                return
            
            # Update total activity count
            self.student_metrics[student_id]['total_activities'] += 1
            
            # Route to specific processor
            processors = {
                'LOGIN': self.process_login_event,
                'VIDEO_VIEW': self.process_video_view_event,
                'ASSIGNMENT_SUBMIT': self.process_assignment_submit_event,
                'QUIZ_ATTEMPT': self.process_quiz_attempt_event,
                'FORUM_POST': self.process_forum_post_event,
                'MATERIAL_DOWNLOAD': self.process_material_download_event,
                'ATTENDANCE': self.process_attendance_event
            }
            
            processor = processors.get(event_type)
            if processor:
                processor(event)
            else:
                logger.warning(f"Unknown event type: {event_type}")
            
            self.processed_count += 1
            
        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)
    
    def print_statistics(self):
        """Print current processing statistics"""
        logger.info("=" * 70)
        logger.info(f"STATISTICS - Processed Events: {self.processed_count}")
        logger.info(f"Unique Students: {len(self.student_metrics)}")
        logger.info("=" * 70)
        
        # Top 5 most active students
        top_students = sorted(
            self.student_metrics.items(),
            key=lambda x: x[1]['total_activities'],
            reverse=True
        )[:5]
        
        logger.info("Top 5 Most Active Students:")
        for student_id, metrics in top_students:
            engagement = self.calculate_engagement_score(student_id)
            logger.info(
                f"  {student_id}: {metrics['total_activities']} activities, "
                f"Engagement: {engagement:.1f}/100"
            )
        
        # Event type distribution
        event_counts = defaultdict(int)
        for metrics in self.student_metrics.values():
            event_counts['LOGIN'] += metrics['login_count']
            event_counts['VIDEO_VIEW'] += metrics['video_views']
            event_counts['ASSIGNMENT'] += metrics['assignments_submitted']
            event_counts['QUIZ'] += metrics['quiz_attempts']
            event_counts['FORUM'] += metrics['forum_posts']
        
        logger.info("\nEvent Distribution:")
        for event_type, count in event_counts.items():
            logger.info(f"  {event_type}: {count}")
        
        logger.info("=" * 70)
    
    def consume_messages(self, topics: List[str], statistics_interval: int = 100):
        """
        Start consuming messages from Kafka
        
        Args:
            topics: List of topics to consume from
            statistics_interval: Print statistics every N messages
        """
        self.connect_to_kafka(topics)
        self.running = True
        
        logger.info("Starting message consumption...")
        logger.info("Press Ctrl+C to stop")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                # Process the message
                self.process_event(message.value, message.topic)
                
                # Print statistics periodically
                if self.processed_count % statistics_interval == 0:
                    self.print_statistics()
                
        except KeyboardInterrupt:
            logger.info("Consumption interrupted by user")
        except Exception as e:
            logger.error(f"Error during consumption: {e}", exc_info=True)
        finally:
            self.close()
    
    def close(self):
        """Close the Kafka consumer connection"""
        if self.consumer:
            logger.info("Closing consumer...")
            self.consumer.close()
            logger.info("Consumer closed")
            
            # Print final statistics
            self.print_statistics()


def main():
    """Main function to run the consumer"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Student Activity Stream Consumer')
    parser.add_argument(
        '--topics',
        nargs='+',
        default=['student-activities', 'student-attendance'],
        help='Kafka topics to consume from'
    )
    parser.add_argument(
        '--group',
        default='student-analytics-group',
        help='Consumer group ID'
    )
    parser.add_argument(
        '--kafka',
        default='localhost:9092',
        help='Kafka bootstrap server'
    )
    parser.add_argument(
        '--stats-interval',
        type=int,
        default=100,
        help='Print statistics every N messages'
    )
    
    args = parser.parse_args()
    
    # Create and run consumer
    consumer = StudentActivityConsumer(
        bootstrap_servers=[args.kafka],
        group_id=args.group
    )
    
    consumer.consume_messages(
        topics=args.topics,
        statistics_interval=args.stats_interval
    )


if __name__ == '__main__':
    main()
