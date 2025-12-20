import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

import os

# Cấu hình Kafka
KAFKA_TOPIC_AUTH = 'auth_topic'
KAFKA_TOPIC_ASSESSMENT = 'assessment_topic'
KAFKA_TOPIC_VIDEO = 'video_topic'
KAFKA_TOPIC_COURSE = 'course_topic'
KAFKA_TOPIC_PROFILE = 'profile_topic'
KAFKA_TOPIC_NOTIFICATION = 'notification_topic'

# Read from env or default to localhost:9092
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

fake = Faker(['vi_VN', 'en_US'])  # Vietnamese and English locales

# Danh sách ID giả định
STUDENT_IDS = [f"SV{i:03d}" for i in range(1, 51)]  # 50 students
TEACHER_IDS = [f"GV{i:03d}" for i in range(1, 11)]  # 10 teachers
COURSE_IDS = [f"COURSE{i:03d}" for i in range(1, 21)]  # 20 courses
ASSIGNMENT_IDS = [f"ASM{i:03d}" for i in range(1, 101)]  # 100 assignments
QUIZ_IDS = [f"QUIZ{i:03d}" for i in range(1, 51)]  # 50 quizzes
VIDEO_IDS = [f"VID{i:03d}" for i in range(1, 201)]  # 200 videos

# Session tracking (để mô phỏng thực tế hơn)
active_sessions = {}

import pytz
vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')

def get_timestamp():
    return datetime.now(vn_tz).strftime("%Y-%m-%d %H:%M:%S")

# --- PRODUCER FUNCTIONS ---

# ============= 1. AUTH EVENTS =============

def generate_auth_event():
    """Auth Producer: Login, Logout, Signup"""
    user_type = random.choice(['student', 'teacher'])
    user_id = random.choice(STUDENT_IDS) if user_type == 'student' else random.choice(TEACHER_IDS)
    
    event_type = random.choice(['LOGIN', 'LOGOUT', 'SIGNUP'])
    
    if event_type == 'LOGIN':
        session_id = fake.uuid4()
        active_sessions[user_id] = {
            'session_id': session_id,
            'login_time': datetime.now()
        }
        data = {
            "event_category": "AUTH",
            "event_type": "LOGIN",
            "user_id": user_id,
            "role": user_type,
            "timestamp": get_timestamp(),
            "session_id": session_id
        }
    
    elif event_type == 'LOGOUT':
        session_info = active_sessions.get(user_id, {
            'session_id': fake.uuid4(),
            'login_time': datetime.now() - timedelta(hours=2)
        })
        data = {
            "event_category": "AUTH",
            "event_type": "LOGOUT",
            "user_id": user_id,
            "role": user_type,
            "timestamp": get_timestamp(),
            "session_id": session_info['session_id']
        }
        if user_id in active_sessions:
            del active_sessions[user_id]
    
    elif event_type == 'SIGNUP':
        data = {
            "event_category": "AUTH",
            "event_type": "SIGNUP",
            "user_id": user_id,
            "role": user_type,
            "timestamp": get_timestamp(),
            "email": f"{user_id.lower()}@university.edu",
            "registration_source": random.choice(['web', 'mobile', 'admin'])
        }
    
    
    return KAFKA_TOPIC_AUTH, data


# ============= 2. ASSESSMENT EVENTS =============

def generate_assessment_event():
    """Assessment Producer: View, Submit, Edit, Delete, Grade, Quiz"""
    event_type = random.choice([
        'VIEW_ASSIGNMENT_FIRST_TIME', 'SUBMIT_ASSIGNMENT', 'EDIT_ASSIGNMENT', 
        'DELETE_ASSIGNMENT', 'QUIZ_COMPLETED', 'VIEW_STUDENT_WORK', 
        'GRADE_ASSIGNMENT', 'ASSIGNMENT_REOPENED'
    ])
    
    # Determine user role based on event type
    teacher_events = ['VIEW_STUDENT_WORK', 'GRADE_ASSIGNMENT', 'ASSIGNMENT_REOPENED']
    user_id = random.choice(TEACHER_IDS) if event_type in teacher_events else random.choice(STUDENT_IDS)
    role = 'teacher' if event_type in teacher_events else 'student'
    
    course_id = random.choice(COURSE_IDS)
    
    if event_type == 'VIEW_ASSIGNMENT_FIRST_TIME':
        assignment_id = random.choice(ASSIGNMENT_IDS)
        data = {
            "event_category": "ASSESSMENT",
            "event_type": "VIEW_ASSIGNMENT_FIRST_TIME",
            "user_id": user_id,
            "role": role,
            "assignment_id": assignment_id,
            "course_id": course_id,
            "timestamp": get_timestamp(),
            "assignment_type": random.choice(['homework', 'project', 'essay', 'lab'])
        }
    
    elif event_type == 'SUBMIT_ASSIGNMENT':
        assignment_id = random.choice(ASSIGNMENT_IDS)
        data = {
            "event_category": "ASSESSMENT",
            "event_type": "SUBMIT_ASSIGNMENT",
            "user_id": user_id,
            "role": role,
            "assignment_id": assignment_id,
            "course_id": course_id,
            "timestamp": get_timestamp(),
            "file_url": f"s3://submissions/{user_id}_{assignment_id}.pdf"
        }
    
    elif event_type == 'EDIT_ASSIGNMENT':
        assignment_id = random.choice(ASSIGNMENT_IDS)
        data = {
            "event_category": "ASSESSMENT",
            "event_type": "EDIT_ASSIGNMENT",
            "user_id": user_id,
            "role": role,
            "assignment_id": assignment_id,
            "course_id": course_id,
            "timestamp": get_timestamp(),
            "file_url": f"s3://submissions/{user_id}_{assignment_id}_v2.pdf"
        }
    
    elif event_type == 'DELETE_ASSIGNMENT':
        data = {
            "event_category": "ASSESSMENT",
            "event_type": "DELETE_ASSIGNMENT",
            "user_id": user_id,
            "role": role,
            "assignment_id": random.choice(ASSIGNMENT_IDS),
            "timestamp": get_timestamp()
        }
    
    elif event_type == 'QUIZ_COMPLETED':
        quiz_id = random.choice(QUIZ_IDS)
        correct_answers = random.randint(5, 50)  # Raw number of correct answers
        
        data = {
            "event_category": "ASSESSMENT",
            "event_type": "QUIZ_COMPLETED",
            "user_id": user_id,
            "role": role,
            "quiz_id": quiz_id,
            "course_id": course_id,
            "timestamp": get_timestamp(),
            "correct_answers": correct_answers
        }
    
    elif event_type == 'VIEW_STUDENT_WORK':
        student_id = random.choice(STUDENT_IDS)
        data = {
            "event_category": "ASSESSMENT",
            "event_type": "VIEW_STUDENT_WORK",
            "user_id": user_id,
            "role": role,
            "student_id": student_id,
            "assignment_id": random.choice(ASSIGNMENT_IDS),
            "course_id": course_id,
            "timestamp": get_timestamp()
        }
    
    elif event_type == 'GRADE_ASSIGNMENT':
        student_id = random.choice(STUDENT_IDS)
        score = random.randint(40, 100)  # Raw score value
        
        data = {
            "event_category": "ASSESSMENT",
            "event_type": "GRADE_ASSIGNMENT",
            "user_id": user_id,
            "role": role,
            "student_id": student_id,
            "assignment_id": random.choice(ASSIGNMENT_IDS),
            "course_id": course_id,
            "timestamp": get_timestamp(),
            "score": score
        }
    
    else:  # ASSIGNMENT_REOPENED
        data = {
            "event_category": "ASSESSMENT",
            "event_type": "ASSIGNMENT_REOPENED",
            "user_id": user_id,
            "role": role,
            "student_id": random.choice(STUDENT_IDS),
            "assignment_id": random.choice(ASSIGNMENT_IDS),
            "timestamp": get_timestamp(),
            "new_due_date": (datetime.now() + timedelta(days=random.randint(2, 7))).strftime("%Y-%m-%d 23:59:00")
        }
    
    return KAFKA_TOPIC_ASSESSMENT, data


# ============= 3. VIDEO TRACKING EVENTS =============

def generate_video_tracking_event():
    """Video Tracking Producer: Record raw watch duration per session"""
    user_id = random.choice(STUDENT_IDS)
    video_id = random.choice(VIDEO_IDS)
    course_id = random.choice(COURSE_IDS)
    session_id = fake.uuid4()
    
    # Simulate watch duration per session (in seconds)
    # Students might watch in multiple short sessions or one long session
    watch_duration_seconds = random.choices(
        population=[30, 60, 120, 300, 600, 1200, 1800],  # Various session lengths
        weights=[10, 15, 20, 25, 15, 10, 5],  # Shorter sessions more common
        k=1
    )[0]
    
    data = {
        "event_category": "VIDEO",
        "event_type": "VIDEO_WATCHED",
        "user_id": user_id,
        "role": "student",
        "video_id": video_id,
        "course_id": course_id,
        "timestamp": get_timestamp(),
        "watch_duration_seconds": watch_duration_seconds,
        "session_id": session_id
    }
    
    return KAFKA_TOPIC_VIDEO, data



# ============= 4. COURSE INTERACTION EVENTS =============

def generate_course_interaction_event():
    """Course Interaction Producer: Create, Delete, Complete, View, Update, Enroll, etc."""
    event_type = random.choice([
        'COURSE_CREATED', 'COURSE_DELETED', 'COURSE_VIEW', 
        'ITEM_CLICK', 'DOWNLOAD_MATERIAL', 'COURSE_ENROLLED', 'COURSE_DROPPED'
    ])
    
    # GV creates/updates/deletes, SV views/enrolls/completes
    teacher_events = ['COURSE_CREATED', 'COURSE_DELETED', 'COURSE_UPDATED']
    both_events = ['FORUM_POST_CREATED']
    
    if event_type in teacher_events:
        user_id = random.choice(TEACHER_IDS)
        role = 'teacher'
    elif event_type in both_events:
        user_type = random.choice(['student', 'teacher'])
        user_id = random.choice(STUDENT_IDS) if user_type == 'student' else random.choice(TEACHER_IDS)
        role = user_type
    else:
        user_id = random.choice(STUDENT_IDS)
        role = 'student'
    
    course_id = random.choice(COURSE_IDS)
    
    if event_type == 'COURSE_CREATED':
        data = {
            "event_category": "COURSE",
            "event_type": "COURSE_CREATED",
            "user_id": user_id,
            "role": role,
            "course_id": course_id,
            "timestamp": get_timestamp(),
            "course_name": fake.catch_phrase(),
            "course_code": f"CS{random.randint(100, 499)}",
            "semester": random.choice(["Fall 2025", "Spring 2026", "Summer 2026"]),
            "max_students": random.choice([30, 50, 100, 150])
        }
    
    elif event_type == 'COURSE_DELETED':
        data = {
            "event_category": "COURSE",
            "event_type": "COURSE_DELETED",
            "user_id": user_id,
            "role": role,
            "course_id": course_id,
            "timestamp": get_timestamp()
        }
    
    elif event_type == 'COURSE_VIEW':
        data = {
            "event_category": "COURSE",
            "event_type": "COURSE_VIEW",
            "user_id": user_id,
            "role": role,
            "course_id": course_id,
            "timestamp": get_timestamp()
        }
    
    elif event_type == 'ITEM_CLICK':
        data = {
            "event_category": "COURSE",
            "event_type": "ITEM_CLICK",
            "user_id": user_id,
            "role": role,
            "course_id": course_id,
            "item_id": f"ITEM{random.randint(1, 500)}",
            "item_type": random.choice(['lesson', 'module', 'quiz', 'assignment', 'discussion']),
            "timestamp": get_timestamp()
        }
    
    elif event_type == 'DOWNLOAD_MATERIAL':
        data = {
            "event_category": "COURSE",
            "event_type": "DOWNLOAD_MATERIAL",
            "user_id": user_id,
            "role": role,
            "course_id": course_id,
            "timestamp": get_timestamp(),
            "material_id": f"MAT{random.randint(1, 200)}"
        }
    
    elif event_type == 'COURSE_ENROLLED':
        data = {
            "event_category": "COURSE",
            "event_type": "COURSE_ENROLLED",
            "user_id": user_id,
            "role": role,
            "course_id": course_id,
            "timestamp": get_timestamp()
        }
    
    elif event_type == 'COURSE_DROPPED':
        data = {
            "event_category": "COURSE",
            "event_type": "COURSE_DROPPED",
            "user_id": user_id,
            "role": role,
            "course_id": course_id,
            "timestamp": get_timestamp()
        }
    
    return KAFKA_TOPIC_COURSE, data


# ============= 5. PROFILE EVENTS =============

def generate_profile_event():
    """Profile Producer: Create, Update, Picture Update, Password Change"""
    event_type = random.choice([
        'CREATE_PROFILE', 'UPDATE_PROFILE', 
        'PROFILE_PICTURE_UPDATED', 'PASSWORD_CHANGED'
    ])
    
    user_type = random.choice(['student', 'teacher'])
    user_id = random.choice(STUDENT_IDS) if user_type == 'student' else random.choice(TEACHER_IDS)
    
    if event_type == 'CREATE_PROFILE':
        data = {
            "event_category": "PROFILE",
            "event_type": "CREATE_PROFILE",
            "user_id": user_id,
            "role": user_type,
            "timestamp": get_timestamp(),
            "email": f"{user_id.lower()}@university.edu",
            "full_name": fake.name(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=60).strftime("%Y-%m-%d"),
            "gender": random.choice(['male', 'female', 'other']),
            "phone_number": fake.phone_number(),
            "major": random.choice([
                "Computer Science", "Data Science", "Software Engineering",
                "Information Technology", "Cybersecurity"
            ]) if user_type == 'student' else None,
            "enrollment_year": random.randint(2018, 2024) if user_type == 'student' else None
        }
    
    elif event_type == 'UPDATE_PROFILE':
        data = {
            "event_category": "PROFILE",
            "event_type": "UPDATE_PROFILE",
            "user_id": user_id,
            "role": user_type,
            "timestamp": get_timestamp(),
            "updated_fields": random.sample(['phone_number', 'email', 'address', 'emergency_contact'], k=random.randint(1, 2)),
            "old_email": f"old.{user_id.lower()}@university.edu",
            "new_email": f"{user_id.lower()}@university.edu",
            "old_phone": fake.phone_number(),
            "new_phone": fake.phone_number()
        }
    
    elif event_type == 'PROFILE_PICTURE_UPDATED':
        data = {
            "event_category": "PROFILE",
            "event_type": "PROFILE_PICTURE_UPDATED",
            "user_id": user_id,
            "role": user_type,
            "timestamp": get_timestamp(),
            "picture_url": f"s3://profiles/{user_id}_avatar.jpg"
        }
    
    else:  # PASSWORD_CHANGED
        data = {
            "event_category": "PROFILE",
            "event_type": "PASSWORD_CHANGED",
            "user_id": user_id,
            "role": user_type,
            "timestamp": get_timestamp()
        }
    
    return KAFKA_TOPIC_PROFILE, data


# ============= 6. NOTIFICATION EVENTS (NEW) =============

def generate_notification_event():
    """Notification Producer: Track notification delivery and engagement"""
    user_id = random.choice(STUDENT_IDS + TEACHER_IDS)
    
    event_type = random.choice(['NOTIFICATION_SENT', 'NOTIFICATION_OPENED', 'NOTIFICATION_CLICKED'])
    
    data = {
        "event_category": "NOTIFICATION",
        "event_type": event_type,
        "user_id": user_id,
        "role": 'student' if user_id in STUDENT_IDS else 'teacher',
        "notification_id": f"NOTIF{random.randint(1, 10000)}",
        "timestamp": get_timestamp(),
        "notification_type": random.choice([
            'assignment_reminder',
            'grade_posted',
            'course_announcement',
            'deadline_approaching',
            'new_message'
        ]),
        "channel": random.choice(['email', 'push', 'sms', 'in_app']),
        "related_object_id": random.choice(ASSIGNMENT_IDS + COURSE_IDS)
    }
    
    return KAFKA_TOPIC_NOTIFICATION, data


# --- MAIN LOOP ---

def simulate():
    """Main simulation loop - generates realistic event streams"""
    print("=" * 60)
    print("UNIVERSITY LEARNING ANALYTICS - DATA PRODUCER (v2.0)")
    print("=" * 60)
    print(f"Students: {len(STUDENT_IDS)} | Teachers: {len(TEACHER_IDS)} | Courses: {len(COURSE_IDS)}")
    print(f"Kafka Topics: 6 (auth, assessment, video, course, profile, notification)")
    print("Press Ctrl+C to stop...")
    print("=" * 60)
    
    event_count = {
        'AUTH': 0,
        'ASSESSMENT': 0,
        'VIDEO': 0,
        'COURSE': 0,
        'PROFILE': 0,
        'NOTIFICATION': 0
    }
    
    try:
        while True:
            # Weighted random selection (more realistic distribution)
            event_generators = random.choices(
                population=[
                    generate_auth_event,
                    generate_assessment_event,
                    generate_video_tracking_event,
                    generate_course_interaction_event,
                    generate_profile_event,
                    generate_notification_event
                ],
                weights=[15, 25, 35, 18, 3, 4],  # Video and assessment are most common
                k=1
            )[0]
            
            topic, data = event_generators()
            
            # Callbacks for delivery reports
            def on_send_success(record_metadata):
                pass # Successfully delivered
            
            def on_send_error(excp):
                print(f"!!! Error sending to Kafka: {excp}", flush=True)

            # Send to Kafka with callbacks
            # We use user_id as the key to ensure all events for the same user go to the same partition
            # This is also REQUIRED for compacted topics like profile_topic
            # The key_serializer now handles the encoding.
            producer.send(
                topic, 
                key=data['user_id'],
                value=data
            ).add_callback(on_send_success).add_errback(on_send_error)
            
            # Update counters
            event_category = data['event_category']
            event_count[event_category] += 1
            
            # Pretty print
            event_type = data.get('event_type', data.get('action', 'UNKNOWN'))
            user_id = data['user_id']
            role = data.get('role', '?')
            
            print(f"[{event_category:12}] {event_type:30} | {user_id} ({role:7}) | Total: {sum(event_count.values())}")
            
            # Show statistics every 50 events
            if sum(event_count.values()) % 50 == 0:
                print("\n" + "-" * 60)
                print("EVENT STATISTICS:")
                for cat, count in sorted(event_count.items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / sum(event_count.values())) * 100
                    print(f"  {cat:15}: {count:5} events ({percentage:5.1f}%)")
                print("-" * 60 + "\n")
            
            # Variable sleep time for realistic simulation
            time.sleep(random.uniform(0.1, 1.5))
            
    except KeyboardInterrupt:
        print("\n\n" + "=" * 60)
        print("SIMULATION STOPPED")
        print("=" * 60)
        print("\nFinal Statistics:")
        total = sum(event_count.values())
        for cat, count in sorted(event_count.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total) * 100 if total > 0 else 0
            print(f"  {cat:15}: {count:5} events ({percentage:5.1f}%)")
        print(f"\nTotal Events Generated: {total}")
        print("=" * 60)
        producer.close()


if __name__ == "__main__":
    simulate()