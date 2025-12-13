# Event Schema Specification
## University Learning Analytics System - Kafka Event Streams

This document defines all event types, their schemas, and data flows for the student learning analytics system.

---

## Overview

The system captures 6 main categories of events:
1. **Authentication Events** - Login/Logout/Signup activities
2. **Assessment Events** - Assignment and quiz interactions
3. **Video Tracking Events** - Video watching behavior (raw watch duration)
4. **Course Interaction Events** - Course-related activities
5. **Profile Events** - User profile management
6. **Notification Events** - Notification delivery and engagement

---

## 1. Authentication Events (auth_topic)

### Event Types:

#### 1.1 LOGIN
**Triggered when:** Student or teacher logs into the system
```json
{
  "event_category": "AUTH",
  "event_type": "LOGIN",
  "user_id": "SV001",
  "role": "student",
  "timestamp": "2025-12-10 14:30:00",
  "session_id": "session_uuid_12345"
}
```

#### 1.2 LOGOUT
**Triggered when:** User logs out
```json
{
  "event_category": "AUTH",
  "event_type": "LOGOUT",
  "user_id": "SV001",
  "role": "student",
  "timestamp": "2025-12-10 16:45:00",
  "session_id": "session_uuid_12345"
}
```

#### 1.3 SIGNUP
**Triggered when:** New user creates an account
```json
{
  "event_category": "AUTH",
  "event_type": "SIGNUP",
  "user_id": "SV099",
  "role": "student",
  "timestamp": "2025-12-10 10:00:00",
  "email": "student099@university.edu",
  "registration_source": "web"
}
```

---

## 2. Assessment Events (assessment_topic)

### Event Types:

#### 2.1 VIEW_ASSIGNMENT_FIRST_TIME (Student)
**Triggered when:** Student views an assignment for the first time
```json
{
  "event_category": "ASSESSMENT",
  "event_type": "VIEW_ASSIGNMENT_FIRST_TIME",
  "user_id": "SV001",
  "role": "student",
  "assignment_id": "ASM001",
  "course_id": "COURSE001",
  "timestamp": "2025-12-10 09:00:00",
  "assignment_type": "homework",
}
```

#### 2.2 SUBMIT_ASSIGNMENT (Student)
**Triggered when:** Student submits an assignment
```json
{
  "event_category": "ASSESSMENT",
  "event_type": "SUBMIT_ASSIGNMENT",
  "user_id": "SV001",
  "role": "student",
  "assignment_id": "ASM001",
  "course_id": "COURSE001",
  "timestamp": "2025-12-14 22:30:00",
  "file_url": "s3://submissions/SV001_ASM001.pdf",
}
```

#### 2.3 EDIT_ASSIGNMENT (Student)
**Triggered when:** Student edits their submission before deadline
```json
{
  "event_category": "ASSESSMENT",
  "event_type": "EDIT_ASSIGNMENT",
  "user_id": "SV001",
  "role": "student",
  "assignment_id": "ASM001",
  "course_id": "COURSE001",
  "timestamp": "2025-12-15 10:00:00",
  "file_url": "s3://submissions/SV001_ASM001_v2.pdf",
}
```

#### 2.4 DELETE_ASSIGNMENT (Student)
**Triggered when:** Student deletes their submission
```json
{
  "event_category": "ASSESSMENT",
  "event_type": "DELETE_ASSIGNMENT",
  "user_id": "SV001",
  "role": "student",
  "assignment_id": "ASM001",
  "timestamp": "2025-12-15 11:00:00"
}
```

#### 2.5 QUIZ_COMPLETED (Student)
**Triggered when:** Student completes a quiz
```json
{
  "event_category": "ASSESSMENT",
  "event_type": "QUIZ_COMPLETED",
  "user_id": "SV001",
  "role": "student",
  "quiz_id": "QUIZ001",
  "course_id": "COURSE001",
  "timestamp": "2025-12-10 14:30:00",
  "correct_answers": 17
}
```

#### 2.6 VIEW_STUDENT_WORK (Teacher)
**Triggered when:** Teacher views a student's submission
```json
{
  "event_category": "ASSESSMENT",
  "event_type": "VIEW_STUDENT_WORK",
  "user_id": "GV001",
  "role": "teacher",
  "student_id": "SV001",
  "assignment_id": "ASM001",
  "course_id": "COURSE001",
  "timestamp": "2025-12-16 09:00:00"
}
```

#### 2.7 GRADE_ASSIGNMENT (Teacher)
**Triggered when:** Teacher grades a student's work
```json
{
  "event_category": "ASSESSMENT",
  "event_type": "GRADE_ASSIGNMENT",
  "user_id": "GV001",
  "role": "teacher",
  "student_id": "SV001",
  "assignment_id": "ASM001",
  "course_id": "COURSE001",
  "timestamp": "2025-12-16 09:30:00",
  "score": 88
}
```

#### 2.8 ASSIGNMENT_REOPENED (Teacher) - Suggested
**Triggered when:** Teacher allows student to resubmit
```json
{
  "event_category": "ASSESSMENT",
  "event_type": "ASSIGNMENT_REOPENED",
  "user_id": "GV001",
  "role": "teacher",
  "student_id": "SV001",
  "assignment_id": "ASM001",
  "timestamp": "2025-12-17 10:00:00",
  "new_due_date": "2025-12-20 23:59:00"
}
```

---

## 3. Video Tracking Events (video_topic)

### Design Philosophy:
- **Raw Data Ingestion**: Store only raw watch duration per session
- **Aggregation Downstream**: Percentage and completion calculations happen in batch/stream processing layers
- **Cumulative Tracking**: Multiple watch sessions are summed to get total watch time

### Event Types:

#### 3.1 VIDEO_WATCHED (Student)
**Triggered when:** Student watches a video (records each viewing session)
```json
{
  "event_category": "VIDEO",
  "event_type": "VIDEO_WATCHED",
  "user_id": "SV001",
  "role": "student",
  "video_id": "VID001",
  "course_id": "COURSE001",
  "timestamp": "2025-12-10 10:25:00",
  "watch_duration_seconds": 120,
  "session_id": "video_session_uuid_123"
}
```

**Fields Explanation:**
- `watch_duration_seconds`: Raw duration watched in this specific session
- `session_id`: Unique identifier for this viewing session
- **No percentage calculation** - aggregation happens in batch/stream processing layers

**Example Scenario:**
```
Session 1: Student watches VID001 for 6 seconds
  → Event: {"video_id": "VID001", "watch_duration_seconds": 6}

Session 2: Student watches VID001 for 8 seconds  
  → Event: {"video_id": "VID001", "watch_duration_seconds": 8}

Batch Processing Layer:
  → Aggregates: Total watch time = 6 + 8 = 14 seconds
  → Calculates: Completion percentage using video metadata
  → Determines: is_completed based on 80% threshold
```

---

## 4. Course Interaction Events (course_topic)

### Event Types:

#### 4.1 COURSE_CREATED (Teacher)
**Triggered when:** Teacher creates a new course
```json
{
  "event_category": "COURSE",
  "event_type": "COURSE_CREATED",
  "user_id": "GV001",
  "role": "teacher",
  "course_id": "COURSE010",
  "timestamp": "2025-12-10 08:00:00",
  "course_name": "Advanced Data Analytics",
  "course_code": "CS401",
  "semester": "Fall 2025",
  "max_students": 50
}
```

#### 4.2 COURSE_DELETED (Teacher)
**Triggered when:** Teacher deletes a course
```json
{
  "event_category": "COURSE",
  "event_type": "COURSE_DELETED",
  "user_id": "GV001",
  "role": "teacher",
  "course_id": "COURSE009",
  "timestamp": "2025-12-10 09:00:00"
}
```

#### 4.4 COURSE_VIEW (Student)
**Triggered when:** Student views course homepage
```json
{
  "event_category": "COURSE",
  "event_type": "COURSE_VIEW",
  "user_id": "SV001",
  "role": "student",
  "course_id": "COURSE001",
  "timestamp": "2025-12-10 09:00:00"
}
```

#### 4.6 ITEM_CLICK (Student)
**Triggered when:** Student clicks on course item (lesson, module, etc.)
```json
{
  "event_category": "COURSE",
  "event_type": "ITEM_CLICK",
  "user_id": "SV001",
  "role": "student",
  "course_id": "COURSE001",
  "item_id": "ITEM025",
  "item_type": "lesson",
  "timestamp": "2025-12-10 10:30:00"
}
```

#### 4.7 DOWNLOAD_MATERIAL (Student)
**Triggered when:** Student downloads course materials
```json
{
  "event_category": "COURSE",
  "event_type": "DOWNLOAD_MATERIAL",
  "user_id": "SV001",
  "role": "student",
  "course_id": "COURSE001",
  "timestamp": "2025-12-10 11:00:00",
  "material_id": "MAT050"
}
```

#### 4.8 COURSE_ENROLLED (Student) - Suggested
**Triggered when:** Student enrolls in a course
```json
{
  "event_category": "COURSE",
  "event_type": "COURSE_ENROLLED",
  "user_id": "SV001",
  "role": "student",
  "course_id": "COURSE002",
  "timestamp": "2025-12-10 08:30:00"
}
```

#### 4.9 COURSE_DROPPED (Student) - Suggested
**Triggered when:** Student drops a course
```json
{
  "event_category": "COURSE",
  "event_type": "COURSE_DROPPED",
  "user_id": "SV001",
  "role": "student",
  "course_id": "COURSE003",
  "timestamp": "2025-12-10 12:00:00"
}
```

---

## 5. Profile Events (profile_topic)

### Event Types:

#### 5.1 CREATE_PROFILE (Student/Teacher)
**Triggered when:** New user creates their profile
```json
{
  "event_category": "PROFILE",
  "event_type": "CREATE_PROFILE",
  "user_id": "SV099",
  "role": "student",
  "timestamp": "2025-12-10 08:00:00",
  "email": "student099@university.edu",
  "full_name": "Nguyen Van A",
  "date_of_birth": "2003-05-15",
  "gender": "male",
  "phone_number": "+84912345678",
  "major": "Computer Science",
  "enrollment_year": 2021
}
```

#### 5.2 UPDATE_PROFILE (Student/Teacher)
**Triggered when:** User updates their profile information
```json
{
  "event_category": "PROFILE",
  "event_type": "UPDATE_PROFILE",
  "user_id": "SV001",
  "role": "student",
  "timestamp": "2025-12-10 14:00:00",
  "updated_fields": ["phone_number", "email"],
  "old_email": "old.email@university.edu",
  "new_email": "new.email@university.edu",
  "old_phone": "+84911111111",
  "new_phone": "+84922222222"
}
```

#### 5.3 PROFILE_PICTURE_UPDATED (Student/Teacher) - Suggested
**Triggered when:** User updates profile picture
```json
{
  "event_category": "PROFILE",
  "event_type": "PROFILE_PICTURE_UPDATED",
  "user_id": "SV001",
  "role": "student",
  "timestamp": "2025-12-10 15:00:00",
  "picture_url": "s3://profiles/SV001_avatar.jpg"
}
```

#### 5.4 PASSWORD_CHANGED (Student/Teacher) - Suggested
**Triggered when:** User changes password
```json
{
  "event_category": "PROFILE",
  "event_type": "PASSWORD_CHANGED",
  "user_id": "SV001",
  "role": "student",
  "timestamp": "2025-12-10 16:00:00",
  "change_reason": "user_initiated"
}
```

---

## 6. Notification Events (notification_topic)

**Purpose:** Track notification delivery and engagement

```json
{
  "event_category": "NOTIFICATION",
  "event_type": "NOTIFICATION_SENT",
  "user_id": "SV001",
  "notification_id": "NOTIF123",
  "timestamp": "2025-12-10 09:00:00",
  "notification_type": "assignment_reminder",
  "channel": "email",
  "related_object_id": "ASM001"
}
```

**Event Types:**
- NOTIFICATION_SENT
- NOTIFICATION_OPENED
- NOTIFICATION_CLICKED

---

## 7. Event Priority & Retention Policy

| Event Category | Priority | Retention Period | Use Case |
|----------------|----------|------------------|----------|
| AUTH | High | 2 years | Security audit, login patterns |
| ASSESSMENT | Critical | 5 years | Grade calculation, analytics |
| VIDEO | High | 2 years | Engagement analysis, completion tracking |
| COURSE | High | 3 years | Learning behavior, recommendations |
| PROFILE | Critical | Permanent | User management |
| NOTIFICATION | Low | 6 months | Delivery tracking |

---

## 8. Data Quality Standards

### Required Fields (All Events):
- `event_category`: String (enum)
- `event_type`: String (enum)
- `user_id`: String (format: SV### or GV###)
- `timestamp`: String (ISO 8601 format)

### Optional Common Fields:
- `role`: String (student/teacher)
- `course_id`: String
- `session_id`: UUID
- `device_type`: String (desktop/mobile/tablet)
- `ip_address`: String (IPv4/IPv6)

### Data Validation Rules:
1. Timestamps must be in UTC
2. User IDs must exist in user database
3. Course IDs must be valid
4. Scores must be between 0 and max_score
5. Duration fields must be non-negative

---

## 9. Analytics Use Cases

### Predictive Analytics:
- **Dropout Prediction**: Auth login frequency, video completion rates, assignment submission patterns
- **Grade Prediction**: Quiz scores, video watch completion, assignment scores, attendance
- **At-Risk Student Detection**: Declining video completion, missed deadlines, low quiz scores

### Descriptive Analytics:
- **Learning Behavior Patterns**: Peak learning hours, video completion trends
- **Course Effectiveness**: Video completion rates vs. assignment performance correlation
- **Teacher Performance**: Grading speed, student engagement in courses

### Prescriptive Analytics:
- **Personalized Recommendations**: Course suggestions based on video completion patterns
- **Intervention Triggers**: Alerts for students with low video completion rates
- **Resource Optimization**: Identify videos that need improvement (low completion rates)

### Video Completion Analysis:
- **Completion Threshold**: Default 80% - videos watched >= 80% are marked as completed
- **Engagement Metrics**: Track which videos have highest/lowest completion rates
- **Student Progress**: Monitor video completion as indicator of course progress
- **Early Warning**: Students with < 50% video completion rate may be at risk

---

## 10. Implementation Notes

### Kafka Topic Configuration:
```yaml
topics:
  auth_topic:
    partitions: 3
    replication_factor: 2
    retention_ms: 63072000000  # 2 years
  
  assessment_topic:
    partitions: 5
    replication_factor: 3
    retention_ms: 157680000000  # 5 years
  
  video_topic:
    partitions: 5
    replication_factor: 2
    retention_ms: 63072000000  # 2 years
    # Note: Raw watch duration only, aggregate in batch layer
  
  course_topic:
    partitions: 3
    replication_factor: 2
    retention_ms: 94608000000  # 3 years
  
  profile_topic:
    partitions: 2
    replication_factor: 3
    retention_ms: -1  # Infinite (compact)
    cleanup_policy: compact
  
  notification_topic:
    partitions: 2
    replication_factor: 2
    retention_ms: 15552000000  # 6 months
```

### Schema Evolution Strategy:
- Use schema registry (Confluent Schema Registry or AWS Glue)
- Version all schemas
- Maintain backward compatibility
- Add new fields as optional

### Video Watch Duration Processing:
**Raw Data (Ingestion Layer):**
```json
{"user_id": "SV001", "video_id": "VID001", "watch_duration_seconds": 6}
{"user_id": "SV001", "video_id": "VID001", "watch_duration_seconds": 8}
```

**Batch Layer Processing (PySpark Example):**
```python
# Aggregate total watch time per student per video
video_engagement = df_raw \
    .filter(col("event_category") == "VIDEO") \
    .groupBy("user_id", "video_id") \
    .agg(
        sum("watch_duration_seconds").alias("total_watch_seconds"),
        count("*").alias("watch_sessions")
    )

# Join with video metadata to calculate completion
video_completion = video_engagement \
    .join(video_catalog, "video_id") \
    .withColumn(
        "completion_percentage",
        (col("total_watch_seconds") / col("video_duration_seconds")) * 100
    ) \
    .withColumn(
        "is_completed",
        col("completion_percentage") >= 80.0
    )
```

---

**Version:** 1.0  
**Last Updated:** December 10, 2025  
**Maintainer:** BTLbigdata-group30
