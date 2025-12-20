import streamlit as st
import pandas as pd
import requests
import time
import plotly.express as px
import plotly.graph_objects as go
import os

# Configuration
API_URL = os.environ.get("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="University Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'page' not in st.session_state:
    st.session_state['page'] = 'Dashboard'
if 'selected_course' not in st.session_state:
    st.session_state['selected_course'] = None
if 'selected_student' not in st.session_state:
    st.session_state['selected_student'] = None

# Sidebar Navigation
st.sidebar.title("🎓 Navigation")
page = st.sidebar.radio(
    "Select View",
    ["Dashboard", "Courses", "Students"],
    index=["Dashboard", "Courses", "Students"].index(st.session_state['page'])
)
st.session_state['page'] = page

auto_refresh = st.sidebar.checkbox("Auto Refresh (5s)", value=False)

# ========== HELPER FUNCTIONS ==========

def get_summary():
    try:
        response = requests.get(f"{API_URL}/analytics/summary")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching summary: {e}")
    return {}

def get_recent_activity(hours=1):
    try:
        response = requests.get(f"{API_URL}/analytics/recent_activity?hours={hours}")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching recent activity: {e}")
    return {}

def get_student_engagement_distribution():
    try:
        response = requests.get(f"{API_URL}/analytics/student_engagement_distribution")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching student distribution: {e}")
    return {}

def get_dau(hours=6):
    try:
        response = requests.get(f"{API_URL}/analytics/dau?hours={hours}")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching DAU: {e}")
    return []

def get_course_popularity(limit=10):
    try:
        response = requests.get(f"{API_URL}/analytics/course_popularity?limit={limit}")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching Course Popularity: {e}")
    return []

# ========== PAGE: OVERVIEW DASHBOARD ==========

def show_overview_dashboard():
    st.title("🎓 University Learning Analytics Dashboard")
    st.markdown("Real-time oversight of university-wide activities")
    
    # ===== ROW 1: KEY METRICS =====
    st.subheader("📈 Key Metrics")
    summary = get_summary()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        current_users = summary.get('current_active_users', 0)
        st.metric("👥 Current Active Users", current_users)
        st.caption("🔴 Live")
    
    with col2:
        active_courses = summary.get('total_active_courses', 0)
        st.metric("📚 Active Courses (24h)", active_courses)
    
    with col3:
        total_students = summary.get('total_students', 0)
        st.metric("🎓 Total Students", total_students)
    
    with col4:
        speed_status = summary.get('speed_layer_status', 'unknown')
        status_emoji = "🟢" if speed_status == "healthy" else "🟡" if speed_status == "stale" else "🔴"
        st.metric("🔌 System Status", f"{status_emoji} {speed_status.title()}")
    
    st.markdown("---")
    
    # ===== ROW 2: ACTIVITY OVERVIEW =====
    st.subheader("📉 Activity Overview (Last 6 Hours)")
    col1, col2 = st.columns(2)
    
    # User Activity Trend (Speed layer only for performance)
    with col1:
        st.markdown("**👥 User Activity Trend**")
        dau_data = get_dau(hours=6)
        if dau_data:
            df_dau = pd.DataFrame(dau_data)
            # Filter to speed layer only for real-time trend
            df_speed = df_dau[df_dau['source'] == 'speed']
            if not df_speed.empty:
                df_speed = df_speed.sort_values('date').reset_index(drop=True)
                df_speed['date'] = pd.to_datetime(df_speed['date'])
                fig = px.line(df_speed, x="date", y="users", 
                             title="Active Users (Real-time)", markers=True)
                fig.update_layout(showlegend=False, height=300)
                st.plotly_chart(fig, use_container_width=True)
                
                # Show peak
                peak_users = df_speed['users'].max()
                st.caption(f"🔺 Peak: {peak_users} users")
            else:
                st.info("No real-time data available")
        else:
            st.info("No data available")
    
    # Top Engaged Courses
    with col2:
        st.markdown("**📚 Top 10 Engaged Courses**")
        course_data = get_course_popularity(limit=10)
        if course_data:
            df_course = pd.DataFrame(course_data).reset_index(drop=True)
            fig = px.bar(df_course, x="course_id", y="interactions", 
                        title="Course Interactions", color="interactions",
                        color_continuous_scale="Blues")
            fig.update_layout(showlegend=False, height=300)
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No course data available")
    
    st.markdown("---")
    
    # ===== ROW 3: ENGAGEMENT INSIGHTS =====
    st.subheader("🎯 Engagement Insights")
    col1, col2 = st.columns(2)
    
    # Content Consumption (Last Hour)
    with col1:
        st.markdown("**🎥 Content Consumption (Last Hour)**")
        activity = get_recent_activity(hours=1)
        
        if activity:
            consumption_data = pd.DataFrame([
                {"Type": "Videos Watched", "Count": activity.get('videos_watched', 0)},
                {"Type": "Materials Downloaded", "Count": activity.get('materials_downloaded', 0)}
            ])
            
            fig = px.bar(consumption_data, x="Type", y="Count",
                        title="Recent Activity", color="Type",
                        color_discrete_map={"Videos Watched": "#3b82f6", "Materials Downloaded": "#10b981"})
            fig.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No activity data available")
    
    # Student Engagement Distribution
    with col2:
        st.markdown("**👥 Student Engagement Distribution**")
        distribution = get_student_engagement_distribution()
        
        if distribution and sum(distribution.values()) > 0:
            dist_data = pd.DataFrame([
                {"Category": "Highly Active", "Students": distribution.get('highly_active', 0)},
                {"Category": "Moderately Active", "Students": distribution.get('moderately_active', 0)},
                {"Category": "Low Activity", "Students": distribution.get('low_activity', 0)},
                {"Category": "Inactive", "Students": distribution.get('inactive', 0)}
            ])
            
            fig = px.pie(dist_data, values="Students", names="Category",
                        title="Student Activity Levels",
                        color_discrete_sequence=["#10b981", "#3b82f6", "#f59e0b", "#ef4444"])
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No student data available")

# ========== PAGE: COURSES VIEW ==========

def show_courses_view():
    st.title("📚 Course Analytics")
    
    # Fetch courses
    try:
        response = requests.get(f"{API_URL}/analytics/courses")
        courses = response.json() if response.status_code == 200 else []
    except:
        courses = []
        st.error("Failed to load courses")
    
    if not courses:
        st.warning("No course data available. Run batch job first.")
        return
    
    # Course selection
    course_ids = [c['course_id'] for c in courses]
    
    # Use session state if navigating back
    if st.session_state['selected_course'] and st.session_state['selected_course'] in course_ids:
        default_idx = course_ids.index(st.session_state['selected_course'])
        st.session_state['selected_course'] = None  # Reset
    else:
        default_idx = 0
    
    selected_course = st.selectbox("Select Course", course_ids, index=default_idx)
    
    if selected_course:
        # Show course overview
        course_info = [c for c in courses if c['course_id'] == selected_course][0]
        
        col1, col2, col3 = st.columns(3)
        col1.metric("📊 Total Students", course_info.get('total_enrolled_students', 0))
        col2.metric("🎥 Videos Watched", course_info.get('total_videos_watched', 0))
        col3.metric("📥 Materials Downloaded", course_info.get('total_materials_downloaded', 0))
        
        st.markdown("---")
        
        # Show students in course
        st.subheader(f"Students in {selected_course}")
        try:
            response = requests.get(f"{API_URL}/analytics/course/{selected_course}/students")
            students = response.json() if response.status_code == 200 else []
        except:
            students = []
        
        if students:
            df_students = pd.DataFrame(students)
            st.dataframe(df_students, use_container_width=True)
            
            # Student selection
            student_ids = df_students['student_id'].tolist()
            selected_student = st.selectbox("View Student Details", ["Select..."] + student_ids)
            
            if selected_student != "Select...":
                st.markdown(f"### Performance: {selected_student} in {selected_course}")
                
                try:
                    response = requests.get(
                        f"{API_URL}/analytics/student/{selected_student}/course/{selected_course}"
                    )
                    perf = response.json() if response.status_code == 200 else {}
                except:
                    perf = {}
                
                if perf:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("🎥 Videos Watched", perf.get('videos_watched', 0))
                    col2.metric("⏱️ Watch Time (min)", round(perf.get('watch_time_minutes', 0), 1))
                    col3.metric("📥 Materials Downloaded", perf.get('materials_downloaded', 0))
                    
                    # Button to navigate to student profile
                    if st.button(f"👤 View Full Profile: {selected_student}"):
                        st.session_state['selected_student'] = selected_student
                        st.session_state['page'] = 'Students'
                        st.rerun()
        else:
            st.info("No students enrolled yet")

# ========== PAGE: STUDENTS VIEW ==========

def show_students_view():
    st.title("👥 Student Analytics")
    
    # Check if navigating from course view
    if st.session_state['selected_student']:
        default_student = st.session_state['selected_student']
        st.session_state['selected_student'] = None
    else:
        default_student = None
    
    # Fetch students
    try:
        response = requests.get(f"{API_URL}/analytics/students")
        students = response.json() if response.status_code == 200 else []
    except:
        students = []
        st.error("Failed to load students")
    
    if not students:
        st.warning("No student data available. Run batch job first.")
        return
    
    # Student selection
    student_ids = [s['student_id'] for s in students]
    
    if default_student and default_student in student_ids:
        default_idx = student_ids.index(default_student)
    else:
        default_idx = 0
    
    selected_student = st.selectbox("Select Student", student_ids, index=default_idx)
    
    if selected_student:
        # Show student overview
        student_info = [s for s in students if s['student_id'] == selected_student][0]
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("📚 Courses Enrolled", student_info.get('total_courses_enrolled', 0))
        col2.metric("🎥 Videos Watched", student_info.get('total_videos_watched', 0))
        col3.metric("📝 Assignments Submitted", student_info.get('total_assignments_submitted', 0))
        col4.metric("🔐 Total Logins", student_info.get('total_logins', 0))
        
        st.markdown("---")
        
        # Show courses enrolled
        st.subheader(f"Courses for {selected_student}")
        try:
            response = requests.get(f"{API_URL}/analytics/student/{selected_student}/courses")
            courses = response.json() if response.status_code == 200 else []
        except:
            courses = []
        
        if courses:
            df_courses = pd.DataFrame(courses).reset_index(drop=True)
            # Clean dataframe: replace NaN/None with empty string and ensure consistent types
            df_courses = df_courses.fillna(0)
            # Convert all numeric columns to proper types
            for col in df_courses.columns:
                if df_courses[col].dtype == 'object':
                    try:
                        df_courses[col] = pd.to_numeric(df_courses[col], errors='ignore')
                    except:
                        pass
            st.dataframe(df_courses, use_container_width=True)
            
            # Course selection
            course_ids = df_courses['course_id'].tolist()
            selected_course = st.selectbox("View Course Details", ["Select..."] + course_ids)
            
            if selected_course != "Select...":
                st.markdown(f"### Performance: {selected_student} in {selected_course}")
                
                try:
                    response = requests.get(
                        f"{API_URL}/analytics/student/{selected_student}/course/{selected_course}"
                    )
                    perf = response.json() if response.status_code == 200 else {}
                except:
                    perf = {}
                
                if perf:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("🎥 Videos Watched", perf.get('videos_watched', 0))
                    col2.metric("⏱️ Watch Time (min)", round(perf.get('watch_time_minutes', 0), 1))
                    col3.metric("📥 Materials Downloaded", perf.get('materials_downloaded', 0))
                    
                    # Button to navigate to course view
                    if st.button(f"📚 View Full Course Details: {selected_course}"):
                        st.session_state['selected_course'] = selected_course
                        st.session_state['page'] = 'Courses'
                        st.rerun()
        else:
            st.info("Student not enrolled in any courses yet")

# ========== MAIN ROUTING ==========

if page == "Dashboard":
    show_overview_dashboard()
elif page == "Courses":
    show_courses_view()
elif page == "Students":
    show_students_view()

# Auto-refresh logic
if auto_refresh:
    time.sleep(5)
    st.rerun()
