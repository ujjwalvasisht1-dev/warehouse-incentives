from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import os
import csv
import io
from functools import wraps

# Database setup - PostgreSQL for production, SQLite for local dev
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL:
    # PostgreSQL (Render)
    import psycopg2
    from psycopg2.extras import RealDictCursor
    USE_POSTGRES = True
    # Fix for Render's postgres:// vs postgresql://
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
else:
    # SQLite (local development)
    import sqlite3
    USE_POSTGRES = False

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-in-production')
app.config['DATABASE'] = os.environ.get('DATABASE_PATH', 'incentives.db')
app.config['CSV_UPLOAD_FOLDER'] = 'csv_uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB max file size

# Ensure CSV upload folder exists
os.makedirs(app.config['CSV_UPLOAD_FOLDER'], exist_ok=True)

def get_db():
    """Get database connection"""
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    else:
        conn = sqlite3.connect(app.config['DATABASE'])
        conn.row_factory = sqlite3.Row
        return conn

def execute_query(cursor, query, params=None):
    """Execute query with proper parameter placeholder"""
    if USE_POSTGRES:
        # Convert ? to %s for PostgreSQL
        query = query.replace('?', '%s')
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)

def init_db():
    """Initialize database with tables"""
    conn = get_db()
    cursor = conn.cursor()
    
    if USE_POSTGRES:
        # PostgreSQL schema
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                picker_id TEXT UNIQUE,
                password TEXT,
                role TEXT,
                name TEXT,
                cohort INTEGER DEFAULT NULL,
                doj DATE DEFAULT NULL,
                password_changed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Add new columns if they don't exist (migration)
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS name TEXT")
            cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS doj DATE")
        except:
            pass
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS items (
                id SERIAL PRIMARY KEY,
                source_warehouse TEXT,
                picker_id TEXT,
                item_status TEXT,
                dispatch_by_date TIMESTAMP,
                external_picklist_id TEXT,
                location_bin_id TEXT,
                location_sequence TEXT,
                updated_at TIMESTAMP,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                csv_file TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_csvs (
                id SERIAL PRIMARY KEY,
                filename TEXT UNIQUE,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_picker_id ON items(picker_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_updated_at ON items(updated_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_picker_updated ON items(picker_id, updated_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_cohort ON users(cohort)')
        
    else:
        # SQLite schema (existing)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                picker_id TEXT UNIQUE,
                password TEXT,
                role TEXT,
                name TEXT,
                cohort INTEGER DEFAULT NULL,
                doj DATE DEFAULT NULL,
                password_changed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Add columns if they don't exist (for existing databases)
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN password_changed INTEGER DEFAULT 0')
        except:
            pass
        
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN cohort INTEGER DEFAULT NULL')
        except:
            pass
        
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN name TEXT')
        except:
            pass
        
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN doj DATE')
        except:
            pass
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_warehouse TEXT,
                picker_id TEXT,
                item_status TEXT,
                dispatch_by_date TIMESTAMP,
                external_picklist_id TEXT,
                location_bin_id TEXT,
                location_sequence TEXT,
                updated_at TIMESTAMP,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                csv_file TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_csvs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT UNIQUE,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    
    conn.commit()
    conn.close()

def calculate_age_in_days(doj):
    """Calculate the number of days since date of joining"""
    if not doj:
        return None
    try:
        if isinstance(doj, str):
            # Try different date formats
            for fmt in ['%Y-%m-%d', '%d-%b-%Y', '%d/%m/%Y']:
                try:
                    doj = datetime.strptime(doj, fmt).date()
                    break
                except ValueError:
                    continue
        today = datetime.now().date()
        return (today - doj).days
    except:
        return None

def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def supervisor_required(f):
    """Decorator to require supervisor role"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session or session.get('role') != 'supervisor':
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """Decorator to require admin role"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session or session.get('role') != 'admin':
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def index():
    """Redirect to login"""
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page"""
    if request.method == 'POST':
        picker_id = request.form.get('picker_id', '').strip()
        password = request.form.get('password', '')
        
        conn = get_db()
        cursor = conn.cursor()
        execute_query(cursor, 'SELECT * FROM users WHERE LOWER(picker_id) = LOWER(?)', (picker_id,))
        user = cursor.fetchone()
        conn.close()
        
        if user and check_password_hash(user['password'], password):
            session['user_id'] = user['picker_id']
            session['role'] = user['role']
            
            # Store additional user info in session
            try:
                session['cohort'] = user['cohort'] if user['cohort'] else None
                session['name'] = user['name'] if user.get('name') else None
                session['doj'] = str(user['doj']) if user.get('doj') else None
            except:
                session['cohort'] = None
                session['name'] = None
                session['doj'] = None
            
            # Check if password needs to be changed (first login)
            try:
                password_changed = user['password_changed'] if user['password_changed'] else 0
            except:
                password_changed = 0
            
            if not password_changed and user['role'] in ['picker', 'supervisor']:
                return redirect(url_for('change_password_first'))
            
            if user['role'] == 'supervisor':
                return redirect(url_for('supervisor_dashboard'))
            else:
                return redirect(url_for('picker_dashboard'))
        else:
            return render_template('login.html', error='Invalid credentials')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    """Logout"""
    session.clear()
    return redirect(url_for('login'))

@app.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password_first():
    """Change password on first login"""
    if request.method == 'POST':
        new_password = request.form.get('new_password', '')
        confirm_password = request.form.get('confirm_password', '')
        
        # Validation
        if len(new_password) < 6:
            return render_template('change_password.html', error='Password must be at least 6 characters')
        
        if new_password != confirm_password:
            return render_template('change_password.html', error='Passwords do not match')
        
        # Update password
        conn = get_db()
        cursor = conn.cursor()
        execute_query(cursor, '''
            UPDATE users SET password = ?, password_changed = 1 WHERE LOWER(picker_id) = LOWER(?)
        ''', (generate_password_hash(new_password), session['user_id']))
        conn.commit()
        conn.close()
        
        # Redirect to appropriate dashboard
        if session.get('role') == 'supervisor':
            return redirect(url_for('supervisor_dashboard'))
        else:
            return redirect(url_for('picker_dashboard'))
    
    return render_template('change_password.html')

@app.route('/settings/change-password', methods=['GET', 'POST'])
@login_required
def change_password_settings():
    """Change password from settings (anytime)"""
    if request.method == 'POST':
        current_password = request.form.get('current_password', '')
        new_password = request.form.get('new_password', '')
        confirm_password = request.form.get('confirm_password', '')
        
        # Verify current password
        conn = get_db()
        cursor = conn.cursor()
        execute_query(cursor, 'SELECT password FROM users WHERE LOWER(picker_id) = LOWER(?)', (session['user_id'],))
        user = cursor.fetchone()
        
        if not user or not check_password_hash(user['password'], current_password):
            conn.close()
            return render_template('change_password_settings.html', error='Current password is incorrect')
        
        # Validation
        if len(new_password) < 6:
            conn.close()
            return render_template('change_password_settings.html', error='New password must be at least 6 characters')
        
        if new_password != confirm_password:
            conn.close()
            return render_template('change_password_settings.html', error='Passwords do not match')
        
        # Update password
        execute_query(cursor, '''
            UPDATE users SET password = ?, password_changed = 1 WHERE LOWER(picker_id) = LOWER(?)
        ''', (generate_password_hash(new_password), session['user_id']))
        conn.commit()
        conn.close()
        
        return render_template('change_password_settings.html', success='Password changed successfully!')
    
    return render_template('change_password_settings.html')

@app.route('/picker/dashboard')
@login_required
def picker_dashboard():
    """Picker dashboard"""
    if session.get('role') == 'supervisor':
        return redirect(url_for('supervisor_dashboard'))
    
    picker_id = session['user_id']
    time_filter = request.args.get('filter', 'today')
    cohort = session.get('cohort')
    name = session.get('name')
    doj = session.get('doj')
    
    # Calculate age in system
    age_in_days = None
    if doj:
        age_in_days = calculate_age_in_days(doj)
    
    return render_template('picker_dashboard.html', 
                          picker_id=picker_id, 
                          time_filter=time_filter, 
                          cohort=cohort,
                          name=name,
                          doj=doj,
                          age_in_days=age_in_days)

@app.route('/picker/api/stats')
@login_required
def picker_api_stats():
    """API endpoint for picker stats - filtered by cohort"""
    if session.get('role') == 'supervisor':
        return jsonify({'error': 'Unauthorized'}), 403
    
    picker_id = session['user_id']
    cohort = session.get('cohort')
    time_filter = request.args.get('filter', 'today')
    
    # Calculate date range
    now = datetime.now()
    if time_filter == 'today':
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif time_filter == 'yesterday':
        yesterday = now - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif time_filter == 'this_week':
        # Monday to current day
        days_since_monday = now.weekday()
        start_date = (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif time_filter == 'last_week':
        # Last week (Monday to Sunday)
        days_since_monday = now.weekday()
        last_monday = now - timedelta(days=days_since_monday + 7)
        start_date = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
        last_sunday = last_monday + timedelta(days=6)
        end_date = last_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif time_filter == 'all_time':
        # All time - use a very old date
        start_date = datetime(2020, 1, 1)
        end_date = now
    else:
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    
    conn = get_db()
    cursor = conn.cursor()
    
    start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Get current user's info (name, doj)
    execute_query(cursor, 'SELECT name, doj, cohort FROM users WHERE LOWER(picker_id) = LOWER(?)', (picker_id,))
    user_info = cursor.fetchone()
    user_name = user_info['name'] if user_info and user_info.get('name') else None
    user_doj = user_info['doj'] if user_info and user_info.get('doj') else None
    user_age_in_days = calculate_age_in_days(user_doj)
    
    # Get picker stats (case-insensitive match)
    execute_query(cursor, '''
        SELECT 
            COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
            COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
            COUNT(DISTINCT external_picklist_id) as unique_picklists
        FROM items
        WHERE LOWER(picker_id) = LOWER(?) AND updated_at >= ? AND updated_at <= ?
    ''', (picker_id, start_str, end_str))
    
    stats = cursor.fetchone()
    items_picked = stats['items_picked'] if stats else 0
    items_lost = stats['items_lost'] if stats else 0
    unique_picklists = stats['unique_picklists'] if stats else 0
    score = items_picked
    
    # Get cohort picker IDs and their info if user has a cohort
    cohort_users = {}
    cohort_picker_ids = []
    if cohort:
        execute_query(cursor, 'SELECT picker_id, name, doj FROM users WHERE cohort = ?', (cohort,))
        for row in cursor.fetchall():
            pid = row['picker_id'].lower()
            cohort_picker_ids.append(pid)
            cohort_users[pid] = {
                'name': row['name'] if row.get('name') else None,
                'doj': row['doj'] if row.get('doj') else None,
                'age_in_days': calculate_age_in_days(row['doj']) if row.get('doj') else None
            }
    
    # Get all pickers' scores for ranking (with items picked, lost, and unique picklists)
    # Filter by cohort if the user belongs to a cohort
    if cohort and cohort_picker_ids:
        # For PostgreSQL, we need to use ANY instead of IN with array
        if USE_POSTGRES:
            query = '''
                SELECT 
                    picker_id,
                    COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
                    COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
                    COUNT(DISTINCT external_picklist_id) as unique_picklists,
                    COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
                FROM items
                WHERE updated_at >= %s AND updated_at <= %s AND LOWER(picker_id) = ANY(%s)
                GROUP BY LOWER(picker_id), picker_id
                ORDER BY score DESC
            '''
            cursor.execute(query, (start_str, end_str, cohort_picker_ids))
        else:
            placeholders = ','.join(['?' for _ in cohort_picker_ids])
            query = f'''
                SELECT 
                    picker_id,
                    COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
                    COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
                    COUNT(DISTINCT external_picklist_id) as unique_picklists,
                    COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
                FROM items
                WHERE updated_at >= ? AND updated_at <= ? AND LOWER(picker_id) IN ({placeholders})
                GROUP BY LOWER(picker_id)
                ORDER BY score DESC
            '''
            params = [start_str, end_str] + cohort_picker_ids
            cursor.execute(query, params)
    else:
        # No cohort - show all pickers (fallback for non-cohort users)
        execute_query(cursor, '''
            SELECT 
                picker_id,
                COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
                COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
                COUNT(DISTINCT external_picklist_id) as unique_picklists,
                COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
            FROM items
            WHERE updated_at >= ? AND updated_at <= ?
            GROUP BY LOWER(picker_id), picker_id
            ORDER BY score DESC
        ''', (start_str, end_str))
    
    all_pickers = cursor.fetchall()
    
    # Calculate rank
    rank = 0  # 0 means not ranked
    total_pickers = len(all_pickers)
    items_to_next_rank = 0
    difference_from_first = 0
    picker_found = False
    
    for idx, picker in enumerate(all_pickers):
        if picker['picker_id'].lower() == picker_id.lower():
            rank = idx + 1
            picker_found = True
            if idx > 0:
                items_to_next_rank = all_pickers[idx - 1]['score'] - score + 1
            if len(all_pickers) > 0:
                difference_from_first = all_pickers[0]['score'] - score
            break
    
    # If picker not found but has items, they should still be ranked
    # If picker has no items, they are not ranked
    if not picker_found and items_picked == 0:
        rank = 0  # Not ranked
        if len(all_pickers) > 0:
            difference_from_first = all_pickers[0]['score']
    
    # Calculate daily average for color coding (within cohort)
    if cohort and cohort_picker_ids:
        if USE_POSTGRES:
            avg_query = '''
                SELECT AVG(score) as avg_score
                FROM (
                    SELECT 
                        LOWER(picker_id) as picker,
                        COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
                    FROM items
                    WHERE updated_at >= %s AND updated_at <= %s AND LOWER(picker_id) = ANY(%s)
                    GROUP BY LOWER(picker_id)
                ) subq
            '''
            cursor.execute(avg_query, (start_str, end_str, cohort_picker_ids))
        else:
            placeholders = ','.join(['?' for _ in cohort_picker_ids])
            avg_query = f'''
                SELECT AVG(score) as avg_score
                FROM (
                    SELECT 
                        LOWER(picker_id) as picker,
                        COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
                    FROM items
                    WHERE updated_at >= ? AND updated_at <= ? AND LOWER(picker_id) IN ({placeholders})
                    GROUP BY LOWER(picker_id)
                )
            '''
            params = [start_str, end_str] + cohort_picker_ids
            cursor.execute(avg_query, params)
    else:
        execute_query(cursor, '''
            SELECT AVG(score) as avg_score
            FROM (
                SELECT 
                    LOWER(picker_id) as picker,
                    COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
                FROM items
                WHERE updated_at >= ? AND updated_at <= ?
                GROUP BY LOWER(picker_id)
            ) subq
        ''', (start_str, end_str))
    
    avg_result = cursor.fetchone()
    daily_avg = float(avg_result['avg_score']) if avg_result and avg_result['avg_score'] else 0
    
    conn.close()
    
    # Determine color status for current picker
    if daily_avg > 0:
        if score > daily_avg * 1.05:
            status_color = 'green'
        elif score >= daily_avg * 0.95:
            status_color = 'yellow'
        else:
            status_color = 'red'
    else:
        status_color = 'yellow'
    
    # Build leaderboard with status colors (limit to top 15)
    leaderboard = []
    current_user_entry = None
    
    for idx, p in enumerate(all_pickers):
        p_score = p['items_picked']
        if daily_avg > 0:
            if p_score > daily_avg * 1.05:
                p_status = 'green'
            elif p_score >= daily_avg * 0.95:
                p_status = 'yellow'
            else:
                p_status = 'red'
        else:
            p_status = 'yellow'
        
        is_current_user = p['picker_id'].lower() == picker_id.lower()
        
        # Get name and age from cohort_users
        picker_lower = p['picker_id'].lower()
        picker_name = cohort_users.get(picker_lower, {}).get('name')
        picker_age = cohort_users.get(picker_lower, {}).get('age_in_days')
        
        entry = {
            'rank': idx + 1,
            'picker_id': p['picker_id'],
            'name': picker_name,
            'age_in_days': picker_age,
            'items_picked': p['items_picked'],
            'items_lost': p['items_lost'],
            'unique_picklists': p['unique_picklists'],
            'score': p_score,
            'status_color': p_status,
            'is_current_user': is_current_user
        }
        
        # Add to top 15
        if idx < 15:
            leaderboard.append(entry)
        
        # Track current user if they're beyond top 15
        if is_current_user:
            current_user_entry = entry
    
    return jsonify({
        'items_picked': items_picked,
        'items_lost': items_lost,
        'unique_picklists': unique_picklists,
        'score': score,
        'rank': rank,
        'total_pickers': total_pickers,
        'items_to_next_rank': items_to_next_rank,
        'difference_from_first': difference_from_first,
        'daily_avg': round(daily_avg, 2),
        'status_color': status_color,
        'leaderboard': leaderboard,
        'current_user_entry': current_user_entry,
        'cohort': cohort,
        'user_name': user_name,
        'user_age_in_days': user_age_in_days
    })

@app.route('/supervisor/dashboard')
@supervisor_required
def supervisor_dashboard():
    """Supervisor dashboard"""
    time_filter = request.args.get('filter', 'today')
    return render_template('supervisor_dashboard.html', time_filter=time_filter)

@app.route('/supervisor/api/rankings')
@supervisor_required
def supervisor_api_rankings():
    """API endpoint for supervisor rankings - filtered by cohort"""
    time_filter = request.args.get('filter', 'today')
    cohort = request.args.get('cohort', '1')
    
    # Handle "all" cohorts view
    show_all = cohort == 'all'
    
    if not show_all:
        try:
            cohort = int(cohort)
        except ValueError:
            cohort = 1
    
    # Calculate date range
    now = datetime.now()
    if time_filter == 'today':
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif time_filter == 'yesterday':
        yesterday = now - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif time_filter == 'this_week':
        days_since_monday = now.weekday()
        start_date = (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif time_filter == 'last_week':
        days_since_monday = now.weekday()
        last_monday = now - timedelta(days=days_since_monday + 7)
        start_date = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
        last_sunday = last_monday + timedelta(days=6)
        end_date = last_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif time_filter == 'all_time':
        start_date = datetime(2020, 1, 1)
        end_date = now
    else:
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    
    conn = get_db()
    cursor = conn.cursor()
    
    start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Get all users with their info
    if show_all:
        execute_query(cursor, 'SELECT picker_id, name, doj, cohort FROM users WHERE role = ?', ('picker',))
    else:
        execute_query(cursor, 'SELECT picker_id, name, doj, cohort FROM users WHERE cohort = ?', (cohort,))
    
    users_data = {}
    cohort_picker_ids = []
    for row in cursor.fetchall():
        pid = row['picker_id'].lower()
        cohort_picker_ids.append(pid)
        users_data[pid] = {
            'name': row['name'] if row.get('name') else None,
            'doj': row['doj'] if row.get('doj') else None,
            'age_in_days': calculate_age_in_days(row['doj']) if row.get('doj') else None,
            'cohort': row['cohort'] if row.get('cohort') else None
        }
    
    if not cohort_picker_ids:
        conn.close()
        return jsonify({
            'rankings': [],
            'daily_avg': 0,
            'total_pickers': 0,
            'cohort': 'all' if show_all else cohort
        })
    
    # Get stats for cohort pickers only
    if USE_POSTGRES:
        query = '''
            SELECT 
                picker_id,
                COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
                COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
                COUNT(DISTINCT external_picklist_id) as unique_picklists
            FROM items
            WHERE updated_at >= %s AND updated_at <= %s AND LOWER(picker_id) = ANY(%s)
            GROUP BY LOWER(picker_id), picker_id
            ORDER BY items_picked DESC
        '''
        cursor.execute(query, (start_str, end_str, cohort_picker_ids))
    else:
        placeholders = ','.join(['?' for _ in cohort_picker_ids])
        query = f'''
            SELECT 
                picker_id,
                COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
                COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
                COUNT(DISTINCT external_picklist_id) as unique_picklists
            FROM items
            WHERE updated_at >= ? AND updated_at <= ? AND LOWER(picker_id) IN ({placeholders})
            GROUP BY LOWER(picker_id)
            ORDER BY items_picked DESC
        '''
        params = [start_str, end_str] + cohort_picker_ids
        cursor.execute(query, params)
    
    pickers = cursor.fetchall()
    
    # Calculate cohort average
    if USE_POSTGRES:
        avg_query = '''
            SELECT AVG(score) as avg_score
            FROM (
                SELECT 
                    LOWER(picker_id) as picker,
                    COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
                FROM items
                WHERE updated_at >= %s AND updated_at <= %s AND LOWER(picker_id) = ANY(%s)
                GROUP BY LOWER(picker_id)
            ) subq
        '''
        cursor.execute(avg_query, (start_str, end_str, cohort_picker_ids))
    else:
        placeholders = ','.join(['?' for _ in cohort_picker_ids])
        avg_query = f'''
            SELECT AVG(score) as avg_score
            FROM (
                SELECT 
                    LOWER(picker_id) as picker,
                    COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
                FROM items
                WHERE updated_at >= ? AND updated_at <= ? AND LOWER(picker_id) IN ({placeholders})
                GROUP BY LOWER(picker_id)
            )
        '''
        params = [start_str, end_str] + cohort_picker_ids
        cursor.execute(avg_query, params)
    
    avg_result = cursor.fetchone()
    daily_avg = float(avg_result['avg_score']) if avg_result and avg_result['avg_score'] else 0
    
    conn.close()
    
    # Format results
    rankings = []
    for idx, picker in enumerate(pickers):
        score = picker['items_picked']
        if daily_avg > 0:
            if score > daily_avg * 1.05:
                status_color = 'green'
            elif score >= daily_avg * 0.95:
                status_color = 'yellow'
            else:
                status_color = 'red'
        else:
            status_color = 'yellow'
        
        picker_lower = picker['picker_id'].lower()
        user_data = users_data.get(picker_lower, {})
        
        rankings.append({
            'rank': idx + 1,
            'picker_id': picker['picker_id'],
            'name': user_data.get('name'),
            'age_in_days': user_data.get('age_in_days'),
            'cohort': user_data.get('cohort'),
            'items_picked': picker['items_picked'],
            'items_lost': picker['items_lost'],
            'unique_picklists': picker['unique_picklists'],
            'score': score,
            'status_color': status_color
        })
    
    return jsonify({
        'rankings': rankings,
        'daily_avg': round(daily_avg, 2),
        'total_pickers': len(rankings),
        'cohort': 'all' if show_all else cohort
    })

@app.route('/supervisor/api/picker/<picker_id>')
@supervisor_required
def supervisor_api_picker_detail(picker_id):
    """API endpoint for picker detail"""
    time_filter = request.args.get('filter', 'today')
    
    # Calculate date range
    now = datetime.now()
    if time_filter == 'today':
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif time_filter == 'yesterday':
        yesterday = now - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif time_filter == 'this_week':
        days_since_monday = now.weekday()
        start_date = (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif time_filter == 'last_week':
        days_since_monday = now.weekday()
        last_monday = now - timedelta(days=days_since_monday + 7)
        start_date = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
        last_sunday = last_monday + timedelta(days=6)
        end_date = last_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif time_filter == 'all_time':
        start_date = datetime(2020, 1, 1)
        end_date = now
    else:
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    
    conn = get_db()
    cursor = conn.cursor()
    
    start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Get picker info
    execute_query(cursor, 'SELECT name, doj, cohort FROM users WHERE LOWER(picker_id) = LOWER(?)', (picker_id,))
    user_info = cursor.fetchone()
    
    # Get picker details (case-insensitive)
    execute_query(cursor, '''
        SELECT 
            external_picklist_id,
            location_bin_id,
            item_status,
            updated_at
        FROM items
        WHERE LOWER(picker_id) = LOWER(?) AND updated_at >= ? AND updated_at <= ?
        ORDER BY updated_at DESC
    ''', (picker_id, start_str, end_str))
    
    details = cursor.fetchall()
    conn.close()
    
    return jsonify({
        'picker_id': picker_id,
        'name': user_info['name'] if user_info and user_info.get('name') else None,
        'doj': str(user_info['doj']) if user_info and user_info.get('doj') else None,
        'cohort': user_info['cohort'] if user_info and user_info.get('cohort') else None,
        'age_in_days': calculate_age_in_days(user_info['doj']) if user_info and user_info.get('doj') else None,
        'details': [dict(row) for row in details]
    })

@app.route('/supervisor/download')
@supervisor_required
def supervisor_download():
    """Download CSV report - filtered by cohort"""
    time_filter = request.args.get('filter', 'today')
    cohort = request.args.get('cohort', '1')
    
    show_all = cohort == 'all'
    
    if not show_all:
        try:
            cohort = int(cohort)
        except ValueError:
            cohort = 1
    
    # Calculate date range
    now = datetime.now()
    if time_filter == 'today':
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif time_filter == 'yesterday':
        yesterday = now - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif time_filter == 'this_week':
        days_since_monday = now.weekday()
        start_date = (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif time_filter == 'last_week':
        days_since_monday = now.weekday()
        last_monday = now - timedelta(days=days_since_monday + 7)
        start_date = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
        last_sunday = last_monday + timedelta(days=6)
        end_date = last_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif time_filter == 'all_time':
        start_date = datetime(2020, 1, 1)
        end_date = now
    else:
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    
    conn = get_db()
    cursor = conn.cursor()
    
    start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Get user info
    if show_all:
        execute_query(cursor, 'SELECT picker_id, name, doj, cohort FROM users WHERE role = ?', ('picker',))
    else:
        execute_query(cursor, 'SELECT picker_id, name, doj, cohort FROM users WHERE cohort = ?', (cohort,))
    
    users_data = {}
    cohort_picker_ids = []
    for row in cursor.fetchall():
        pid = row['picker_id'].lower()
        cohort_picker_ids.append(pid)
        users_data[pid] = {
            'name': row['name'] if row.get('name') else '',
            'age_in_days': calculate_age_in_days(row['doj']) if row.get('doj') else '',
            'cohort': row['cohort'] if row.get('cohort') else ''
        }
    
    if not cohort_picker_ids:
        conn.close()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Rank', 'Picker ID', 'Name', 'Cohort', 'Age (Days)', 'Picklists', 'Items Picked', 'Items Lost', 'Score'])
        output.seek(0)
        filename = f'{"all" if show_all else f"cohort{cohort}"}_rankings_{time_filter}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        return send_file(
            io.BytesIO(output.getvalue().encode()),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
    
    if USE_POSTGRES:
        query = '''
            SELECT 
                picker_id,
                COUNT(DISTINCT external_picklist_id) as unique_picklists,
                COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
                COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
                COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
            FROM items
            WHERE updated_at >= %s AND updated_at <= %s AND LOWER(picker_id) = ANY(%s)
            GROUP BY LOWER(picker_id), picker_id
            ORDER BY score DESC
        '''
        cursor.execute(query, (start_str, end_str, cohort_picker_ids))
    else:
        placeholders = ','.join(['?' for _ in cohort_picker_ids])
        query = f'''
            SELECT 
                picker_id,
                COUNT(DISTINCT external_picklist_id) as unique_picklists,
                COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
                COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
                COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
            FROM items
            WHERE updated_at >= ? AND updated_at <= ? AND LOWER(picker_id) IN ({placeholders})
            GROUP BY LOWER(picker_id)
            ORDER BY score DESC
        '''
        params = [start_str, end_str] + cohort_picker_ids
        cursor.execute(query, params)
    
    rows = cursor.fetchall()
    conn.close()
    
    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Rank', 'Picker ID', 'Name', 'Cohort', 'Age (Days)', 'Picklists', 'Items Picked', 'Items Lost', 'Score'])
    
    for idx, row in enumerate(rows, 1):
        picker_lower = row['picker_id'].lower()
        user_data = users_data.get(picker_lower, {})
        writer.writerow([
            idx, 
            row['picker_id'], 
            user_data.get('name', ''),
            user_data.get('cohort', ''),
            user_data.get('age_in_days', ''),
            row['unique_picklists'], 
            row['items_picked'], 
            row['items_lost'], 
            row['score']
        ])
    
    output.seek(0)
    filename = f'{"all" if show_all else f"cohort{cohort}"}_rankings_{time_filter}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    return send_file(
        io.BytesIO(output.getvalue().encode()),
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

# ==================== ADMIN ROUTES ====================

@app.route('/admin')
def admin_index():
    """Redirect to admin login"""
    return redirect(url_for('admin_login'))

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    """Admin login page"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        
        conn = get_db()
        cursor = conn.cursor()
        execute_query(cursor, 'SELECT * FROM users WHERE picker_id = ? AND role = ?', (username, 'admin'))
        user = cursor.fetchone()
        conn.close()
        
        if user and check_password_hash(user['password'], password):
            session['user_id'] = user['picker_id']
            session['role'] = user['role']
            return redirect(url_for('admin_dashboard'))
        else:
            return render_template('admin_login.html', error='Invalid admin credentials')
    
    return render_template('admin_login.html')

@app.route('/admin/logout')
def admin_logout():
    """Admin logout"""
    session.clear()
    return redirect(url_for('admin_login'))

@app.route('/admin/dashboard')
@admin_required
def admin_dashboard():
    """Admin dashboard"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get stats
    execute_query(cursor, 'SELECT COUNT(*) as count FROM items')
    total_items = cursor.fetchone()['count']
    
    execute_query(cursor, 'SELECT COUNT(DISTINCT picker_id) as count FROM items')
    total_pickers = cursor.fetchone()['count']
    
    execute_query(cursor, "SELECT COUNT(*) as count FROM users WHERE role = 'picker'")
    registered_pickers = cursor.fetchone()['count']
    
    # Get cohort stats
    execute_query(cursor, 'SELECT COUNT(DISTINCT cohort) as count FROM users WHERE cohort IS NOT NULL')
    total_cohorts = cursor.fetchone()['count']
    
    execute_query(cursor, 'SELECT COUNT(*) as count FROM users WHERE cohort IS NOT NULL')
    pickers_in_cohorts = cursor.fetchone()['count']
    
    execute_query(cursor, 'SELECT filename, processed_at FROM processed_csvs ORDER BY processed_at DESC LIMIT 10')
    recent_uploads = cursor.fetchall()
    
    conn.close()
    
    return render_template('admin_dashboard.html', 
                          total_items=total_items,
                          total_pickers=total_pickers,
                          registered_pickers=registered_pickers,
                          total_cohorts=total_cohorts,
                          pickers_in_cohorts=pickers_in_cohorts,
                          recent_uploads=recent_uploads)

@app.route('/admin/upload', methods=['POST'])
@admin_required
def admin_upload():
    """Handle CSV upload with optimized batch processing"""
    if 'csv_file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['csv_file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be a CSV'}), 400
    
    try:
        # Read CSV content with multiple encoding attempts
        try:
            content = file.read().decode('utf-8')
        except UnicodeDecodeError:
            file.seek(0)
            content = file.read().decode('latin-1')
        
        # Use StringIO for proper CSV parsing
        csv_file = io.StringIO(content)
        reader = csv.DictReader(csv_file)
        
        conn = get_db()
        cursor = conn.cursor()
        
        # Collect data for batch insert
        items_batch = []
        pickers_seen = set()
        rows_inserted = 0
        pickers_added = 0
        
        BATCH_SIZE = 500  # Smaller batches for reliability
        
        # Import execute_values for faster PostgreSQL inserts
        if USE_POSTGRES:
            from psycopg2.extras import execute_values
        
        for row in reader:
            # Parse updated_at timestamp
            updated_at_str = row.get('updated_at', '').strip()
            if not updated_at_str:
                continue
            
            try:
                updated_at = datetime.strptime(updated_at_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    updated_at = datetime.strptime(updated_at_str, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    continue
            
            # Get picker_id
            picker_id = row.get('picker_ldap', '').strip()
            if not picker_id:
                continue
            
            # Add to batch
            items_batch.append((
                row.get('source_warehouse', ''),
                picker_id,
                row.get('item_status', ''),
                row.get('dispatch_by_date', ''),
                row.get('external_picklist_id', ''),
                row.get('location_bin_id', ''),
                row.get('location_sequence', ''),
                updated_at.strftime('%Y-%m-%d %H:%M:%S'),
                file.filename
            ))
            
            # Track unique pickers
            pickers_seen.add(picker_id)
            
            # Insert batch when full and COMMIT immediately
            if len(items_batch) >= BATCH_SIZE:
                if USE_POSTGRES:
                    execute_values(cursor, '''
                        INSERT INTO items (
                            source_warehouse, picker_id, item_status, dispatch_by_date,
                            external_picklist_id, location_bin_id, location_sequence,
                            updated_at, csv_file
                        ) VALUES %s
                    ''', items_batch)
                else:
                    cursor.executemany('''
                        INSERT INTO items (
                            source_warehouse, picker_id, item_status, dispatch_by_date,
                            external_picklist_id, location_bin_id, location_sequence,
                            updated_at, csv_file
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', items_batch)
                conn.commit()  # Commit after each batch
                rows_inserted += len(items_batch)
                items_batch = []
        
        # Insert remaining items
        if items_batch:
            if USE_POSTGRES:
                execute_values(cursor, '''
                    INSERT INTO items (
                        source_warehouse, picker_id, item_status, dispatch_by_date,
                        external_picklist_id, location_bin_id, location_sequence,
                        updated_at, csv_file
                    ) VALUES %s
                ''', items_batch)
            else:
                cursor.executemany('''
                    INSERT INTO items (
                        source_warehouse, picker_id, item_status, dispatch_by_date,
                        external_picklist_id, location_bin_id, location_sequence,
                        updated_at, csv_file
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', items_batch)
            conn.commit()
            rows_inserted += len(items_batch)
        
        # Record the upload
        if USE_POSTGRES:
            cursor.execute('''
                INSERT INTO processed_csvs (filename, processed_at) 
                VALUES (%s, %s)
                ON CONFLICT (filename) DO UPDATE SET processed_at = EXCLUDED.processed_at
            ''', (file.filename, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        else:
            cursor.execute('INSERT OR REPLACE INTO processed_csvs (filename, processed_at) VALUES (?, ?)',
                          (file.filename, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'rows_inserted': rows_inserted,
            'pickers_added': pickers_added,
            'filename': file.filename
        })
        
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

@app.route('/admin/clear-data', methods=['POST'])
@admin_required
def admin_clear_data():
    """Clear all item data (keeps users)"""
    conn = get_db()
    cursor = conn.cursor()
    execute_query(cursor, 'DELETE FROM items')
    execute_query(cursor, 'DELETE FROM processed_csvs')
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'message': 'All item data cleared'})

@app.route('/admin/clear-all', methods=['POST'])
@admin_required
def admin_clear_all():
    """Clear all data including users (except admin)"""
    conn = get_db()
    cursor = conn.cursor()
    execute_query(cursor, 'DELETE FROM items')
    execute_query(cursor, 'DELETE FROM processed_csvs')
    execute_query(cursor, "DELETE FROM users WHERE role != 'admin'")
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'message': 'All data cleared (except admin users)'})

@app.route('/admin/upload-pickers', methods=['POST'])
@admin_required
def admin_upload_pickers():
    """Handle picker CSV upload with name, cohort, and DOJ"""
    if 'picker_file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['picker_file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be a CSV'}), 400
    
    try:
        # Read CSV content
        try:
            content = file.read().decode('utf-8')
        except UnicodeDecodeError:
            file.seek(0)
            content = file.read().decode('latin-1')
        
        csv_file = io.StringIO(content)
        reader = csv.DictReader(csv_file)
        
        conn = get_db()
        cursor = conn.cursor()
        
        created = 0
        updated = 0
        
        for row in reader:
            # Get picker info - handle different column names
            picker_id = row.get('Casper ID', row.get('casper_id', row.get('picker_id', ''))).strip()
            name = row.get('Name', row.get('name', '')).strip()
            cohort = row.get('Cohort', row.get('cohort', '')).strip()
            doj_str = row.get('DOJ', row.get('doj', row.get('Date of Joining', ''))).strip()
            
            if not picker_id:
                continue
            
            # Parse cohort
            try:
                cohort_num = int(cohort) if cohort else None
            except ValueError:
                cohort_num = None
            
            # Parse DOJ
            doj = None
            if doj_str:
                for fmt in ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
                    try:
                        doj = datetime.strptime(doj_str, fmt).date()
                        break
                    except ValueError:
                        continue
            
            # Check if user exists (case-insensitive)
            execute_query(cursor, 'SELECT id FROM users WHERE LOWER(picker_id) = LOWER(?)', (picker_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing user
                if USE_POSTGRES:
                    cursor.execute('''
                        UPDATE users SET name = %s, cohort = %s, doj = %s, password = %s 
                        WHERE LOWER(picker_id) = LOWER(%s)
                    ''', (name, cohort_num, doj, generate_password_hash(picker_id), picker_id))
                else:
                    cursor.execute('''
                        UPDATE users SET name = ?, cohort = ?, doj = ?, password = ? 
                        WHERE LOWER(picker_id) = LOWER(?)
                    ''', (name, cohort_num, doj, generate_password_hash(picker_id), picker_id))
                updated += 1
            else:
                # Create new user with password = picker_id
                execute_query(cursor, '''
                    INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                    VALUES (?, ?, ?, ?, ?, ?, 0)
                ''', (picker_id, generate_password_hash(picker_id), 'picker', name, cohort_num, doj))
                created += 1
        
        conn.commit()
        
        # Get cohort summary
        execute_query(cursor, '''
            SELECT cohort, COUNT(*) as count 
            FROM users 
            WHERE cohort IS NOT NULL 
            GROUP BY cohort 
            ORDER BY cohort
        ''')
        cohort_summary = {row['cohort']: row['count'] for row in cursor.fetchall()}
        
        conn.close()
        
        return jsonify({
            'success': True,
            'total_pickers': created + updated,
            'created': created,
            'updated': updated,
            'cohort_summary': cohort_summary
        })
        
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

@app.route('/admin/upload-cohorts', methods=['POST'])
@admin_required
def admin_upload_cohorts():
    """Handle cohort CSV upload (legacy format with columns as cohorts)"""
    if 'cohort_file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['cohort_file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be a CSV'}), 400
    
    try:
        # Read CSV content
        try:
            content = file.read().decode('utf-8')
        except UnicodeDecodeError:
            file.seek(0)
            content = file.read().decode('latin-1')
        
        csv_file = io.StringIO(content)
        reader = csv.reader(csv_file)
        header = next(reader)  # Get header row (Cohort 1, Cohort 2, ...)
        
        # Create a mapping: column index -> cohort number
        cohort_map = {}
        for idx, col_name in enumerate(header):
            if col_name.strip().lower().startswith('cohort'):
                try:
                    cohort_num = int(col_name.strip().split()[-1])
                    cohort_map[idx] = cohort_num
                except ValueError:
                    continue
        
        # Read all rows and build picker-cohort mapping
        picker_cohorts = {}
        for row in reader:
            for idx, cohort_num in cohort_map.items():
                if idx < len(row):
                    picker_id = row[idx].strip()
                    if picker_id:  # Not empty
                        picker_cohorts[picker_id] = cohort_num
        
        conn = get_db()
        cursor = conn.cursor()
        
        # Create/update users with cohort assignments
        # Password = picker_id (same as username)
        created = 0
        updated = 0
        
        for picker_id, cohort_num in picker_cohorts.items():
            # Check if user exists (case-insensitive)
            execute_query(cursor, 'SELECT id, cohort FROM users WHERE LOWER(picker_id) = LOWER(?)', (picker_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Update cohort and password (password = picker_id)
                execute_query(cursor, 'UPDATE users SET cohort = ?, password = ? WHERE LOWER(picker_id) = LOWER(?)', 
                             (cohort_num, generate_password_hash(picker_id), picker_id))
                updated += 1
            else:
                # Create new user with password = picker_id
                execute_query(cursor, '''
                    INSERT INTO users (picker_id, password, role, cohort, password_changed)
                    VALUES (?, ?, ?, ?, 0)
                ''', (picker_id, generate_password_hash(picker_id), 'picker', cohort_num))
                created += 1
        
        conn.commit()
        
        # Get cohort summary
        execute_query(cursor, '''
            SELECT cohort, COUNT(*) as count 
            FROM users 
            WHERE cohort IS NOT NULL 
            GROUP BY cohort 
            ORDER BY cohort
        ''')
        cohort_summary = {row['cohort']: row['count'] for row in cursor.fetchall()}
        
        conn.close()
        
        return jsonify({
            'success': True,
            'total_pickers': len(picker_cohorts),
            'created': created,
            'updated': updated,
            'cohorts': len(cohort_map),
            'cohort_summary': cohort_summary
        })
        
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

# Fast batch load using pre-computed hashes
@app.route('/debug/fast-load')
def fast_load():
    """Fast load pickers using pre-computed password hashes - call repeatedly until done"""
    import json
    
    PICKERS_JSON = 'data_to_upload/pickers_with_hashes.json'
    BATCH_SIZE = 100  # Larger batch since no hash computation needed
    
    if not os.path.exists(PICKERS_JSON):
        return jsonify({'error': 'Pre-computed hashes file not found'}), 404
    
    try:
        # Load pre-computed data
        with open(PICKERS_JSON, 'r') as f:
            all_pickers = json.load(f)
        
        conn = get_db()
        cursor = conn.cursor()
        
        # Get existing picker IDs
        if USE_POSTGRES:
            cursor.execute("SELECT LOWER(picker_id) FROM users WHERE role = 'picker'")
        else:
            execute_query(cursor, "SELECT LOWER(picker_id) FROM users WHERE role = 'picker'")
        existing = set(row[0] for row in cursor.fetchall())
        
        # Find pickers not yet in DB
        to_insert = []
        for p in all_pickers:
            if p['picker_id'].lower() not in existing:
                to_insert.append(p)
                if len(to_insert) >= BATCH_SIZE:
                    break
        
        # Insert batch
        created = 0
        for p in to_insert:
            try:
                if USE_POSTGRES:
                    cursor.execute("""
                        INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                        VALUES (%s, %s, %s, %s, %s, %s, 0)
                    """, (p['picker_id'], p['password'], 'picker', p['name'], p['cohort'], p['doj']))
                else:
                    execute_query(cursor, """
                        INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                        VALUES (?, ?, ?, ?, ?, ?, 0)
                    """, (p['picker_id'], p['password'], 'picker', p['name'], p['cohort'], p['doj']))
                created += 1
            except Exception as e:
                pass  # Skip duplicates
        
        conn.commit()
        
        # Get total count
        if USE_POSTGRES:
            cursor.execute("SELECT COUNT(*) FROM users WHERE role = 'picker'")
        else:
            execute_query(cursor, "SELECT COUNT(*) FROM users WHERE role = 'picker'")
        total = cursor.fetchone()[0]
        
        conn.close()
        
        remaining = len(all_pickers) - total
        done = remaining <= 0
        
        return jsonify({
            'created_this_batch': created,
            'total_in_db': total,
            'total_expected': len(all_pickers),
            'remaining': max(0, remaining),
            'done': done,
            'message': '✅ ALL DONE! You can now login with any picker.' if done else f'Created {created}. Refresh to load more. {remaining} remaining.'
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

# Load pickers in batches (call multiple times until done) - OLD VERSION
@app.route('/debug/load-batch')
def load_batch():
    """Load pickers in batches of 30 - call repeatedly until all loaded"""
    import csv as csv_module
    from datetime import datetime as dt
    
    PICKERS_FILE = 'data_to_upload/pickers.csv'
    BATCH_SIZE = 30
    
    def parse_date(date_str):
        if not date_str:
            return None
        formats = ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
        for fmt in formats:
            try:
                return dt.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None
    
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Get list of existing picker_ids
        if USE_POSTGRES:
            cursor.execute("SELECT LOWER(picker_id) as pid FROM users WHERE role = 'picker'")
        else:
            execute_query(cursor, "SELECT LOWER(picker_id) as pid FROM users WHERE role = 'picker'")
        existing = set(row['pid'] for row in cursor.fetchall())
        
        # Read CSV and find pickers not yet in database
        created = 0
        skipped = 0
        total_in_csv = 0
        
        with open(PICKERS_FILE, 'r', encoding='utf-8') as f:
            reader = csv_module.DictReader(f)
            
            for row in reader:
                total_in_csv += 1
                picker_id = row.get('Casper ID', '').strip()
                
                if not picker_id:
                    continue
                
                # Skip if already exists
                if picker_id.lower() in existing:
                    skipped += 1
                    continue
                
                # Stop after BATCH_SIZE new inserts
                if created >= BATCH_SIZE:
                    break
                
                name = row.get('Name', '').strip()
                cohort_str = row.get('Cohort', '').strip()
                doj_str = row.get('DOJ', '').strip()
                
                try:
                    cohort = int(cohort_str) if cohort_str else None
                except:
                    cohort = None
                
                doj = parse_date(doj_str)
                password_hash = generate_password_hash(picker_id)
                
                if USE_POSTGRES:
                    cursor.execute('''
                        INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                        VALUES (%s, %s, %s, %s, %s, %s, 0)
                    ''', (picker_id, password_hash, 'picker', name, cohort, doj))
                else:
                    execute_query(cursor, '''
                        INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                        VALUES (?, ?, ?, ?, ?, ?, 0)
                    ''', (picker_id, password_hash, 'picker', name, cohort, str(doj) if doj else None))
                
                created += 1
                existing.add(picker_id.lower())
        
        conn.commit()
        
        # Get total count
        if USE_POSTGRES:
            cursor.execute("SELECT COUNT(*) as count FROM users WHERE role = 'picker'")
        else:
            execute_query(cursor, "SELECT COUNT(*) as count FROM users WHERE role = 'picker'")
        total_pickers = cursor.fetchone()['count']
        
        conn.close()
        
        remaining = total_in_csv - total_pickers
        done = remaining <= 0
        
        return jsonify({
            'created_this_batch': created,
            'total_pickers_in_db': total_pickers,
            'total_in_csv': total_in_csv,
            'remaining': max(0, remaining),
            'done': done,
            'message': 'ALL DONE! You can now login.' if done else f'Created {created}. Call this URL again to load more. {remaining} remaining.'
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

# Check if CSV file exists
@app.route('/debug/check-csv')
def debug_check_csv():
    """Check if the pickers CSV file exists"""
    PICKERS_FILE = 'data_to_upload/pickers.csv'
    result = {
        'cwd': os.getcwd(),
        'file_path': PICKERS_FILE,
        'file_exists': os.path.exists(PICKERS_FILE),
        'data_to_upload_exists': os.path.exists('data_to_upload'),
    }
    if os.path.exists('data_to_upload'):
        result['data_to_upload_contents'] = os.listdir('data_to_upload')
    if os.path.exists(PICKERS_FILE):
        with open(PICKERS_FILE, 'r') as f:
            lines = f.readlines()
            result['file_lines'] = len(lines)
            result['first_5_lines'] = lines[:5]
    return jsonify(result)

# Force load pickers from CSV file
@app.route('/debug/force-load-pickers')
def force_load_pickers():
    """Force load all pickers from CSV - run this once to fix the database"""
    import csv as csv_module
    from datetime import datetime as dt
    
    PICKERS_FILE = 'data_to_upload/pickers.csv'
    
    def parse_date(date_str):
        if not date_str:
            return None
        formats = ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
        for fmt in formats:
            try:
                return dt.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None
    
    # Check if file exists first
    if not os.path.exists(PICKERS_FILE):
        return jsonify({
            'error': f'File not found: {PICKERS_FILE}',
            'cwd': os.getcwd(),
            'data_to_upload_exists': os.path.exists('data_to_upload'),
            'files_in_cwd': os.listdir('.')[:20]
        }), 404
    
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Step 1: Delete ALL existing pickers
        if USE_POSTGRES:
            cursor.execute("DELETE FROM users WHERE role = 'picker'")
            deleted = cursor.rowcount
        else:
            execute_query(cursor, "DELETE FROM users WHERE role = 'picker'")
            deleted = cursor.rowcount
        conn.commit()
        
        # Step 2: Load pickers from CSV in batches
        created = 0
        errors = []
        
        with open(PICKERS_FILE, 'r', encoding='utf-8') as f:
            reader = csv_module.DictReader(f)
            
            batch = []
            for row in reader:
                picker_id = row.get('Casper ID', '').strip()
                name = row.get('Name', '').strip()
                cohort_str = row.get('Cohort', '').strip()
                doj_str = row.get('DOJ', '').strip()
                
                if not picker_id:
                    continue
                
                try:
                    cohort = int(cohort_str) if cohort_str else None
                except:
                    cohort = None
                
                doj = parse_date(doj_str)
                password_hash = generate_password_hash(picker_id)
                
                batch.append((picker_id, password_hash, 'picker', name, cohort, doj))
                
                # Insert in batches of 50
                if len(batch) >= 50:
                    for item in batch:
                        try:
                            if USE_POSTGRES:
                                cursor.execute('''
                                    INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                                    VALUES (%s, %s, %s, %s, %s, %s, 0)
                                ''', item)
                            else:
                                execute_query(cursor, '''
                                    INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                                    VALUES (?, ?, ?, ?, ?, ?, 0)
                                ''', (item[0], item[1], item[2], item[3], item[4], str(item[5]) if item[5] else None))
                            created += 1
                        except Exception as e:
                            errors.append(f"{item[0]}: {str(e)}")
                    conn.commit()
                    batch = []
            
            # Insert remaining
            for item in batch:
                try:
                    if USE_POSTGRES:
                        cursor.execute('''
                            INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                            VALUES (%s, %s, %s, %s, %s, %s, 0)
                        ''', item)
                    else:
                        execute_query(cursor, '''
                            INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                            VALUES (?, ?, ?, ?, ?, ?, 0)
                        ''', (item[0], item[1], item[2], item[3], item[4], str(item[5]) if item[5] else None))
                    created += 1
                except Exception as e:
                    errors.append(f"{item[0]}: {str(e)}")
            conn.commit()
        
        conn.close()
        
        return jsonify({
            'success': True,
            'deleted': deleted,
            'created': created,
            'errors': errors[:10] if errors else [],
            'message': f'Loaded {created} pickers! Login with picker_id as both username and password (e.g., ca.3867958 / ca.3867958)'
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

# Check specific picker
@app.route('/debug/check-picker/<picker_id>')
def debug_check_specific_picker(picker_id):
    """Check if a specific picker exists and test password"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        execute_query(cursor, "SELECT picker_id, password, name, cohort FROM users WHERE LOWER(picker_id) = LOWER(?)", (picker_id,))
        user = cursor.fetchone()
        
        if not user:
            # Try to find similar
            execute_query(cursor, "SELECT picker_id FROM users WHERE picker_id LIKE ?", (f'%{picker_id[-7:]}%',))
            similar = cursor.fetchall()
            conn.close()
            return jsonify({
                'found': False,
                'picker_id': picker_id,
                'similar_pickers': [s['picker_id'] for s in similar][:10]
            })
        
        # Test password
        password_works = check_password_hash(user['password'], picker_id)
        password_works_lower = check_password_hash(user['password'], picker_id.lower())
        
        picker_id_val = user['picker_id']
        conn.close()
        return jsonify({
            'found': True,
            'picker_id': picker_id_val,
            'name': user['name'],
            'cohort': user['cohort'],
            'password_test': password_works,
            'password_test_lowercase': password_works_lower,
            'message': 'Login with: ' + picker_id_val + ' / ' + picker_id_val
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

# Diagnostic endpoint to debug login issues
@app.route('/debug/check-pickers')
def debug_check_pickers():
    """Debug endpoint to check picker data in database"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Count all users
        execute_query(cursor, "SELECT role, COUNT(*) as count FROM users GROUP BY role")
        role_counts = cursor.fetchall()
        
        # Get sample pickers
        execute_query(cursor, "SELECT picker_id, name, cohort FROM users WHERE role = 'picker' LIMIT 10")
        sample_pickers = cursor.fetchall()
        
        # Test specific picker
        test_picker_id = 'ca.3867958'
        execute_query(cursor, "SELECT picker_id, password, name, cohort FROM users WHERE LOWER(picker_id) = LOWER(?)", (test_picker_id,))
        test_picker = cursor.fetchone()
        
        password_test = None
        if test_picker:
            # Test password verification
            password_test = {
                'picker_id': test_picker['picker_id'],
                'name': test_picker['name'],
                'password_hash_length': len(test_picker['password']) if test_picker['password'] else 0,
                'test_lowercase': check_password_hash(test_picker['password'], 'ca.3867958'),
                'test_uppercase': check_password_hash(test_picker['password'], 'Ca.3867958'),
                'test_picker123': check_password_hash(test_picker['password'], 'picker123'),
            }
        
        conn.close()
        
        return jsonify({
            'database_type': 'PostgreSQL' if USE_POSTGRES else 'SQLite',
            'role_counts': [dict(r) for r in role_counts],
            'sample_pickers': [dict(p) for p in sample_pickers],
            'test_picker': password_test,
            'message': 'If test_lowercase is True, login with ca.3867958 / ca.3867958'
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

if __name__ == '__main__':
    init_db()
    port = int(os.environ.get('PORT', 5001))
    debug = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'
    app.run(debug=debug, host='0.0.0.0', port=port)
