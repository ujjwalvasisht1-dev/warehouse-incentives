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
                cohort INTEGER DEFAULT NULL,
                password_changed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
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
                cohort INTEGER DEFAULT NULL,
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
        execute_query(cursor, 'SELECT * FROM users WHERE picker_id = ?', (picker_id,))
        user = cursor.fetchone()
        conn.close()
        
        if user and check_password_hash(user['password'], password):
            session['user_id'] = user['picker_id']
            session['role'] = user['role']
            
            # Store cohort in session for pickers
            try:
                session['cohort'] = user['cohort'] if user['cohort'] else None
            except:
                session['cohort'] = None
            
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
            UPDATE users SET password = ?, password_changed = 1 WHERE picker_id = ?
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
        execute_query(cursor, 'SELECT password FROM users WHERE picker_id = ?', (session['user_id'],))
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
            UPDATE users SET password = ?, password_changed = 1 WHERE picker_id = ?
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
    
    return render_template('picker_dashboard.html', picker_id=picker_id, time_filter=time_filter, cohort=cohort)

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
    
    # Get cohort picker IDs if user has a cohort
    cohort_picker_ids = []
    if cohort:
        execute_query(cursor, 'SELECT picker_id FROM users WHERE cohort = ?', (cohort,))
        cohort_picker_ids = [row['picker_id'].lower() for row in cursor.fetchall()]
    
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
    
    # Build leaderboard with status colors
    leaderboard = []
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
        
        leaderboard.append({
            'rank': idx + 1,
            'picker_id': p['picker_id'],
            'items_picked': p['items_picked'],
            'items_lost': p['items_lost'],
            'unique_picklists': p['unique_picklists'],
            'score': p_score,
            'status_color': p_status,
            'is_current_user': is_current_user
        })
    
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
        'cohort': cohort
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
    
    # Get picker IDs for the selected cohort
    execute_query(cursor, 'SELECT picker_id FROM users WHERE cohort = ?', (cohort,))
    cohort_picker_ids = [row['picker_id'].lower() for row in cursor.fetchall()]
    
    if not cohort_picker_ids:
        conn.close()
        return jsonify({
            'rankings': [],
            'daily_avg': 0,
            'total_pickers': 0,
            'cohort': cohort
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
        
        rankings.append({
            'rank': idx + 1,
            'picker_id': picker['picker_id'],
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
        'cohort': cohort
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
        'details': [dict(row) for row in details]
    })

@app.route('/supervisor/download')
@supervisor_required
def supervisor_download():
    """Download CSV report - filtered by cohort"""
    time_filter = request.args.get('filter', 'today')
    cohort = request.args.get('cohort', '1')
    
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
    
    # Get picker IDs for the selected cohort
    execute_query(cursor, 'SELECT picker_id FROM users WHERE cohort = ?', (cohort,))
    cohort_picker_ids = [row['picker_id'].lower() for row in cursor.fetchall()]
    
    if cohort_picker_ids:
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
    else:
        # Empty result
        rows = []
        conn.close()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Rank', 'Picker ID', 'Picklists', 'Items Picked', 'Items Lost', 'Score'])
        output.seek(0)
        filename = f'cohort{cohort}_rankings_{time_filter}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        return send_file(
            io.BytesIO(output.getvalue().encode()),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
    
    rows = cursor.fetchall()
    conn.close()
    
    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Rank', 'Picker ID', 'Picklists', 'Items Picked', 'Items Lost', 'Score'])
    
    for idx, row in enumerate(rows, 1):
        writer.writerow([idx, row['picker_id'], row['unique_picklists'], row['items_picked'], row['items_lost'], row['score']])
    
    output.seek(0)
    filename = f'cohort{cohort}_rankings_{time_filter}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
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
        
        # Create picker users for all unique pickers (only if they don't exist)
        default_password = generate_password_hash('picker123')
        for picker_id in pickers_seen:
            try:
                if USE_POSTGRES:
                    cursor.execute('''
                        INSERT INTO users (picker_id, password, role, password_changed) 
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (picker_id) DO NOTHING
                    ''', (picker_id, default_password, 'picker', 0))
                else:
                    cursor.execute('INSERT OR IGNORE INTO users (picker_id, password, role, password_changed) VALUES (?, ?, ?, ?)',
                                  (picker_id, default_password, 'picker', 0))
                pickers_added += cursor.rowcount
            except:
                pass  # User already exists
        
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

@app.route('/admin/upload-cohorts', methods=['POST'])
@admin_required
def admin_upload_cohorts():
    """Handle cohort CSV upload"""
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

if __name__ == '__main__':
    init_db()
    port = int(os.environ.get('PORT', 5001))
    debug = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'
    app.run(debug=debug, host='0.0.0.0', port=port)
