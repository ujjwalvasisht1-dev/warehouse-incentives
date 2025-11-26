from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import sqlite3
import os
import csv
import io
from functools import wraps

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-in-production'
app.config['DATABASE'] = 'incentives.db'
app.config['CSV_UPLOAD_FOLDER'] = 'csv_uploads'

# Ensure CSV upload folder exists
os.makedirs(app.config['CSV_UPLOAD_FOLDER'], exist_ok=True)

def get_db():
    """Get database connection"""
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize database with tables"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Users table (pickers and supervisors)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            picker_id TEXT UNIQUE,
            password TEXT,
            role TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Items table (stores all picking records)
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
    
    # Processed CSV files tracking
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
        cursor.execute('SELECT * FROM users WHERE picker_id = ?', (picker_id,))
        user = cursor.fetchone()
        conn.close()
        
        if user and check_password_hash(user['password'], password):
            session['user_id'] = user['picker_id']
            session['role'] = user['role']
            
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

@app.route('/picker/dashboard')
@login_required
def picker_dashboard():
    """Picker dashboard"""
    if session.get('role') == 'supervisor':
        return redirect(url_for('supervisor_dashboard'))
    
    picker_id = session['user_id']
    time_filter = request.args.get('filter', 'today')
    
    return render_template('picker_dashboard.html', picker_id=picker_id, time_filter=time_filter)

@app.route('/picker/api/stats')
@login_required
def picker_api_stats():
    """API endpoint for picker stats"""
    if session.get('role') == 'supervisor':
        return jsonify({'error': 'Unauthorized'}), 403
    
    picker_id = session['user_id']
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
    
    # Get picker stats (case-insensitive match)
    cursor.execute('''
        SELECT 
            COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
            COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
            COUNT(DISTINCT external_picklist_id) as unique_picklists
        FROM items
        WHERE LOWER(picker_id) = LOWER(?) AND updated_at >= ? AND updated_at <= ?
    ''', (picker_id, start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')))
    
    stats = cursor.fetchone()
    items_picked = stats['items_picked'] if stats else 0
    items_lost = stats['items_lost'] if stats else 0
    unique_picklists = stats['unique_picklists'] if stats else 0
    score = items_picked
    
    # Get all pickers' scores for ranking (with items picked, lost, and unique picklists)
    cursor.execute('''
        SELECT 
            picker_id,
            COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
            COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
            COUNT(DISTINCT external_picklist_id) as unique_picklists,
            COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
        FROM items
        WHERE updated_at >= ? AND updated_at <= ?
        GROUP BY LOWER(picker_id)
        ORDER BY score DESC
    ''', (start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')))
    
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
    
    # Calculate daily average for color coding (for all filters, not just today)
    cursor.execute('''
        SELECT AVG(score) as avg_score
        FROM (
            SELECT 
                LOWER(picker_id) as picker,
                COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
            FROM items
            WHERE updated_at >= ? AND updated_at <= ?
            GROUP BY LOWER(picker_id)
        )
    ''', (start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')))
    avg_result = cursor.fetchone()
    daily_avg = avg_result['avg_score'] if avg_result and avg_result['avg_score'] else 0
    
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
        'leaderboard': leaderboard
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
    """API endpoint for supervisor rankings"""
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
    
    # Format dates as strings for SQLite
    start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Get all pickers' stats
    cursor.execute('''
        SELECT 
            picker_id,
            COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
            COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
            COUNT(DISTINCT external_picklist_id) as unique_picklists
        FROM items
        WHERE updated_at >= ? AND updated_at <= ?
        GROUP BY LOWER(picker_id)
        ORDER BY items_picked DESC
    ''', (start_str, end_str))
    
    pickers = cursor.fetchall()
    
    # Calculate daily average (for all filters)
    cursor.execute('''
        SELECT AVG(score) as avg_score
        FROM (
            SELECT 
                LOWER(picker_id) as picker,
                COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
            FROM items
            WHERE updated_at >= ? AND updated_at <= ?
            GROUP BY LOWER(picker_id)
        )
    ''', (start_str, end_str))
    avg_result = cursor.fetchone()
    daily_avg = avg_result['avg_score'] if avg_result and avg_result['avg_score'] else 0
    
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
        'total_pickers': len(rankings)
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
    
    # Format dates as strings for SQLite
    start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Get picker details (case-insensitive)
    cursor.execute('''
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
    """Download CSV report"""
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
    
    # Format dates as strings for SQLite
    start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    cursor.execute('''
        SELECT 
            picker_id,
            COUNT(DISTINCT external_picklist_id) as unique_picklists,
            COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as items_picked,
            COUNT(CASE WHEN item_status = 'ITEM_NOT_FOUND' THEN 1 END) as items_lost,
            COUNT(CASE WHEN item_status IN ('COMPLETED', 'ITEM_REPLACED') THEN 1 END) as score
        FROM items
        WHERE updated_at >= ? AND updated_at <= ?
        GROUP BY LOWER(picker_id)
        ORDER BY score DESC
    ''', (start_str, end_str))
    
    rows = cursor.fetchall()
    conn.close()
    
    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Rank', 'Picker ID', 'Picklists', 'Items Picked', 'Items Lost', 'Score'])
    
    for idx, row in enumerate(rows, 1):
        writer.writerow([idx, row['picker_id'], row['unique_picklists'], row['items_picked'], row['items_lost'], row['score']])
    
    output.seek(0)
    filename = f'picker_rankings_{time_filter}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    return send_file(
        io.BytesIO(output.getvalue().encode()),
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='127.0.0.1', port=5001)

