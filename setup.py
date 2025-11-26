"""
Setup script to initialize database and create sample users
Run this once before starting the application
"""

import sqlite3
from werkzeug.security import generate_password_hash
from csv_processor import process_csv_file
import os

DATABASE = 'incentives.db'

def init_database():
    """Initialize database"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            picker_id TEXT UNIQUE,
            password TEXT,
            role TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Items table
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
    print("Database initialized")

def create_sample_users():
    """Create sample picker and supervisor users"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Sample pickers (using picker IDs from CSV)
    sample_pickers = [
        ('Ca.3817385', 'picker123'),
        ('ca.3833623', 'picker123'),
        ('ca.3704873', 'picker123'),
        ('ca.3550156', 'picker123'),
        ('ca.3835340', 'picker123'),
    ]
    
    for picker_id, password in sample_pickers:
        try:
            cursor.execute('''
                INSERT INTO users (picker_id, password, role)
                VALUES (?, ?, ?)
            ''', (picker_id, generate_password_hash(password), 'picker'))
            print(f"Created picker user: {picker_id}")
        except sqlite3.IntegrityError:
            print(f"Picker {picker_id} already exists")
    
    # Supervisor user
    try:
        cursor.execute('''
            INSERT INTO users (picker_id, password, role)
            VALUES (?, ?, ?)
        ''', ('supervisor', generate_password_hash('supervisor123'), 'supervisor'))
        print("Created supervisor user: supervisor")
    except sqlite3.IntegrityError:
        print("Supervisor user already exists")
    
    # Admin user (for data upload)
    import os
    admin_password = os.environ.get('ADMIN_PASSWORD', 'admin@warehouse2024')
    try:
        cursor.execute('''
            INSERT INTO users (picker_id, password, role)
            VALUES (?, ?, ?)
        ''', ('admin', generate_password_hash(admin_password), 'admin'))
        print("Created admin user: admin")
    except sqlite3.IntegrityError:
        print("Admin user already exists")
    
    conn.commit()
    conn.close()
    print("Sample users created")

def load_initial_csv():
    """Load the initial CSV file if it exists"""
    initial_csv = 'sqllab_picker_productivity_20251126T001629.csv'
    if os.path.exists(initial_csv):
        print(f"Loading initial CSV: {initial_csv}")
        try:
            rows = process_csv_file(initial_csv, initial_csv)
            print(f"Initial CSV loaded: {rows} rows inserted")
        except Exception as e:
            print(f"Error loading initial CSV: {e}")
    else:
        print("Initial CSV file not found. Skipping...")
        print("You can place CSV files in the 'csv_uploads' folder and run csv_processor.py")

if __name__ == '__main__':
    print("Setting up database...")
    init_database()
    create_sample_users()
    load_initial_csv()
    print("\nSetup complete!")
    print("\nSample Login Credentials:")
    print("=" * 50)
    print("Pickers:")
    print("  Picker ID: Ca.3817385, Password: picker123")
    print("  Picker ID: ca.3833623, Password: picker123")
    print("  Picker ID: ca.3704873, Password: picker123")
    print("\nSupervisor:")
    print("  Picker ID: supervisor, Password: supervisor123")
    print("\nAdmin (for CSV upload):")
    print("  Username: admin, Password: admin@warehouse2024")
    print("  Access at: /admin")
    print("=" * 50)
    print("\nTo start the application, run: python app.py")
    print("To process CSV files, run: python csv_processor.py")
    print("To set up auto-processing, add csv_processor.py to cron (every 10 minutes)")

