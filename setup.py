"""
Setup script to initialize database and create sample users
Works with both SQLite (local) and PostgreSQL (production)
Run this once before starting the application
"""

from werkzeug.security import generate_password_hash
import os
import sys

# Debug: Print all database-related env vars
print("🔍 Checking environment variables...")
for key in sorted(os.environ.keys()):
    if 'DATABASE' in key.upper() or 'PG' in key.upper() or 'POSTGRES' in key.upper():
        # Don't print full URL for security
        val = os.environ[key]
        if len(val) > 20:
            val = val[:20] + '...'
        print(f"   {key} = {val}")

# Database setup - PostgreSQL for production, SQLite for local dev
# Try multiple possible env var names
DATABASE_URL = (
    os.environ.get('DATABASE_URL') or 
    os.environ.get('DATABASE_PUBLIC_URL') or
    os.environ.get('POSTGRES_URL')
)

if DATABASE_URL:
    # PostgreSQL
    import psycopg2
    from psycopg2.extras import RealDictCursor
    USE_POSTGRES = True
    # Fix for postgres:// vs postgresql://
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    print(f"✅ Found DATABASE_URL, using PostgreSQL")
else:
    # SQLite (local development)
    import sqlite3
    USE_POSTGRES = False
    print("⚠️ No DATABASE_URL found, falling back to SQLite")

DATABASE = 'incentives.db'

def get_db():
    """Get database connection"""
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    else:
        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row
        return conn

def execute_query(cursor, query, params=None):
    """Execute query with proper parameter placeholder"""
    if USE_POSTGRES:
        query = query.replace('?', '%s')
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)

def init_database():
    """Initialize database"""
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
        
        # Add columns if they don't exist (for existing databases)
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
        # SQLite schema
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                picker_id TEXT UNIQUE,
                password TEXT,
                role TEXT,
                name TEXT,
                cohort INTEGER DEFAULT NULL,
                doj TEXT DEFAULT NULL,
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
            cursor.execute('ALTER TABLE users ADD COLUMN doj TEXT')
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
    print(f"✅ Database initialized ({'PostgreSQL' if USE_POSTGRES else 'SQLite'})")

def create_sample_users():
    """Create admin and supervisor users"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Supervisor user
    try:
        if USE_POSTGRES:
            cursor.execute('''
                INSERT INTO users (picker_id, password, role, password_changed)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (picker_id) DO NOTHING
            ''', ('supervisor', generate_password_hash('supervisor123'), 'supervisor', 0))
        else:
            cursor.execute('''
                INSERT OR IGNORE INTO users (picker_id, password, role, password_changed)
                VALUES (?, ?, ?, ?)
            ''', ('supervisor', generate_password_hash('supervisor123'), 'supervisor', 0))
        print("✅ Created supervisor user: supervisor")
    except Exception as e:
        print(f"  Supervisor user: {e}")
    
    # Admin user (for data upload)
    admin_password = os.environ.get('ADMIN_PASSWORD', 'admin@warehouse2024')
    try:
        if USE_POSTGRES:
            cursor.execute('''
                INSERT INTO users (picker_id, password, role, password_changed)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (picker_id) DO NOTHING
            ''', ('admin', generate_password_hash(admin_password), 'admin', 1))
        else:
            cursor.execute('''
                INSERT OR IGNORE INTO users (picker_id, password, role, password_changed)
                VALUES (?, ?, ?, ?)
            ''', ('admin', generate_password_hash(admin_password), 'admin', 1))
        print("✅ Created admin user: admin")
    except Exception as e:
        print(f"  Admin user: {e}")
    
    conn.commit()
    conn.close()
    print("✅ Sample users created")

if __name__ == '__main__':
    print("🚀 Setting up database...")
    print(f"   Mode: {'PostgreSQL (Production)' if USE_POSTGRES else 'SQLite (Local)'}")
    init_database()
    create_sample_users()
    print("\n✅ Setup complete!")
    print("\n📋 Login Credentials:")
    print("=" * 50)
    print("Supervisor:")
    print("  Picker ID: supervisor, Password: supervisor123")
    print("\nAdmin (for CSV upload):")
    print("  Username: admin, Password: admin@warehouse2024")
    print("  Access at: /admin")
    print("=" * 50)
    print("\n💡 Picker accounts are created automatically when you:")
    print("   1. Upload a data CSV via Admin Dashboard")
    print("   2. Upload a cohort CSV via Admin Dashboard")
    print("   Default picker password = their picker_id")
