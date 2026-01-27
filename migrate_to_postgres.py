#!/usr/bin/env python3
"""
Script to migrate data from local SQLite database to PostgreSQL on Render.

Usage:
    1. Get the PostgreSQL connection string from Render dashboard
    2. Run: python migrate_to_postgres.py "postgresql://user:pass@host/dbname"
"""

import sys
import sqlite3
import os

# Check if psycopg2 is available
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("❌ psycopg2 not installed. Run: pip3 install psycopg2-binary")
    sys.exit(1)

def migrate_data(postgres_url):
    """Migrate all data from SQLite to PostgreSQL"""
    
    # Connect to SQLite
    sqlite_db = 'incentives.db'
    if not os.path.exists(sqlite_db):
        print(f"❌ SQLite database '{sqlite_db}' not found")
        sys.exit(1)
    
    print(f"📂 Connecting to SQLite: {sqlite_db}")
    sqlite_conn = sqlite3.connect(sqlite_db)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cursor = sqlite_conn.cursor()
    
    # Connect to PostgreSQL
    print(f"🐘 Connecting to PostgreSQL...")
    # Fix postgres:// vs postgresql://
    if postgres_url.startswith('postgres://'):
        postgres_url = postgres_url.replace('postgres://', 'postgresql://', 1)
    
    pg_conn = psycopg2.connect(postgres_url)
    pg_cursor = pg_conn.cursor()
    
    # Create tables in PostgreSQL
    print("📋 Creating tables...")
    
    pg_cursor.execute('''
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
    
    pg_cursor.execute('''
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
    
    pg_cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_csvs (
            id SERIAL PRIMARY KEY,
            filename TEXT UNIQUE,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create indexes
    pg_cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_picker_id ON items(picker_id)')
    pg_cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_updated_at ON items(updated_at)')
    pg_cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_picker_updated ON items(picker_id, updated_at)')
    pg_cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_cohort ON users(cohort)')
    
    pg_conn.commit()
    
    # Migrate users
    print("👥 Migrating users...")
    sqlite_cursor.execute('SELECT picker_id, password, role, cohort, password_changed, created_at FROM users')
    users = sqlite_cursor.fetchall()
    
    if users:
        # Clear existing users first (optional - comment out to keep existing)
        pg_cursor.execute('DELETE FROM users')
        
        for user in users:
            try:
                pg_cursor.execute('''
                    INSERT INTO users (picker_id, password, role, cohort, password_changed, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (picker_id) DO UPDATE SET
                        password = EXCLUDED.password,
                        role = EXCLUDED.role,
                        cohort = EXCLUDED.cohort,
                        password_changed = EXCLUDED.password_changed
                ''', (user['picker_id'], user['password'], user['role'], 
                      user['cohort'], user['password_changed'], user['created_at']))
            except Exception as e:
                print(f"  ⚠️ Error migrating user {user['picker_id']}: {e}")
        
        pg_conn.commit()
        print(f"  ✅ Migrated {len(users)} users")
    else:
        print("  ⚠️ No users to migrate")
    
    # Migrate items (in batches)
    print("📦 Migrating items...")
    sqlite_cursor.execute('SELECT COUNT(*) FROM items')
    total_items = sqlite_cursor.fetchone()[0]
    print(f"  📊 Total items to migrate: {total_items:,}")
    
    # Clear existing items
    pg_cursor.execute('DELETE FROM items')
    pg_conn.commit()
    
    BATCH_SIZE = 5000
    offset = 0
    migrated = 0
    
    while True:
        sqlite_cursor.execute(f'''
            SELECT source_warehouse, picker_id, item_status, dispatch_by_date,
                   external_picklist_id, location_bin_id, location_sequence,
                   updated_at, processed_at, csv_file
            FROM items
            LIMIT {BATCH_SIZE} OFFSET {offset}
        ''')
        items = sqlite_cursor.fetchall()
        
        if not items:
            break
        
        # Use execute_values for fast bulk insert
        values = [(
            item['source_warehouse'],
            item['picker_id'],
            item['item_status'],
            item['dispatch_by_date'],
            item['external_picklist_id'],
            item['location_bin_id'],
            item['location_sequence'],
            item['updated_at'],
            item['processed_at'],
            item['csv_file']
        ) for item in items]
        
        execute_values(pg_cursor, '''
            INSERT INTO items (source_warehouse, picker_id, item_status, dispatch_by_date,
                              external_picklist_id, location_bin_id, location_sequence,
                              updated_at, processed_at, csv_file)
            VALUES %s
        ''', values)
        
        pg_conn.commit()
        migrated += len(items)
        print(f"  📦 Migrated {migrated:,} / {total_items:,} items ({migrated*100//total_items}%)")
        
        offset += BATCH_SIZE
    
    print(f"  ✅ Migrated {migrated:,} items")
    
    # Migrate processed_csvs
    print("📄 Migrating processed CSV records...")
    sqlite_cursor.execute('SELECT filename, processed_at FROM processed_csvs')
    csvs = sqlite_cursor.fetchall()
    
    if csvs:
        pg_cursor.execute('DELETE FROM processed_csvs')
        for csv_record in csvs:
            pg_cursor.execute('''
                INSERT INTO processed_csvs (filename, processed_at)
                VALUES (%s, %s)
                ON CONFLICT (filename) DO UPDATE SET processed_at = EXCLUDED.processed_at
            ''', (csv_record['filename'], csv_record['processed_at']))
        pg_conn.commit()
        print(f"  ✅ Migrated {len(csvs)} CSV records")
    else:
        print("  ⚠️ No CSV records to migrate")
    
    # Close connections
    sqlite_conn.close()
    pg_conn.close()
    
    print("\n✅ Migration complete!")
    print("🔗 Your app will now use PostgreSQL automatically via DATABASE_URL")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python migrate_to_postgres.py <POSTGRESQL_URL>")
        print("\nExample:")
        print('  python migrate_to_postgres.py "postgresql://user:pass@host/dbname"')
        print("\nGet the connection string from Render Dashboard → Database → External Connection String")
        sys.exit(1)
    
    postgres_url = sys.argv[1]
    migrate_data(postgres_url)

