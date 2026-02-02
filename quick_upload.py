#!/usr/bin/env python3
"""
Quick upload script to push data directly to PostgreSQL on Render.
Much faster and more reliable than web uploads.

Usage:
    python3 quick_upload.py "postgresql://..." --cohorts path/to/cohorts.csv
    python3 quick_upload.py "postgresql://..." --data path/to/data.csv
    python3 quick_upload.py "postgresql://..." --cohorts cohorts.csv --data data.csv
"""

import sys
import csv
import os
from datetime import datetime
from werkzeug.security import generate_password_hash

try:
    import psycopg2
    from psycopg2.extras import execute_values, RealDictCursor
except ImportError:
    print("❌ psycopg2 not installed. Run: pip3 install psycopg2-binary")
    sys.exit(1)

def connect_db(url):
    """Connect to PostgreSQL"""
    if url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)
    print(f"🔗 Connecting to PostgreSQL...")
    conn = psycopg2.connect(url, cursor_factory=RealDictCursor)
    print("✅ Connected!")
    return conn

def upload_cohorts(conn, csv_path):
    """Upload cohort assignments"""
    print(f"\n📤 Uploading cohorts from: {csv_path}")
    
    cursor = conn.cursor()
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        # Map column index to cohort number
        cohort_map = {}
        for idx, col_name in enumerate(header):
            if col_name.strip().lower().startswith('cohort'):
                try:
                    cohort_num = int(col_name.strip().split()[-1])
                    cohort_map[idx] = cohort_num
                except ValueError:
                    continue
        
        print(f"   Found {len(cohort_map)} cohorts: {list(cohort_map.values())}")
        
        # Build picker-cohort mapping
        picker_cohorts = {}
        for row in reader:
            for idx, cohort_num in cohort_map.items():
                if idx < len(row):
                    picker_id = row[idx].strip()
                    if picker_id:
                        picker_cohorts[picker_id] = cohort_num
        
        print(f"   Total pickers: {len(picker_cohorts)}")
        
        # Upsert users
        created = 0
        updated = 0
        
        for picker_id, cohort_num in picker_cohorts.items():
            cursor.execute('SELECT id FROM users WHERE LOWER(picker_id) = LOWER(%s)', (picker_id,))
            existing = cursor.fetchone()
            
            if existing:
                cursor.execute(
                    'UPDATE users SET cohort = %s, password = %s WHERE LOWER(picker_id) = LOWER(%s)',
                    (cohort_num, generate_password_hash(picker_id), picker_id)
                )
                updated += 1
            else:
                cursor.execute('''
                    INSERT INTO users (picker_id, password, role, cohort, password_changed)
                    VALUES (%s, %s, %s, %s, 0)
                ''', (picker_id, generate_password_hash(picker_id), 'picker', cohort_num))
                created += 1
        
        conn.commit()
        print(f"✅ Cohorts uploaded! Created: {created}, Updated: {updated}")

def upload_data(conn, csv_path):
    """Upload picker data CSV"""
    print(f"\n📤 Uploading data from: {csv_path}")
    
    cursor = conn.cursor()
    
    # Count rows first
    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        total_rows = sum(1 for _ in f) - 1  # minus header
    print(f"   Total rows to process: {total_rows:,}")
    
    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        batch = []
        pickers_seen = set()
        rows_inserted = 0
        BATCH_SIZE = 1000
        
        for row in reader:
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
            
            picker_id = row.get('picker_ldap', '').strip()
            if not picker_id:
                continue
            
            batch.append((
                row.get('source_warehouse', ''),
                picker_id,
                row.get('item_status', ''),
                row.get('dispatch_by_date', ''),
                row.get('external_picklist_id', ''),
                row.get('location_bin_id', ''),
                row.get('location_sequence', ''),
                updated_at.strftime('%Y-%m-%d %H:%M:%S'),
                os.path.basename(csv_path)
            ))
            pickers_seen.add(picker_id)
            
            if len(batch) >= BATCH_SIZE:
                execute_values(cursor, '''
                    INSERT INTO items (
                        source_warehouse, picker_id, item_status, dispatch_by_date,
                        external_picklist_id, location_bin_id, location_sequence,
                        updated_at, csv_file
                    ) VALUES %s
                ''', batch)
                conn.commit()
                rows_inserted += len(batch)
                print(f"   Progress: {rows_inserted:,} / {total_rows:,} ({rows_inserted*100//total_rows}%)")
                batch = []
        
        # Insert remaining
        if batch:
            execute_values(cursor, '''
                INSERT INTO items (
                    source_warehouse, picker_id, item_status, dispatch_by_date,
                    external_picklist_id, location_bin_id, location_sequence,
                    updated_at, csv_file
                ) VALUES %s
            ''', batch)
            conn.commit()
            rows_inserted += len(batch)
        
        print(f"✅ Data uploaded! {rows_inserted:,} rows inserted")
        
        # Create picker accounts for new pickers
        default_password = generate_password_hash('picker123')
        pickers_added = 0
        for picker_id in pickers_seen:
            cursor.execute('''
                INSERT INTO users (picker_id, password, role, password_changed)
                VALUES (%s, %s, %s, 0)
                ON CONFLICT (picker_id) DO NOTHING
            ''', (picker_id, default_password, 'picker'))
            pickers_added += cursor.rowcount
        conn.commit()
        
        if pickers_added > 0:
            print(f"   Created {pickers_added} new picker accounts")
        
        # Record upload
        cursor.execute('''
            INSERT INTO processed_csvs (filename, processed_at)
            VALUES (%s, %s)
            ON CONFLICT (filename) DO UPDATE SET processed_at = EXCLUDED.processed_at
        ''', (os.path.basename(csv_path), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()

def show_stats(conn):
    """Show database stats"""
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) as count FROM items')
    total_items = cursor.fetchone()['count']
    
    cursor.execute('SELECT COUNT(*) as count FROM users WHERE role = %s', ('picker',))
    total_pickers = cursor.fetchone()['count']
    
    cursor.execute('SELECT COUNT(DISTINCT cohort) as count FROM users WHERE cohort IS NOT NULL')
    total_cohorts = cursor.fetchone()['count']
    
    cursor.execute('SELECT MIN(updated_at), MAX(updated_at) FROM items')
    dates = cursor.fetchone()
    
    print(f"\n📊 Database Stats:")
    print(f"   Total items: {total_items:,}")
    print(f"   Total pickers: {total_pickers}")
    print(f"   Active cohorts: {total_cohorts}")
    if dates['min']:
        print(f"   Date range: {dates['min']} to {dates['max']}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage:")
        print('  python3 quick_upload.py "postgresql://..." --cohorts cohorts.csv')
        print('  python3 quick_upload.py "postgresql://..." --data data.csv')
        print('  python3 quick_upload.py "postgresql://..." --cohorts cohorts.csv --data data.csv')
        print('  python3 quick_upload.py "postgresql://..." --stats')
        sys.exit(1)
    
    db_url = sys.argv[1]
    
    # Parse arguments
    cohorts_path = None
    data_path = None
    show_stats_only = False
    
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == '--cohorts' and i + 1 < len(sys.argv):
            cohorts_path = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--data' and i + 1 < len(sys.argv):
            data_path = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--stats':
            show_stats_only = True
            i += 1
        else:
            i += 1
    
    conn = connect_db(db_url)
    
    if cohorts_path:
        upload_cohorts(conn, cohorts_path)
    
    if data_path:
        upload_data(conn, data_path)
    
    show_stats(conn)
    conn.close()
    
    print("\n✅ Done!")



