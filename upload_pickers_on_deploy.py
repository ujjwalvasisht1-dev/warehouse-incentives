#!/usr/bin/env python3
"""
Upload pickers during Render deployment.
This runs as part of the build process using internal database connection.
"""

import os
import csv
import sys
from datetime import datetime

print("=" * 60)
print("🚀 PICKER UPLOAD SCRIPT STARTED")
print("=" * 60)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("⚠️ No DATABASE_URL found, skipping picker upload")
    print("   This is normal for local development without PostgreSQL")
    sys.exit(0)

print(f"✅ DATABASE_URL found (length: {len(DATABASE_URL)})")

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from werkzeug.security import generate_password_hash
    print("✅ Dependencies imported successfully")
except ImportError as e:
    print(f"❌ Missing dependencies: {e}")
    sys.exit(1)

# Fix postgres:// vs postgresql://
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    print("✅ Fixed DATABASE_URL format (postgres:// -> postgresql://)")

PICKERS_FILE = 'data_to_upload/pickers.csv'

def parse_date(date_str):
    """Parse date string in various formats"""
    if not date_str:
        return None
    
    formats = ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d-%B-%Y']
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    print(f"   ⚠️ Could not parse date: {date_str}")
    return None

def upload_pickers():
    if not os.path.exists(PICKERS_FILE):
        print(f"❌ Pickers file not found at: {PICKERS_FILE}")
        print(f"   Current directory: {os.getcwd()}")
        print(f"   Files in data_to_upload/: {os.listdir('data_to_upload') if os.path.exists('data_to_upload') else 'directory not found'}")
        return
    
    print(f"✅ Found pickers file: {PICKERS_FILE}")
    
    # Connect to database
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        print("✅ Connected to PostgreSQL database")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    # Ensure users table has required columns
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS name TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS doj DATE")
        conn.commit()
        print("✅ Database schema updated (name, doj columns)")
    except Exception as e:
        print(f"   Note: Column migration issue (may already exist): {e}")
        conn.rollback()
    
    # STEP 1: Delete ALL picker users completely
    try:
        cursor.execute("SELECT COUNT(*) as count FROM users WHERE role = 'picker'")
        existing_count = cursor.fetchone()['count']
        print(f"📊 Found {existing_count} existing pickers in database")
        
        cursor.execute("DELETE FROM users WHERE role = 'picker'")
        conn.commit()
        print(f"🗑️  DELETED all {existing_count} existing pickers")
    except Exception as e:
        print(f"❌ Error deleting pickers: {e}")
        conn.rollback()
    
    # STEP 2: Read and insert all pickers from CSV
    created = 0
    errors = 0
    
    with open(PICKERS_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        print(f"📋 CSV columns: {reader.fieldnames}")
        
        for row in reader:
            # Get picker info - handle different column names
            picker_id = row.get('Casper ID', row.get('casper_id', row.get('picker_id', ''))).strip()
            name = row.get('Name', row.get('name', '')).strip()
            cohort_str = row.get('Cohort', row.get('cohort', '')).strip()
            doj_str = row.get('DOJ', row.get('doj', row.get('Date of Joining', ''))).strip()
            
            if not picker_id:
                continue
            
            # Parse cohort
            try:
                cohort_num = int(cohort_str) if cohort_str else None
            except ValueError:
                cohort_num = None
            
            # Parse DOJ
            doj = parse_date(doj_str)
            
            # Create password hash (password = picker_id, case-sensitive)
            password_hash = generate_password_hash(picker_id)
            
            # Insert picker
            try:
                cursor.execute('''
                    INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                    VALUES (%s, %s, %s, %s, %s, %s, 0)
                ''', (picker_id, password_hash, 'picker', name, cohort_num, doj))
                created += 1
            except Exception as e:
                print(f"   ❌ Error creating {picker_id}: {e}")
                errors += 1
        
        conn.commit()
    
    print(f"\n{'=' * 60}")
    print(f"✅ PICKER UPLOAD COMPLETE!")
    print(f"   Created: {created} pickers")
    print(f"   Errors: {errors}")
    print(f"{'=' * 60}")
    
    # Show sample pickers for verification
    cursor.execute('''
        SELECT picker_id, name, cohort FROM users 
        WHERE role = 'picker' 
        ORDER BY picker_id 
        LIMIT 5
    ''')
    samples = cursor.fetchall()
    print("\n📋 Sample pickers (use picker_id as BOTH username AND password):")
    for s in samples:
        print(f"   Username: {s['picker_id']} | Name: {s['name']} | Cohort: {s['cohort']}")
    
    # Show cohort summary
    cursor.execute('''
        SELECT cohort, COUNT(*) as count 
        FROM users 
        WHERE role = 'picker' AND cohort IS NOT NULL 
        GROUP BY cohort 
        ORDER BY cohort
    ''')
    cohort_summary = cursor.fetchall()
    print("\n📊 Cohort summary:")
    for row in cohort_summary:
        print(f"   Cohort {row['cohort']}: {row['count']} pickers")
    
    # Verify total count
    cursor.execute("SELECT COUNT(*) as count FROM users WHERE role = 'picker'")
    total = cursor.fetchone()['count']
    print(f"\n✅ TOTAL PICKERS IN DATABASE: {total}")
    
    conn.close()
    print("\n🎉 Upload script completed successfully!")

if __name__ == '__main__':
    upload_pickers()
