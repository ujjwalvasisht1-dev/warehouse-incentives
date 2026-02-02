#!/usr/bin/env python3
"""
Upload pickers during Render deployment.
This runs as part of the build process using internal database connection.
"""

import os
import csv
import sys
from datetime import datetime

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("⚠️ No DATABASE_URL, skipping picker upload")
    sys.exit(0)

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from werkzeug.security import generate_password_hash
except ImportError:
    print("⚠️ Missing dependencies, skipping picker upload")
    sys.exit(0)

# Fix postgres:// vs postgresql://
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

PICKERS_FILE = 'data_to_upload/pickers.csv'

def parse_date(date_str):
    """Parse date string in various formats"""
    if not date_str:
        return None
    
    formats = ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None

def upload_pickers():
    if not os.path.exists(PICKERS_FILE):
        print(f"⚠️ No pickers file at {PICKERS_FILE}")
        return
    
    print(f"📤 Uploading pickers from {PICKERS_FILE}...")
    
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cursor = conn.cursor()
    
    # Add name and doj columns if they don't exist
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS name TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS doj DATE")
        conn.commit()
    except Exception as e:
        print(f"   Note: Column migration: {e}")
        conn.rollback()
    
    with open(PICKERS_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        created = 0
        updated = 0
        
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
            
            # Check if user exists
            cursor.execute('SELECT id FROM users WHERE LOWER(picker_id) = LOWER(%s)', (picker_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing user
                cursor.execute('''
                    UPDATE users SET name = %s, cohort = %s, doj = %s, password = %s 
                    WHERE LOWER(picker_id) = LOWER(%s)
                ''', (name, cohort_num, doj, generate_password_hash(picker_id), picker_id))
                updated += 1
            else:
                # Create new user with password = picker_id
                cursor.execute('''
                    INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                    VALUES (%s, %s, %s, %s, %s, %s, 0)
                ''', (picker_id, generate_password_hash(picker_id), 'picker', name, cohort_num, doj))
                created += 1
        
        conn.commit()
        print(f"✅ Pickers uploaded! Created: {created}, Updated: {updated}")
        
        # Show cohort summary
        cursor.execute('''
            SELECT cohort, COUNT(*) as count 
            FROM users 
            WHERE cohort IS NOT NULL 
            GROUP BY cohort 
            ORDER BY cohort
        ''')
        cohort_summary = cursor.fetchall()
        print("   Cohort summary:")
        for row in cohort_summary:
            print(f"      Cohort {row['cohort']}: {row['count']} pickers")
    
    conn.close()

if __name__ == '__main__':
    upload_pickers()

