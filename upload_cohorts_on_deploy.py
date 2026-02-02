#!/usr/bin/env python3
"""
Upload cohorts during Render deployment.
This runs as part of the build process using internal database connection.
"""

import os
import csv
import sys

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("⚠️ No DATABASE_URL, skipping cohort upload")
    sys.exit(0)

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from werkzeug.security import generate_password_hash
except ImportError:
    print("⚠️ Missing dependencies, skipping cohort upload")
    sys.exit(0)

# Fix postgres:// vs postgresql://
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

COHORTS_FILE = 'data_to_upload/cohorts.csv'

def upload_cohorts():
    if not os.path.exists(COHORTS_FILE):
        print(f"⚠️ No cohorts file at {COHORTS_FILE}")
        return
    
    print(f"📤 Uploading cohorts from {COHORTS_FILE}...")
    
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cursor = conn.cursor()
    
    with open(COHORTS_FILE, 'r', encoding='utf-8') as f:
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
        
        print(f"   Found {len(cohort_map)} cohorts")
        
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
    
    conn.close()

if __name__ == '__main__':
    upload_cohorts()



