#!/usr/bin/env python3
"""
Upload pickers during Render deployment using pre-computed password hashes.
This runs as part of the build process.
"""

import os
import json
import sys

print("=" * 60)
print("🚀 PICKER UPLOAD SCRIPT STARTED")
print("=" * 60)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("⚠️ No DATABASE_URL found, skipping picker upload")
    sys.exit(0)

print(f"✅ DATABASE_URL found")

try:
    import psycopg2
    from psycopg2.extras import execute_values
    print("✅ Dependencies imported")
except ImportError as e:
    print(f"❌ Missing dependencies: {e}")
    sys.exit(1)

# Fix postgres:// vs postgresql://
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

PICKERS_JSON = 'data_to_upload/pickers_with_hashes.json'

def upload_pickers():
    if not os.path.exists(PICKERS_JSON):
        print(f"❌ Pre-computed pickers file not found: {PICKERS_JSON}")
        return
    
    print(f"✅ Found pre-computed pickers file")
    
    # Load pre-computed data
    with open(PICKERS_JSON, 'r') as f:
        pickers = json.load(f)
    
    print(f"📊 Loaded {len(pickers)} pickers with pre-computed hashes")
    
    # Connect to database
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Connected to PostgreSQL")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    # Ensure columns exist
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS name TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS doj DATE")
        conn.commit()
        print("✅ Schema updated")
    except Exception as e:
        print(f"   Schema note: {e}")
        conn.rollback()
    
    # Delete ALL existing pickers
    cursor.execute("DELETE FROM users WHERE role = 'picker'")
    deleted = cursor.rowcount
    conn.commit()
    print(f"🗑️  Deleted {deleted} existing pickers")
    
    # Insert all pickers using batch insert (much faster)
    print("📤 Inserting pickers...")
    
    # Prepare data for batch insert
    values = []
    for p in pickers:
        values.append((
            p['picker_id'],
            p['password'],
            'picker',
            p['name'],
            p['cohort'],
            p['doj'],
            0  # password_changed
        ))
    
    # Batch insert
    try:
        execute_values(
            cursor,
            """
            INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
            VALUES %s
            """,
            values,
            page_size=100
        )
        conn.commit()
        print(f"✅ Inserted {len(values)} pickers")
    except Exception as e:
        print(f"❌ Insert error: {e}")
        conn.rollback()
        
        # Try one by one if batch fails
        print("   Trying individual inserts...")
        created = 0
        for p in pickers:
            try:
                cursor.execute("""
                    INSERT INTO users (picker_id, password, role, name, cohort, doj, password_changed)
                    VALUES (%s, %s, %s, %s, %s, %s, 0)
                    ON CONFLICT (picker_id) DO UPDATE SET
                        password = EXCLUDED.password,
                        name = EXCLUDED.name,
                        cohort = EXCLUDED.cohort,
                        doj = EXCLUDED.doj
                """, (p['picker_id'], p['password'], 'picker', p['name'], p['cohort'], p['doj']))
                created += 1
            except Exception as e2:
                print(f"   Error: {p['picker_id']}: {e2}")
        conn.commit()
        print(f"✅ Created/updated {created} pickers")
    
    # Verify count
    cursor.execute("SELECT COUNT(*) FROM users WHERE role = 'picker'")
    total = cursor.fetchone()[0]
    print(f"\n✅ TOTAL PICKERS IN DATABASE: {total}")
    
    # Show samples
    cursor.execute("""
        SELECT picker_id, name, cohort FROM users 
        WHERE role = 'picker' 
        ORDER BY picker_id 
        LIMIT 5
    """)
    samples = cursor.fetchall()
    print("\n📋 Sample pickers:")
    for s in samples:
        print(f"   {s[0]} | {s[1]} | Cohort {s[2]}")
    
    # Cohort summary
    cursor.execute("""
        SELECT cohort, COUNT(*) FROM users 
        WHERE role = 'picker' AND cohort IS NOT NULL 
        GROUP BY cohort ORDER BY cohort
    """)
    print("\n📊 Cohort summary:")
    for row in cursor.fetchall():
        print(f"   Cohort {row[0]}: {row[1]} pickers")
    
    conn.close()
    print("\n🎉 UPLOAD COMPLETE!")

if __name__ == '__main__':
    upload_pickers()
