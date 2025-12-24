"""
Script to import picker cohorts from CSV file.
This will create users for each picker with their assigned cohort.
"""

import sqlite3
import csv
from werkzeug.security import generate_password_hash
import os

DATABASE = 'incentives.db'
# Password = picker_id (same as username)

def import_cohorts(csv_file):
    """Import cohorts from CSV file"""
    
    if not os.path.exists(csv_file):
        print(f"Error: File {csv_file} not found!")
        return
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Ensure cohort column exists
    try:
        cursor.execute('ALTER TABLE users ADD COLUMN cohort INTEGER DEFAULT NULL')
        print("Added cohort column to users table")
    except:
        pass  # Column already exists
    
    # Read CSV file
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Get header row (Cohort 1, Cohort 2, ...)
        
        # Create a mapping: column index -> cohort number
        cohort_map = {}
        for idx, col_name in enumerate(header):
            if col_name.strip().lower().startswith('cohort'):
                # Extract cohort number from "Cohort X"
                try:
                    cohort_num = int(col_name.strip().split()[-1])
                    cohort_map[idx] = cohort_num
                except ValueError:
                    continue
        
        print(f"Found {len(cohort_map)} cohorts: {list(cohort_map.values())}")
        
        # Read all rows and build picker-cohort mapping
        picker_cohorts = {}
        for row in reader:
            for idx, cohort_num in cohort_map.items():
                if idx < len(row):
                    picker_id = row[idx].strip()
                    if picker_id:  # Not empty
                        picker_cohorts[picker_id] = cohort_num
        
        print(f"Found {len(picker_cohorts)} pickers across all cohorts")
        
        # Create/update users with cohort assignments
        # Password = picker_id (same as username)
        created = 0
        updated = 0
        
        for picker_id, cohort_num in picker_cohorts.items():
            # Check if user exists
            cursor.execute('SELECT id, cohort FROM users WHERE LOWER(picker_id) = LOWER(?)', (picker_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Update cohort and password (password = picker_id)
                cursor.execute('UPDATE users SET cohort = ?, password = ? WHERE LOWER(picker_id) = LOWER(?)', 
                             (cohort_num, generate_password_hash(picker_id), picker_id))
                updated += 1
            else:
                # Create new user with password = picker_id
                cursor.execute('''
                    INSERT INTO users (picker_id, password, role, cohort, password_changed)
                    VALUES (?, ?, ?, ?, 0)
                ''', (picker_id, generate_password_hash(picker_id), 'picker', cohort_num))
                created += 1
        
        conn.commit()
        
        # Print summary by cohort
        print("\n" + "=" * 50)
        print("COHORT SUMMARY")
        print("=" * 50)
        
        cursor.execute('''
            SELECT cohort, COUNT(*) as count 
            FROM users 
            WHERE cohort IS NOT NULL 
            GROUP BY cohort 
            ORDER BY cohort
        ''')
        
        for row in cursor.fetchall():
            print(f"  Cohort {row[0]}: {row[1]} pickers")
        
        print("=" * 50)
        print(f"\nTotal: {created} users created, {updated} users updated")
    
    conn.close()
    print("\nCohort import complete!")
    print(f"\nLogin credentials for all pickers:")
    print(f"  Username: <picker_id> (e.g., Ca.3099373)")
    print(f"  Password: <same as username>")
    print("\nPickers will be prompted to change password on first login.")

def list_cohorts():
    """List all cohorts and their pickers"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT cohort, picker_id 
        FROM users 
        WHERE cohort IS NOT NULL 
        ORDER BY cohort, picker_id
    ''')
    
    results = cursor.fetchall()
    conn.close()
    
    if not results:
        print("No cohort assignments found.")
        return
    
    current_cohort = None
    for cohort, picker_id in results:
        if cohort != current_cohort:
            if current_cohort is not None:
                print()
            print(f"=== COHORT {cohort} ===")
            current_cohort = cohort
        print(f"  {picker_id}")
    
    print(f"\nTotal: {len(results)} pickers in cohorts")

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Import cohorts: python import_cohorts.py <csv_file>")
        print("  List cohorts:   python import_cohorts.py --list")
        print("\nExample:")
        print("  python import_cohorts.py 'Picker cohorts - Sheet1.csv'")
        sys.exit(1)
    
    if sys.argv[1] == '--list':
        list_cohorts()
    else:
        import_cohorts(sys.argv[1])

