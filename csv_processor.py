"""
CSV Processor Script
Processes CSV files from csv_uploads folder and updates the database
Run this script every 10 minutes (via cron or scheduler)
"""

import sqlite3
import csv
import os
from datetime import datetime
import glob

DATABASE = 'incentives.db'
CSV_UPLOAD_FOLDER = 'csv_uploads'

def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def process_csv_file(filepath, filename):
    """Process a single CSV file and insert data into database"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            rows_inserted = 0
            for row in reader:
                # Parse updated_at timestamp
                try:
                    updated_at_str = row.get('updated_at', '').strip()
                    if not updated_at_str:
                        continue
                    
                    try:
                        updated_at = datetime.strptime(updated_at_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            updated_at = datetime.strptime(updated_at_str, '%Y-%m-%d %H:%M:%S.%f')
                        except ValueError:
                            print(f"Warning: Could not parse timestamp: {updated_at_str}")
                            continue
                except Exception as e:
                    print(f"Warning: Error parsing timestamp: {e}")
                    continue
                
                # Normalize picker_id (case-insensitive)
                picker_id = row.get('picker_ldap', '').strip()
                if not picker_id:
                    continue
                
                # Insert item record
                cursor.execute('''
                    INSERT INTO items (
                        source_warehouse, picker_id, item_status, dispatch_by_date,
                        external_picklist_id, location_bin_id, location_sequence,
                        updated_at, csv_file
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row.get('source_warehouse', ''),
                    picker_id,
                    row.get('item_status', ''),
                    row.get('dispatch_by_date', ''),
                    row.get('external_picklist_id', ''),
                    row.get('location_bin_id', ''),
                    row.get('location_sequence', ''),
                    updated_at,
                    filename
                ))
                rows_inserted += 1
            
            # Mark CSV as processed
            cursor.execute('''
                INSERT OR IGNORE INTO processed_csvs (filename, processed_at)
                VALUES (?, ?)
            ''', (filename, datetime.now()))
            
            conn.commit()
            print(f"Processed {filename}: {rows_inserted} rows inserted")
            return rows_inserted
            
    except Exception as e:
        conn.rollback()
        print(f"Error processing {filename}: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0
    finally:
        conn.close()

def process_new_csvs():
    """Process all new CSV files in the upload folder"""
    if not os.path.exists(CSV_UPLOAD_FOLDER):
        print(f"CSV upload folder '{CSV_UPLOAD_FOLDER}' does not exist. Creating it...")
        os.makedirs(CSV_UPLOAD_FOLDER, exist_ok=True)
        return
    
    # Get list of already processed files
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT filename FROM processed_csvs')
    processed_files = {row[0] for row in cursor.fetchall()}
    conn.close()
    
    # Find all CSV files in upload folder
    csv_files = glob.glob(os.path.join(CSV_UPLOAD_FOLDER, '*.csv'))
    
    new_files = [f for f in csv_files if os.path.basename(f) not in processed_files]
    
    if not new_files:
        print("No new CSV files to process")
        return
    
    print(f"Found {len(new_files)} new CSV file(s) to process")
    
    total_rows = 0
    for filepath in new_files:
        filename = os.path.basename(filepath)
        rows = process_csv_file(filepath, filename)
        total_rows += rows
    
    print(f"Processing complete. Total rows inserted: {total_rows}")

if __name__ == '__main__':
    print(f"Starting CSV processing at {datetime.now()}")
    process_new_csvs()
    print("CSV processing finished")

