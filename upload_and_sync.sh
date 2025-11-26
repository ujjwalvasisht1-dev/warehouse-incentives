#!/bin/bash

# Daily Data Upload & Sync Script
# Run this script to upload CSV and sync to Render in one go

echo "╔════════════════════════════════════════════════════════════╗"
echo "║       Warehouse Incentives - Data Upload & Sync            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

cd "$(dirname "$0")"

# Check if a CSV file was provided
if [ -z "$1" ]; then
    echo "Usage: ./upload_and_sync.sh <path_to_csv_file>"
    echo ""
    echo "Example: ./upload_and_sync.sh ~/Downloads/picker_data.csv"
    echo ""
    
    # Check if there are CSV files in csv_uploads folder
    CSV_COUNT=$(ls -1 csv_uploads/*.csv 2>/dev/null | wc -l)
    if [ "$CSV_COUNT" -gt 0 ]; then
        echo "📁 CSV files found in csv_uploads folder:"
        ls -la csv_uploads/*.csv
        echo ""
        echo "You can also process these by copying to csv_uploads and running the processor."
    fi
    exit 1
fi

CSV_FILE="$1"

# Check if file exists
if [ ! -f "$CSV_FILE" ]; then
    echo "❌ Error: File not found: $CSV_FILE"
    exit 1
fi

# Get file info
FILE_SIZE=$(du -h "$CSV_FILE" | cut -f1)
FILE_NAME=$(basename "$CSV_FILE")
echo "📄 File: $FILE_NAME"
echo "📊 Size: $FILE_SIZE"
echo ""

# Step 1: Start local server if not running
echo "1️⃣  Checking local server..."
if ! curl -s http://localhost:5001 > /dev/null 2>&1; then
    echo "   Starting local Flask server..."
    pkill -f "python3 app.py" 2>/dev/null
    sleep 1
    python3 app.py > /dev/null 2>&1 &
    sleep 3
    
    if ! curl -s http://localhost:5001 > /dev/null 2>&1; then
        echo "❌ Error: Could not start local server"
        exit 1
    fi
fi
echo "   ✅ Local server running at http://localhost:5001"
echo ""

# Step 2: Run the database setup if needed
echo "2️⃣  Ensuring database is set up..."
python3 setup.py > /dev/null 2>&1
echo "   ✅ Database ready"
echo ""

# Step 3: Process CSV using Python directly
echo "3️⃣  Processing CSV file..."
python3 << EOF
import csv
import sqlite3
from datetime import datetime
from werkzeug.security import generate_password_hash

# Read the CSV file
csv_file = "$CSV_FILE"
print(f"   Reading: $FILE_NAME")

conn = sqlite3.connect('incentives.db')
cursor = conn.cursor()

items_batch = []
pickers_seen = set()
rows_processed = 0
BATCH_SIZE = 1000

with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    
    for row in reader:
        updated_at_str = row.get('updated_at', '').strip()
        if not updated_at_str:
            continue
        
        try:
            try:
                updated_at = datetime.strptime(updated_at_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                updated_at = datetime.strptime(updated_at_str, '%Y-%m-%d %H:%M:%S.%f')
        except:
            continue
        
        picker_id = row.get('picker_ldap', '').strip()
        if not picker_id:
            continue
        
        items_batch.append((
            row.get('source_warehouse', ''),
            picker_id,
            row.get('item_status', ''),
            row.get('dispatch_by_date', ''),
            row.get('external_picklist_id', ''),
            row.get('location_bin_id', ''),
            row.get('location_sequence', ''),
            updated_at.strftime('%Y-%m-%d %H:%M:%S'),
            "$FILE_NAME"
        ))
        
        pickers_seen.add(picker_id)
        
        if len(items_batch) >= BATCH_SIZE:
            cursor.executemany('''
                INSERT INTO items (
                    source_warehouse, picker_id, item_status, dispatch_by_date,
                    external_picklist_id, location_bin_id, location_sequence,
                    updated_at, csv_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', items_batch)
            rows_processed += len(items_batch)
            print(f"   Processed {rows_processed} rows...", end='\r')
            items_batch = []

# Insert remaining
if items_batch:
    cursor.executemany('''
        INSERT INTO items (
            source_warehouse, picker_id, item_status, dispatch_by_date,
            external_picklist_id, location_bin_id, location_sequence,
            updated_at, csv_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', items_batch)
    rows_processed += len(items_batch)

# Create picker users
default_password = generate_password_hash('picker123')
pickers_added = 0
for picker_id in pickers_seen:
    try:
        cursor.execute('INSERT INTO users (picker_id, password, role, password_changed) VALUES (?, ?, ?, ?)',
                      (picker_id, default_password, 'picker', 0))
        pickers_added += 1
    except:
        pass

# Record upload
cursor.execute('INSERT OR REPLACE INTO processed_csvs (filename, processed_at) VALUES (?, ?)',
              ("$FILE_NAME", datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

conn.commit()
conn.close()

print(f"   ✅ Inserted {rows_processed} rows                    ")
print(f"   ✅ Added {pickers_added} new pickers")
print(f"   ✅ Found {len(pickers_seen)} unique pickers in file")
EOF

echo ""

# Step 4: Show current stats
echo "4️⃣  Database stats:"
sqlite3 incentives.db "SELECT '   Total items: ' || COUNT(*) FROM items;"
sqlite3 incentives.db "SELECT '   Unique pickers (data): ' || COUNT(DISTINCT picker_id) FROM items;"
sqlite3 incentives.db "SELECT '   Registered users: ' || COUNT(*) FROM users;"
echo ""

# Step 5: Ask to sync to Render
echo "5️⃣  Sync to Render?"
read -p "   Push database to Render? (y/n): " confirm

if [ "$confirm" = "y" ]; then
    echo ""
    echo "   📦 Preparing to push..."
    
    # Temporarily allow database in git
    cp .gitignore .gitignore.bak
    grep -v "^\*\.db$" .gitignore > .gitignore.tmp
    mv .gitignore.tmp .gitignore
    
    # Commit and push
    git add incentives.db .gitignore
    git commit -m "Data update - $(date '+%Y-%m-%d %H:%M:%S') - $FILE_NAME"
    
    echo "   🚀 Pushing to GitHub..."
    git push origin main
    
    # Restore gitignore
    mv .gitignore.bak .gitignore
    git add .gitignore
    git commit -m "Restore gitignore" --allow-empty
    git push origin main
    
    echo ""
    echo "   ✅ Pushed to Render!"
    echo "   ⏳ Wait 2-3 minutes for Render to redeploy"
    echo "   🌐 https://warehouse-incentives.onrender.com"
else
    echo ""
    echo "   Skipped sync. You can sync later with:"
    echo "   ./sync_to_render.sh"
fi

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                        ✅ Complete!                        ║"
echo "╚════════════════════════════════════════════════════════════╝"

