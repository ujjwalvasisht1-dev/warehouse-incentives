#!/usr/bin/env python3
"""
Prepare picker data for deployment.

Usage:
1. Place your CSV file in data_to_upload/pickers.csv
2. Run: python prepare_pickers.py
3. Commit and push to deploy

The CSV should have columns: Casper ID, Name, Designation, Cohort, DOJ
"""

import csv
import json
import os
from werkzeug.security import generate_password_hash
from datetime import datetime

def parse_date(date_str):
    """Parse date string in various formats"""
    if not date_str:
        return None
    for fmt in ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None

def main():
    csv_file = 'data_to_upload/pickers.csv'
    output_file = 'data_to_upload/pickers_with_hashes.json'
    
    if not os.path.exists(csv_file):
        print(f"❌ Error: {csv_file} not found!")
        print("Please place your picker CSV file at data_to_upload/pickers.csv")
        return
    
    print(f"📂 Reading {csv_file}...")
    
    pickers = []
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            picker_id = row.get('Casper ID', row.get('casper_id', row.get('picker_id', ''))).strip()
            if not picker_id:
                continue
            
            name = row.get('Name', row.get('name', '')).strip()
            cohort_str = row.get('Cohort', row.get('cohort', '')).strip()
            doj_str = row.get('DOJ', row.get('doj', row.get('Date of Joining', ''))).strip()
            
            try:
                cohort = int(cohort_str) if cohort_str else None
            except ValueError:
                cohort = None
            
            doj = parse_date(doj_str)
            
            pickers.append({
                'picker_id': picker_id,
                'name': name,
                'cohort': cohort,
                'doj': doj
            })
    
    print(f"✅ Found {len(pickers)} pickers")
    
    # Generate password hashes (this is the slow part)
    print(f"🔐 Generating password hashes (this may take a minute)...")
    for i, p in enumerate(pickers):
        p['password_hash'] = generate_password_hash(p['picker_id'])
        if (i + 1) % 100 == 0:
            print(f"   Processed {i + 1}/{len(pickers)}")
    
    print(f"💾 Saving to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(pickers, f, indent=2)
    
    print(f"\n✅ Done! {len(pickers)} pickers prepared.")
    print(f"\n📋 Cohort Summary:")
    cohort_counts = {}
    for p in pickers:
        c = p['cohort'] or 'No Cohort'
        cohort_counts[c] = cohort_counts.get(c, 0) + 1
    for c in sorted([k for k in cohort_counts.keys() if k != 'No Cohort']):
        print(f"   Cohort {c}: {cohort_counts[c]} pickers")
    if 'No Cohort' in cohort_counts:
        print(f"   No Cohort: {cohort_counts['No Cohort']} pickers")
    
    print(f"\n🚀 Next steps:")
    print(f"   1. git add -A")
    print(f"   2. git commit -m 'Update picker data'")
    print(f"   3. git push origin main")
    print(f"   4. Wait for Render to deploy")
    print(f"   5. Visit https://warehouse-incentives.onrender.com/debug/fast-load")
    print(f"      (refresh until all pickers are loaded)")

if __name__ == '__main__':
    main()
