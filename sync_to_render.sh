#!/bin/bash

# Sync local database to Render
# Run this after uploading data via local admin dashboard

echo "🔄 Syncing database to Render..."
echo ""

# Navigate to project directory
cd "$(dirname "$0")"

# Check if database exists
if [ ! -f "incentives.db" ]; then
    echo "❌ Error: incentives.db not found!"
    echo "   Make sure you've uploaded data via http://localhost:5001/admin first"
    exit 1
fi

# Get database size
DB_SIZE=$(du -h incentives.db | cut -f1)
echo "📊 Database size: $DB_SIZE"

# Show stats
echo ""
echo "📈 Current database stats:"
sqlite3 incentives.db "SELECT 'Total items: ' || COUNT(*) FROM items;"
sqlite3 incentives.db "SELECT 'Unique pickers: ' || COUNT(DISTINCT picker_id) FROM items;"
sqlite3 incentives.db "SELECT 'Registered users: ' || COUNT(*) FROM users;"
echo ""

# Confirm before pushing
read -p "Push this database to Render? (y/n): " confirm
if [ "$confirm" != "y" ]; then
    echo "Cancelled."
    exit 0
fi

# Add database to git (temporarily remove from gitignore)
echo ""
echo "📦 Preparing to push..."

# Backup original gitignore
cp .gitignore .gitignore.bak

# Remove *.db from gitignore temporarily
grep -v "^\*\.db$" .gitignore > .gitignore.tmp
mv .gitignore.tmp .gitignore

# Add and commit
git add incentives.db .gitignore
git commit -m "Sync database - $(date '+%Y-%m-%d %H:%M:%S')"

# Push to GitHub
echo "🚀 Pushing to GitHub..."
git push origin main

# Restore gitignore
mv .gitignore.bak .gitignore
git add .gitignore
git commit -m "Restore gitignore"
git push origin main

echo ""
echo "✅ Done! Render will redeploy automatically."
echo "   Wait 2-3 minutes for changes to appear on https://warehouse-incentives.onrender.com"

