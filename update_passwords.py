"""
Script to update all picker passwords to match their picker_id.
Password = picker_id (same case)
"""

import sqlite3
from werkzeug.security import generate_password_hash

DATABASE = 'incentives.db'

def update_passwords():
    """Update all picker passwords to match their picker_id"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Get all pickers
    cursor.execute('SELECT id, picker_id FROM users WHERE role = "picker"')
    pickers = cursor.fetchall()
    
    print(f"Updating passwords for {len(pickers)} pickers...")
    
    updated = 0
    for user_id, picker_id in pickers:
        # Password = picker_id (same case)
        new_password_hash = generate_password_hash(picker_id)
        cursor.execute('UPDATE users SET password = ?, password_changed = 0 WHERE id = ?', 
                      (new_password_hash, user_id))
        updated += 1
    
    conn.commit()
    conn.close()
    
    print(f"✅ Updated {updated} picker passwords")
    print("\nLogin credentials:")
    print("  Username: <picker_id> (e.g., Ca.3099373)")
    print("  Password: <same as username> (e.g., Ca.3099373)")

if __name__ == '__main__':
    update_passwords()

