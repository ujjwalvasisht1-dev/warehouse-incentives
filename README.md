# Warehouse Incentive Tracking System

A web-based dashboard system for tracking picker performance and running incentive contests in warehouses.

## Features

- **Picker Dashboard**: Individual pickers can log in to view their rankings, stats, and performance
- **Supervisor Dashboard**: Supervisors can view all pickers' performance and download reports
- **Automatic CSV Processing**: Processes CSV files from `csv_uploads` folder every 10 minutes
- **Real-time Rankings**: Shows rankings based on Today, Yesterday, or This Week (Monday to current day)
- **Color-coded Performance**: Green (above average), Yellow (average), Red (below average)
- **Mobile-responsive**: Optimized for mobile devices

## Setup Instructions

### 1. Install Dependencies

```bash
pip3 install -r requirements.txt
```

### 2. Initialize Database and Create Sample Users

```bash
python3 setup.py
```

This will:
- Create the database
- Create sample picker and supervisor users
- Load the initial CSV file if present

### 3. Start the Application

```bash
python3 app.py
```

The application will be available at `http://localhost:5001`

**Note:** Port 5000 is often used by macOS AirPlay Receiver, so the app uses port 5001 by default.

### 4. Set Up CSV Auto-Processing

To automatically process CSV files every 10 minutes, you can:

**Option A: Using cron (Linux/Mac)**
```bash
crontab -e
```
Add this line:
```
*/10 * * * * cd /path/to/Incentives && python csv_processor.py >> csv_processor.log 2>&1
```

**Option B: Using Task Scheduler (Windows)**
Create a scheduled task to run `csv_processor.py` every 10 minutes.

**Option C: Manual Processing**
Run `python3 csv_processor.py` manually whenever you upload new CSV files.

**Option D: Using Python Scheduler (Recommended for Development)**
Run `python3 scheduler.py` in a separate terminal. This will process CSV files every 10 minutes automatically.

## Sample Login Credentials

### Pickers:
- **Picker ID**: `Ca.3817385`, **Password**: `picker123`
- **Picker ID**: `ca.3833623`, **Password**: `picker123`
- **Picker ID**: `ca.3704873`, **Password**: `picker123`

### Supervisor:
- **Picker ID**: `supervisor`, **Password**: `supervisor123`

## CSV File Format

Place CSV files in the `csv_uploads` folder. The CSV should have the following columns:

- `source_warehouse`
- `picker_ldap` (Picker ID)
- `item_status` (COMPLETED, ITEM_NOT_FOUND, CANCELLED, ITEM_REPLACED)
- `dispatch_by_date`
- `external_picklist_id`
- `location_bin_id`
- `location_sequence`
- `updated_at` (Format: YYYY-MM-DD HH:MM:SS)

## Scoring System

- **COMPLETED**: 1 point
- **ITEM_REPLACED**: 1 point
- **ITEM_NOT_FOUND**: 0 points (counted as "lost")
- **CANCELLED**: 0 points

## Time Filters

- **Today**: From 12:00 AM to current time
- **Yesterday**: Full day (12:00 AM to 11:59 PM)
- **This Week**: Monday to current day

## Access the Application

After starting the app, access it at: **http://localhost:5001**

**Note:** Port 5000 is often used by macOS AirPlay Receiver, so the app uses port 5001 by default. If you need to use a different port, edit `app.py` and change the port number in the last line.

## Deployment

For deployment to Vercel or similar platforms, you may need to:
1. Use a different database (PostgreSQL, MySQL) instead of SQLite
2. Set up environment variables for database connection
3. Use a background job service for CSV processing (e.g., Vercel Cron Jobs)

## Project Structure

```
Incentives/
├── app.py                 # Main Flask application
├── csv_processor.py       # CSV processing script
├── setup.py              # Database initialization
├── requirements.txt       # Python dependencies
├── templates/            # HTML templates
│   ├── login.html
│   ├── picker_dashboard.html
│   └── supervisor_dashboard.html
├── static/               # Static files
│   ├── css/
│   │   └── style.css
│   └── js/
│       ├── picker_dashboard.js
│       └── supervisor_dashboard.js
└── csv_uploads/          # Folder for CSV uploads
```

## Notes

- The system processes CSV files and stores data in SQLite database
- Rankings are calculated in real-time based on selected time filter
- Color coding is based on daily average (only for "Today" filter)
- All timestamps are stored in UTC

