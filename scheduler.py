"""
Scheduler script to run CSV processor every 10 minutes
Run this script in the background to continuously process CSV files
"""

import time
import schedule
from csv_processor import process_new_csvs

def job():
    """Job to process CSV files"""
    print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Running CSV processor...")
    process_new_csvs()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] CSV processor finished\n")

if __name__ == '__main__':
    print("CSV Processor Scheduler Started")
    print("Processing CSV files every 10 minutes...")
    print("Press Ctrl+C to stop\n")
    
    # Schedule job every 10 minutes
    schedule.every(10).minutes.do(job)
    
    # Run immediately on start
    job()
    
    # Keep running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\nScheduler stopped by user")

