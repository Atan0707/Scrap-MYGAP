import schedule
import time
import threading
import logging
from datetime import datetime
from typing import Callable, List
import traceback

# Import our scraping functions
from scrap_tbm import extract_mygap_tbm_data
from scrap_pf import extract_mygap_pf_data
from scrap_am import extract_mygap_am_data
from scrap_my_organic import extract_mygap_organic_data
from scrap_tanaman import extract_mygap_tanaman_data

# Configure logging specifically for scheduler
scheduler_logger = logging.getLogger('scheduler')
scheduler_logger.setLevel(logging.INFO)

# Create console handler if it doesn't exist
if not scheduler_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    scheduler_logger.addHandler(handler)

class ScrapingScheduler:
    """
    A scheduler class that handles automated scraping of MyGAP data
    """
    
    def __init__(self):
        self.running = False
        self.scheduler_thread = None
        self.scraping_functions = {
            'TBM': extract_mygap_tbm_data,
            'PF': extract_mygap_pf_data,
            'AM': extract_mygap_am_data,
            'Organic': extract_mygap_organic_data,
            'Tanaman': extract_mygap_tanaman_data
        }
        
    def run_scraping_job(self, scraper_name: str, scraper_func: Callable) -> bool:
        """
        Run a single scraping job with error handling and logging
        
        Args:
            scraper_name: Name of the scraper for logging
            scraper_func: The scraping function to execute
            
        Returns:
            bool: True if successful, False if failed
        """
        try:
            scheduler_logger.info(f"Starting scheduled scraping for {scraper_name}")
            start_time = datetime.now()
            
            # Run the scraping function with save_to_file=True
            data = scraper_func(save_to_file=True)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if data is not None:
                record_count = len(data) if isinstance(data, list) else 0
                scheduler_logger.info(
                    f"Successfully completed {scraper_name} scraping: "
                    f"{record_count} records in {duration:.2f} seconds"
                )
                return True
            else:
                scheduler_logger.error(f"Failed to extract data for {scraper_name}")
                return False
                
        except Exception as e:
            scheduler_logger.error(
                f"Error during {scraper_name} scraping: {str(e)}\n{traceback.format_exc()}"
            )
            return False
    
    def run_all_scrapers(self):
        """
        Run all scraping functions sequentially
        """
        scheduler_logger.info("=" * 60)
        scheduler_logger.info("Starting scheduled scraping of all MyGAP data sources")
        scheduler_logger.info("=" * 60)
        
        overall_start_time = datetime.now()
        results = {}
        
        for scraper_name, scraper_func in self.scraping_functions.items():
            success = self.run_scraping_job(scraper_name, scraper_func)
            results[scraper_name] = success
            
            # Add a small delay between scrapers to be respectful to the website
            time.sleep(2)
        
        overall_end_time = datetime.now()
        total_duration = (overall_end_time - overall_start_time).total_seconds()
        
        # Log summary
        successful_scrapers = [name for name, success in results.items() if success]
        failed_scrapers = [name for name, success in results.items() if not success]
        
        scheduler_logger.info("=" * 60)
        scheduler_logger.info("Scheduled scraping completed")
        scheduler_logger.info(f"Total duration: {total_duration:.2f} seconds")
        scheduler_logger.info(f"Successful scrapers: {successful_scrapers}")
        if failed_scrapers:
            scheduler_logger.warning(f"Failed scrapers: {failed_scrapers}")
        scheduler_logger.info("=" * 60)
    
    def schedule_daily_scraping(self, time_str: str = "00:00"):
        """
        Schedule daily scraping at the specified time
        
        Args:
            time_str: Time in HH:MM format (24-hour format)
        """
        schedule.every().day.at(time_str).do(self.run_all_scrapers)
        scheduler_logger.info(f"Scheduled daily scraping at {time_str}")
    
    def schedule_immediate_test(self):
        """
        Schedule an immediate test run for debugging purposes
        """
        schedule.every(1).minutes.do(self.run_all_scrapers)
        scheduler_logger.info("Scheduled immediate test run (every 1 minute)")
    
    def run_scheduler(self):
        """
        Run the scheduler in a loop
        """
        scheduler_logger.info("Scheduler started. Waiting for scheduled tasks...")
        self.running = True
        
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                scheduler_logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(60)  # Wait a bit longer if there's an error
    
    def start_background_scheduler(self, time_str: str = "00:00"):
        """
        Start the scheduler in a background thread
        
        Args:
            time_str: Time in HH:MM format for daily scraping
        """
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            scheduler_logger.warning("Scheduler is already running")
            return
        
        # Schedule the daily scraping
        self.schedule_daily_scraping(time_str)
        
        # Start scheduler in background thread
        self.scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        scheduler_logger.info(f"Background scheduler started for daily scraping at {time_str}")
    
    def stop_scheduler(self):
        """
        Stop the scheduler
        """
        self.running = False
        if self.scheduler_thread:
            scheduler_logger.info("Stopping scheduler...")
            # Clear all scheduled jobs
            schedule.clear()
            # Note: Thread will stop on next iteration of run_scheduler loop
    
    def get_next_run_time(self):
        """
        Get the next scheduled run time
        
        Returns:
            str: Next run time as a string, or None if no jobs scheduled
        """
        jobs = schedule.get_jobs()
        if jobs:
            next_run = min(job.next_run for job in jobs)
            return next_run.strftime("%Y-%m-%d %H:%M:%S")
        return None
    
    def run_single_scraper(self, scraper_name: str) -> bool:
        """
        Run a single scraper manually
        
        Args:
            scraper_name: Name of the scraper ('TBM', 'PF', 'AM', 'Organic', 'Tanaman')
            
        Returns:
            bool: True if successful, False if failed
        """
        if scraper_name not in self.scraping_functions:
            scheduler_logger.error(f"Unknown scraper: {scraper_name}")
            return False
        
        return self.run_scraping_job(scraper_name, self.scraping_functions[scraper_name])


# Global scheduler instance
scraping_scheduler = ScrapingScheduler()


def start_scheduler(time_str: str = "00:00"):
    """
    Convenience function to start the scheduler
    
    Args:
        time_str: Time in HH:MM format for daily scraping (default: midnight)
    """
    scraping_scheduler.start_background_scheduler(time_str)


def stop_scheduler():
    """
    Convenience function to stop the scheduler
    """
    scraping_scheduler.stop_scheduler()


def run_manual_scraping():
    """
    Convenience function to run scraping manually
    """
    scraping_scheduler.run_all_scrapers()


def get_scheduler_status():
    """
    Get current scheduler status
    
    Returns:
        dict: Scheduler status information
    """
    return {
        "running": scraping_scheduler.running,
        "next_run": scraping_scheduler.get_next_run_time(),
        "available_scrapers": list(scraping_scheduler.scraping_functions.keys())
    }


if __name__ == "__main__":
    """
    Script can be run standalone for testing
    """
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            # Run immediate test
            print("Running immediate test of all scrapers...")
            run_manual_scraping()
        elif sys.argv[1] == "schedule":
            # Start scheduler with custom time if provided
            time_arg = sys.argv[2] if len(sys.argv) > 2 else "00:00"
            print(f"Starting scheduler for daily runs at {time_arg}")
            start_scheduler(time_arg)
            
            # Keep the script running
            try:
                while True:
                    time.sleep(60)
            except KeyboardInterrupt:
                print("\nStopping scheduler...")
                stop_scheduler()
    else:
        print("Usage:")
        print("  python scheduler.py test           # Run immediate test")
        print("  python scheduler.py schedule [HH:MM] # Start scheduler (default: 00:00)")
        print("Example:")
        print("  python scheduler.py schedule 02:30  # Schedule for 2:30 AM")
