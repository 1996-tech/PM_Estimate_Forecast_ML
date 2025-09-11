#!/usr/bin/env python
#################################################################
# Kamiak HPC WRF 2024 Complete Year Downloader
# Optimized for HPC environment with SLURM job scheduling
#
# Usage:
# 1. Copy this script to Kamiak
# 2. Submit SLURM job: sbatch wrf_download_job.sh
# 3. Monitor progress with: tail -f download_log_2024.txt
#################################################################

import sys, os
import requests
import gzip
import shutil
import zipfile
from datetime import datetime, timedelta
import json
import pandas as pd
import numpy as np
import concurrent.futures
import threading
import time
import argparse
from pathlib import Path

class KamiakWRFDownloader:
    def __init__(self, username='empactwsu', password='8mP3ctWSU#a1',
                 base_dir='/data/lab/meng/priom_zarrah/uw_rainier'):
        self.username = username
        self.password = password
        self.base_url = 'https://archive-wrf.atmos.uw.edu/data/'
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # File management
        self.progress_file = self.base_dir / 'download_progress_2024.json'
        self.log_file = self.base_dir / 'download_log_2024.txt'
        self.error_log = self.base_dir / 'download_errors_2024.txt'

        # Progress tracking
        self.successful_downloads = set()
        self.failed_downloads = []
        self.download_lock = threading.Lock()

        # Performance settings
        self.max_workers = int(os.environ.get('SLURM_CPUS_PER_TASK', 8))
        self.chunk_size = 8 * 1024 * 1024  # 8MB chunks for better network performance

        self.load_progress()
        self.setup_logging()

    def setup_logging(self):
        """Setup logging for HPC environment"""
        import logging

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

        # Log system information
        self.logger.info(f"Starting WRF downloader on Kamiak HPC")
        self.logger.info(f"Base directory: {self.base_dir}")
        self.logger.info(f"Max workers: {self.max_workers}")
        self.logger.info(f"Node: {os.environ.get('SLURMD_NODENAME', 'unknown')}")
        self.logger.info(f"Job ID: {os.environ.get('SLURM_JOB_ID', 'interactive')}")

    def load_progress(self):
        """Load previous download progress"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    self.successful_downloads = set(progress.get('successful', []))
                    self.failed_downloads = progress.get('failed', [])
                print(f"Loaded progress: {len(self.successful_downloads)} completed, {len(self.failed_downloads)} failed")
            except Exception as e:
                print(f"Could not load progress: {e}")

    def save_progress(self):
        """Save download progress atomically"""
        progress = {
            'successful': list(self.successful_downloads),
            'failed': self.failed_downloads,
            'last_updated': datetime.now().isoformat(),
            'total_files_downloaded': len(self.successful_downloads),
            'node': os.environ.get('SLURMD_NODENAME', 'unknown'),
            'job_id': os.environ.get('SLURM_JOB_ID', 'interactive')
        }

        # Atomic write
        temp_file = self.progress_file.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(progress, f, indent=2)
        temp_file.rename(self.progress_file)

    def generate_2024_file_list(self, forecast_hours=None, domains=['d4'],
                               months=None, start_date=None, end_date=None):
        """Generate complete file list for 2024"""

        if forecast_hours is None:
            # Every 6 hours for efficiency
            forecast_hours = [0, 6, 12, 18]

        if months is None:
            months = list(range(1, 13))

        # Date range
        if start_date is None:
            start_date = datetime(2024, 1, 1)
        if end_date is None:
            end_date = datetime(2024, 12, 31)

        file_list = []
        current_date = start_date

        while current_date <= end_date:
            if current_date.month in months:
                date_str = current_date.strftime('%Y%m%d00')
                year_month = current_date.strftime('%Y%m')

                for domain in domains:
                    for hour in forecast_hours:
                        filename = f"wrfout_{domain}.{date_str}.f{hour:02d}.0000.gz"
                        file_path = f"{year_month}/{date_str}/{filename}"

                        # Organize by month directories
                        local_dir = self.base_dir / year_month
                        local_dir.mkdir(exist_ok=True)

                        file_info = {
                            'path': file_path,
                            'filename': filename,
                            'local_path': local_dir / filename.replace('.gz', ''),
                            'date': current_date.strftime('%Y-%m-%d'),
                            'domain': domain,
                            'forecast_hour': hour,
                            'url': self.base_url + file_path
                        }
                        file_list.append(file_info)

            current_date += timedelta(days=1)

        return file_list

    def download_single_file(self, file_info, retry_count=3):
        """Download a single file with retry logic"""

        filename = file_info['filename']
        url = file_info['url']
        local_path = file_info['local_path']

        # Skip if already downloaded
        if local_path.exists() or filename in self.successful_downloads:
            return True, f"Already exists: {filename}"

        for attempt in range(retry_count):
            try:
                self.logger.info(f"Downloading {filename} (attempt {attempt + 1}/{retry_count})")

                response = requests.get(
                    url,
                    auth=(self.username, self.password),
                    stream=True,
                    timeout=300  # 5 minute timeout per request
                )

                if response.status_code == 200:
                    file_size = int(response.headers.get('content-length', 0))

                    # Download to temporary file first
                    temp_path = local_path.with_suffix('.tmp')

                    with open(temp_path, 'wb') as f:
                        downloaded = 0
                        start_time = time.time()

                        for chunk in response.iter_content(chunk_size=self.chunk_size):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)

                                # Log progress every 100MB
                                if downloaded % (100 * 1024 * 1024) == 0:
                                    elapsed = time.time() - start_time
                                    speed = downloaded / (1024 * 1024 * elapsed) if elapsed > 0 else 0
                                    self.logger.info(f"{filename}: {downloaded/1024/1024:.1f} MB downloaded ({speed:.1f} MB/s)")

                    # Extract the .gz file
                    if filename.endswith('.gz'):
                        with gzip.open(temp_path, 'rb') as f_in:
                            with open(local_path, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out, length=self.chunk_size)
                        temp_path.unlink()  # Remove .gz file

                        extracted_size = local_path.stat().st_size
                        self.logger.info(f"✓ {filename}: {extracted_size/1024/1024:.1f} MB extracted")
                    else:
                        temp_path.rename(local_path)

                    # Thread-safe progress update
                    with self.download_lock:
                        self.successful_downloads.add(filename)
                        if len(self.successful_downloads) % 10 == 0:
                            self.save_progress()

                    return True, f"Success: {filename}"

                elif response.status_code == 404:
                    error_msg = f"File not found: {filename}"
                    self.logger.warning(error_msg)
                    with self.download_lock:
                        self.failed_downloads.append(error_msg)
                    return False, error_msg

                else:
                    error_msg = f"HTTP {response.status_code}: {filename}"
                    self.logger.warning(error_msg)
                    if attempt == retry_count - 1:  # Last attempt
                        with self.download_lock:
                            self.failed_downloads.append(error_msg)
                    time.sleep(2 ** attempt)  # Exponential backoff

            except Exception as e:
                error_msg = f"Error downloading {filename}: {str(e)}"
                self.logger.error(error_msg)

                if attempt == retry_count - 1:  # Last attempt
                    with self.download_lock:
                        self.failed_downloads.append(error_msg)
                    return False, error_msg

                time.sleep(2 ** attempt)  # Exponential backoff

        return False, f"Failed after {retry_count} attempts: {filename}"

    def download_batch(self, file_list, max_concurrent=None):
        """Download files in parallel using ThreadPoolExecutor"""

        if max_concurrent is None:
            max_concurrent = min(self.max_workers, 4)  # Conservative for network I/O

        self.logger.info(f"Starting batch download with {max_concurrent} concurrent downloads")
        self.logger.info(f"Total files to process: {len(file_list)}")

        # Filter out already downloaded files
        remaining_files = [f for f in file_list
                          if not f['local_path'].exists()
                          and f['filename'] not in self.successful_downloads]

        self.logger.info(f"Files remaining to download: {len(remaining_files)}")

        if not remaining_files:
            self.logger.info("All files already downloaded!")
            return

        start_time = time.time()
        completed = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            # Submit all download tasks
            future_to_file = {
                executor.submit(self.download_single_file, file_info): file_info
                for file_info in remaining_files
            }

            # Process completed downloads
            for future in concurrent.futures.as_completed(future_to_file):
                file_info = future_to_file[future]
                completed += 1

                try:
                    success, message = future.result()

                    if success:
                        self.logger.info(f"[{completed}/{len(remaining_files)}] ✓ {message}")
                    else:
                        self.logger.error(f"[{completed}/{len(remaining_files)}] ✗ {message}")

                    # Progress report every 50 files
                    if completed % 50 == 0:
                        elapsed = time.time() - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        eta = (len(remaining_files) - completed) / rate if rate > 0 else 0

                        self.logger.info(f"Progress: {completed}/{len(remaining_files)} ({completed/len(remaining_files)*100:.1f}%)")
                        self.logger.info(f"Rate: {rate:.1f} files/second, ETA: {eta/3600:.1f} hours")

                        # Save progress
                        self.save_progress()

                except Exception as e:
                    self.logger.error(f"Task failed: {file_info['filename']}: {str(e)}")

        # Final save
        self.save_progress()

        total_time = time.time() - start_time
        self.logger.info(f"Batch complete: {completed} files processed in {total_time/3600:.1f} hours")

    def create_monthly_summaries(self):
        """Create summary files for each month"""
        self.logger.info("Creating monthly summaries...")

        for month_dir in self.base_dir.iterdir():
            if month_dir.is_dir() and month_dir.name.startswith('2024'):
                wrf_files = list(month_dir.glob('wrfout_*.0000'))

                if wrf_files:
                    summary = {
                        'month': month_dir.name,
                        'file_count': len(wrf_files),
                        'total_size_gb': sum(f.stat().st_size for f in wrf_files) / (1024**3),
                        'files': [f.name for f in wrf_files]
                    }

                    summary_file = month_dir / 'month_summary.json'
                    with open(summary_file, 'w') as f:
                        json.dump(summary, f, indent=2)

                    self.logger.info(f"Month {month_dir.name}: {len(wrf_files)} files, {summary['total_size_gb']:.1f} GB")

    def run_full_download(self, forecast_hours=None, months=None, max_concurrent=4):
        """Run the complete download process"""

        self.logger.info("=" * 60)
        self.logger.info("STARTING KAMIAK HPC WRF 2024 DOWNLOAD")
        self.logger.info("=" * 60)

        # Generate file list
        file_list = self.generate_2024_file_list(
            forecast_hours=forecast_hours,
            months=months
        )

        self.logger.info(f"Generated file list: {len(file_list)} files")

        # Estimate storage requirements
        estimated_size_gb = len(file_list) * 0.6  # ~600MB per file
        self.logger.info(f"Estimated storage needed: {estimated_size_gb:.1f} GB")

        # Check available space
        import shutil as disk_utils
        free_space = disk_utils.disk_usage(self.base_dir).free / (1024**3)
        self.logger.info(f"Available disk space: {free_space:.1f} GB")

        if estimated_size_gb > free_space * 0.8:  # Use only 80% of available space
            self.logger.warning("Insufficient disk space! Consider reducing scope.")

        # Start download
        start_time = time.time()
        self.download_batch(file_list, max_concurrent)

        # Create summaries
        self.create_monthly_summaries()

        # Final report
        total_time = time.time() - start_time
        self.logger.info("=" * 60)
        self.logger.info("DOWNLOAD COMPLETE")
        self.logger.info("=" * 60)
        self.logger.info(f"Total time: {total_time/3600:.1f} hours")
        self.logger.info(f"Successful downloads: {len(self.successful_downloads)}")
        self.logger.info(f"Failed downloads: {len(self.failed_downloads)}")

        # Create final summary
        final_summary = {
            'download_completed': datetime.now().isoformat(),
            'total_runtime_hours': total_time / 3600,
            'successful_downloads': len(self.successful_downloads),
            'failed_downloads': len(self.failed_downloads),
            'total_size_gb': sum(f.stat().st_size for f in self.base_dir.rglob('wrfout_*.0000')) / (1024**3) if self.base_dir.exists() else 0,
            'node': os.environ.get('SLURMD_NODENAME', 'unknown'),
            'job_id': os.environ.get('SLURM_JOB_ID', 'interactive')
        }

        with open(self.base_dir / 'final_summary.json', 'w') as f:
            json.dump(final_summary, f, indent=2)

def main():
    parser = argparse.ArgumentParser(description='Download WRF 2024 data on Kamiak HPC')
    parser.add_argument('--hours', type=str, default='0,6,12,18',
                       help='Forecast hours (comma-separated)')
    parser.add_argument('--months', type=str, default='1,2,3,4,5,6,7,8,9,10,11,12',
                       help='Months to download (comma-separated)')
    parser.add_argument('--workers', type=int, default=4,
                       help='Number of concurrent downloads')
    parser.add_argument('--base-dir', type=str, default='/data/lab/meng/priom_zarrah/uw_rainier',
                       help='Base directory for downloads')

    args = parser.parse_args()

    # Parse arguments
    forecast_hours = [int(h.strip()) for h in args.hours.split(',')]
    months = [int(m.strip()) for m in args.months.split(',')]

    print(f"Forecast hours: {forecast_hours}")
    print(f"Months: {months}")
    print(f"Workers: {args.workers}")
    print(f"Base directory: {args.base_dir}")

    # Create downloader and run
    downloader = KamiakWRFDownloader(base_dir=args.base_dir)
    downloader.run_full_download(
        forecast_hours=forecast_hours,
        months=months,
        max_concurrent=args.workers
    )

if __name__ == "__main__":
    main()
