from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import json
from datetime import datetime, timedelta
import logging
import os
import glob

# Import our scraping functions
from scrap_tbm import extract_mygap_tbm_data, DATA_FIELDS as TBM_DATA_FIELDS
from scrap_pf import extract_mygap_pf_data, DATA_FIELDS as PF_DATA_FIELDS
from scrap_am import extract_mygap_am_data, DATA_FIELDS as AM_DATA_FIELDS
from scrap_my_organic import extract_mygap_organic_data, DATA_FIELDS as ORGANIC_DATA_FIELDS
from scrap_tanaman import extract_mygap_tanaman_data, DATA_FIELDS as TANAMAN_DATA_FIELDS

# Import scheduler
from scheduler import (
    start_scheduler, 
    stop_scheduler, 
    run_manual_scraping, 
    get_scheduler_status, 
    scraping_scheduler
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="MyGAP Data Scraper API",
    description="API to fetch Malaysian Good Agricultural Practice (MyGAP) certification data with automated scheduling",
    version="1.0.0"
)

# Application startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Start the scheduler when the application starts"""
    logger.info("Starting MyGAP Data Scraper API...")
    # Start the scheduler for daily scraping at midnight (00:00)
    start_scheduler("00:00")
    logger.info("Automated daily scraping scheduler started at 00:00 (midnight)")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop the scheduler when the application shuts down"""
    logger.info("Shutting down MyGAP Data Scraper API...")
    stop_scheduler()
    logger.info("Scheduler stopped")

# Specific Pydantic models for each scraper type
class TBMRecord(BaseModel):
    no_pensijilan: Optional[str] = None          # Certification Number
    kategori_pemohon: Optional[str] = None       # Applicant Category
    nama: Optional[str] = None                   # Name
    negeri: Optional[str] = None                 # State
    daerah: Optional[str] = None                 # District
    jenis_tanaman: Optional[str] = None          # Plant Type
    kategori_komoditi: Optional[str] = None      # Commodity Category
    kategori_tanaman: Optional[str] = None       # Plant Category
    luas_ladang: Optional[str] = None            # Farm Area
    tahun_pensijilan: Optional[str] = None       # Certification Year
    tarikh_pensijilan: Optional[str] = None      # Certification Date
    tempoh_sah_laku: Optional[str] = None        # Validity Period/Expiry Date

class PFRecord(BaseModel):
    no_pensijilan: Optional[str] = None          # Certification Number
    kategori_pemohon: Optional[str] = None       # Applicant Category
    nama: Optional[str] = None                   # Name
    negeri: Optional[str] = None                 # State
    daerah: Optional[str] = None                 # District
    jenis_tanaman: Optional[str] = None          # Plant Type
    kategori_komoditi: Optional[str] = None      # Commodity Category
    kategori_tanaman: Optional[str] = None       # Plant Category
    luas_ladang: Optional[str] = None            # Farm Area
    tahun_pensijilan: Optional[str] = None       # Certification Year
    tarikh_pensijilan: Optional[str] = None      # Certification Date
    tempoh_sah_laku: Optional[str] = None        # Validity Period/Expiry Date

class AMRecord(BaseModel):
    no_pensijilan: Optional[str] = None          # Certification Number
    kategori_pemohon: Optional[str] = None       # Applicant Category
    nama: Optional[str] = None                   # Name
    negeri: Optional[str] = None                 # State
    daerah: Optional[str] = None                 # District
    jenis_tanaman: Optional[str] = None          # Plant Type
    kategori_komoditi: Optional[str] = None      # Commodity Category
    kategori_tanaman: Optional[str] = None       # Plant Category
    bil_haif: Optional[str] = None               # Number of Hives (AM-specific)
    luas_ladang: Optional[str] = None            # Farm Area
    tahun_pensijilan: Optional[str] = None       # Certification Year
    tarikh_pensijilan: Optional[str] = None      # Certification Date
    tempoh_sah_laku: Optional[str] = None        # Validity Period/Expiry Date

class OrganicRecord(BaseModel):
    no_pensijilan: Optional[str] = None          # Certification Number
    kategori_pemohon: Optional[str] = None       # Applicant Category
    nama: Optional[str] = None                   # Name
    negeri: Optional[str] = None                 # State
    daerah: Optional[str] = None                 # District
    jenis_tanaman: Optional[str] = None          # Plant Type
    kategori_komoditi: Optional[str] = None      # Commodity Category
    kategori_tanaman: Optional[str] = None       # Plant Category
    luas_ladang: Optional[str] = None            # Farm Area
    tahun_pensijilan: Optional[str] = None       # Certification Year
    tarikh_pensijilan: Optional[str] = None      # Certification Date
    tempoh_sah_laku: Optional[str] = None        # Validity Period/Expiry Date

class TanamanRecord(BaseModel):
    no_pensijilan: Optional[str] = None          # Certification Number
    kategori_pemohon: Optional[str] = None       # Applicant Category
    nama: Optional[str] = None                   # Name
    negeri: Optional[str] = None                 # State
    daerah: Optional[str] = None                 # District
    jenis_tanaman: Optional[str] = None          # Plant Type
    kategori_komoditi: Optional[str] = None      # Commodity Category
    kategori_tanaman: Optional[str] = None       # Plant Category
    luas_ladang: Optional[str] = None            # Farm Area
    tahun_pensijilan: Optional[str] = None       # Certification Year
    tarikh_pensijilan: Optional[str] = None      # Certification Date
    tempoh_sah_laku: Optional[str] = None        # Validity Period/Expiry Date

# Response models for each type
class TBMResponse(BaseModel):
    success: bool
    message: str
    total_records: int
    timestamp: str
    data: List[TBMRecord]

class PFResponse(BaseModel):
    success: bool
    message: str
    total_records: int
    timestamp: str
    data: List[PFRecord]

class AMResponse(BaseModel):
    success: bool
    message: str
    total_records: int
    timestamp: str
    data: List[AMRecord]

class OrganicResponse(BaseModel):
    success: bool
    message: str
    total_records: int
    timestamp: str
    data: List[OrganicRecord]

class TanamanResponse(BaseModel):
    success: bool
    message: str
    total_records: int
    timestamp: str
    data: List[TanamanRecord]

class FieldStats(BaseModel):
    field_name: str
    completed_count: int
    total_count: int
    completion_percentage: float

class StatsResponse(BaseModel):
    success: bool
    message: str
    total_records: int
    timestamp: str
    field_statistics: List[FieldStats]

class SchedulerStatusResponse(BaseModel):
    success: bool
    message: str
    timestamp: str
    scheduler_running: bool
    next_run_time: Optional[str] = None
    available_scrapers: List[str]

class ManualScrapingResponse(BaseModel):
    success: bool
    message: str
    timestamp: str
    started_at: str

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "MyGAP Data Scraper API with Automated Scheduling",
        "version": "1.0.0",
        "scheduler_info": "Automatically scrapes all data sources daily at midnight",
        "endpoints": {
            "/mygap/data/tbm": "Fetch MyGAP TBM certification data",
            "/mygap/data/pf": "Fetch MyGAP Plant & Fresh certification data",
            "/mygap/data/am": "Fetch MyGAP Apiary Management certification data",
            "/mygap/data/organic": "Fetch MyGAP Organic certification data",
            "/mygap/data/tanaman": "Fetch MyGAP Tanaman certification data",
            "/mygap/stats": "Get statistics about the data",
            # "/scheduler/status": "Get scheduler status and next run time",
            # "/scheduler/run-now": "Manually trigger scraping of all sources",
            # "/scheduler/run-single/{scraper_name}": "Manually trigger single scraper",
            "/health": "Health check endpoint",
            "/docs": "API documentation (Swagger UI)",
            "/redoc": "API documentation (ReDoc)"
        }
    }

@app.get("/mygap/data/tbm", response_model=TBMResponse)
async def get_mygap_data():
    """
    Fetch MyGAP certification data - reads from JSON file first, 
    only fetches new data if file is older than 1 day
    
    Returns:
        MyGAPResponse: Complete dataset with all certification records
    """
    try:
        # First try to read from existing JSON file
        raw_data = None
        data_source = "cache"
        
        # Find the most recent JSON file
        json_files = glob.glob("mygap_data_tbm*.json")
        if json_files:
            # Sort by modification time, get the newest
            latest_file = max(json_files, key=os.path.getmtime)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            file_age = datetime.now() - file_mtime
            
            logger.info(f"Found existing file: {latest_file}, age: {file_age}")
            
            # If file is less than 1 day old, read from it
            if file_age < timedelta(days=1):
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if isinstance(file_data, list):
                            raw_data = file_data
                        elif isinstance(file_data, dict) and 'data' in file_data:
                            raw_data = file_data['data']
                        else:
                            raw_data = file_data
                    logger.info(f"Successfully loaded {len(raw_data) if raw_data else 0} records from cache")
                except Exception as e:
                    logger.warning(f"Failed to read from cache file: {str(e)}")
                    raw_data = None
            else:
                logger.info(f"File is older than 1 day ({file_age}), fetching fresh data")
        
        # If no valid cached data, extract from website
        if raw_data is None:
            logger.info("Fetching fresh data from MyGAP website...")
            raw_data = extract_mygap_tbm_data(save_to_file=True)  # Save fresh data to file
            data_source = "fresh"
            
            if raw_data is None:
                logger.error("Failed to extract data from MyGAP website")
                raise HTTPException(
                    status_code=500, 
                    detail="Failed to extract data from MyGAP website. The website might be unavailable."
                )
        
        # Convert raw data to Pydantic models
        records = []
        for item in raw_data:
            # Create record with only fields that exist in TBMRecord
            cleaned_item = {k: v for k, v in item.items() if k in TBMRecord.model_fields}
            record = TBMRecord(**cleaned_item)
            records.append(record)
        
        message = f"Successfully loaded {len(records)} MyGAP certification records from {data_source}"
        response = TBMResponse(
            success=True,
            message=message,
            total_records=len(records),
            timestamp=datetime.now().isoformat(),
            data=records
        )
        
        logger.info(message)
        return response
        
    except Exception as e:
        logger.error(f"Error loading MyGAP data: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )
    
@app.get("/mygap/data/pf", response_model=PFResponse)
async def get_mygap_pf_data():
    """
    Fetch MyGAP PF (Plantation Forestry) certification data - reads from JSON file first, 
    only fetches new data if file is older than 1 day
    
    Returns:
        MyGAPResponse: Complete dataset with all certification records
    """
    try:
        # First try to read from existing JSON file
        raw_data = None
        data_source = "cache"
        
        # Find the most recent JSON file
        json_files = glob.glob("mygap_data_pf*.json")
        if json_files:
            # Sort by modification time, get the newest
            latest_file = max(json_files, key=os.path.getmtime)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            file_age = datetime.now() - file_mtime
            
            logger.info(f"Found existing file: {latest_file}, age: {file_age}")
            
            # If file is less than 1 day old, read from it
            if file_age < timedelta(days=1):
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if isinstance(file_data, list):
                            raw_data = file_data
                        elif isinstance(file_data, dict) and 'data' in file_data:
                            raw_data = file_data['data']
                        else:
                            raw_data = file_data
                    logger.info(f"Successfully loaded {len(raw_data) if raw_data else 0} records from cache")
                except Exception as e:
                    logger.warning(f"Failed to read from cache file: {str(e)}")
                    raw_data = None
            else:
                logger.info(f"File is older than 1 day ({file_age}), fetching fresh data")
        
        # If no valid cached data, extract from website
        if raw_data is None:
            logger.info("Fetching fresh data from MyGAP website...")
            raw_data = extract_mygap_pf_data(save_to_file=True)  # Save fresh data to file
            data_source = "fresh"
            
            if raw_data is None:
                logger.error("Failed to extract data from MyGAP website")
                raise HTTPException(
                    status_code=500, 
                    detail="Failed to extract data from MyGAP website. The website might be unavailable."
                )
        
        # Convert raw data to Pydantic models
        records = []
        for item in raw_data:
            # Create record with only fields that exist in PFRecord
            cleaned_item = {k: v for k, v in item.items() if k in PFRecord.model_fields}
            record = PFRecord(**cleaned_item)
            records.append(record)
        
        message = f"Successfully loaded {len(records)} MyGAP certification records from {data_source}"
        response = PFResponse(
            success=True,
            message=message,
            total_records=len(records),
            timestamp=datetime.now().isoformat(),
            data=records
        )
        
        logger.info(message)
        return response
        
    except Exception as e:
        logger.error(f"Error loading MyGAP data: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/data/am", response_model=AMResponse)
async def get_mygap_am_data():
    """
    Fetch MyGAP AM (Apiary Management) certification data - reads from JSON file first, 
    only fetches new data if file is older than 1 day
    
    Returns:
        MyGAPResponse: Complete dataset with all AM certification records
    """
    try:
        # First try to read from existing JSON file
        raw_data = None
        data_source = "cache"
        
        # Find the most recent AM JSON file
        json_files = glob.glob("mygap_data_am_*.json")
        if json_files:
            # Sort by modification time, get the newest
            latest_file = max(json_files, key=os.path.getmtime)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            file_age = datetime.now() - file_mtime
            
            logger.info(f"Found existing AM file: {latest_file}, age: {file_age}")
            
            # If file is less than 1 day old, read from it
            if file_age < timedelta(days=1):
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if isinstance(file_data, list):
                            raw_data = file_data
                        elif isinstance(file_data, dict) and 'data' in file_data:
                            raw_data = file_data['data']
                        else:
                            raw_data = file_data
                    logger.info(f"Successfully loaded {len(raw_data) if raw_data else 0} AM records from cache")
                except Exception as e:
                    logger.warning(f"Failed to read from AM cache file: {str(e)}")
                    raw_data = None
            else:
                logger.info(f"AM file is older than 1 day ({file_age}), fetching fresh data")
        
        # If no valid cached data, extract from website
        if raw_data is None:
            logger.info("Fetching fresh AM data from MyGAP website...")
            raw_data = extract_mygap_am_data(save_to_file=True)  # Save fresh data to file
            data_source = "fresh"
            
            if raw_data is None:
                logger.error("Failed to extract AM data from MyGAP website")
                raise HTTPException(
                    status_code=500, 
                    detail="Failed to extract AM data from MyGAP website. The website might be unavailable."
                )
        
        # Convert raw data to Pydantic models
        records = []
        for item in raw_data:
            # Create record with only fields that exist in AMRecord
            cleaned_item = {k: v for k, v in item.items() if k in AMRecord.model_fields}
            record = AMRecord(**cleaned_item)
            records.append(record)
        
        message = f"Successfully loaded {len(records)} MyGAP AM certification records from {data_source}"
        response = AMResponse(
            success=True,
            message=message,
            total_records=len(records),
            timestamp=datetime.now().isoformat(),
            data=records
        )
        
        logger.info(message)
        return response
        
    except Exception as e:
        logger.error(f"Error loading MyGAP AM data: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/data/organic", response_model=OrganicResponse)
async def get_mygap_organic_data():
    """
    Fetch MyGAP Organic certification data - reads from JSON file first, 
    only fetches new data if file is older than 1 day
    
    Returns:
        MyGAPResponse: Complete dataset with all Organic certification records
    """
    try:
        # First try to read from existing JSON file
        raw_data = None
        data_source = "cache"
        
        # Find the most recent Organic JSON file
        json_files = glob.glob("mygap_data_organic_*.json")
        if json_files:
            # Sort by modification time, get the newest
            latest_file = max(json_files, key=os.path.getmtime)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            file_age = datetime.now() - file_mtime
            
            logger.info(f"Found existing Organic file: {latest_file}, age: {file_age}")
            
            # If file is less than 1 day old, read from it
            if file_age < timedelta(days=1):
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if isinstance(file_data, list):
                            raw_data = file_data
                        elif isinstance(file_data, dict) and 'data' in file_data:
                            raw_data = file_data['data']
                        else:
                            raw_data = file_data
                    logger.info(f"Successfully loaded {len(raw_data) if raw_data else 0} Organic records from cache")
                except Exception as e:
                    logger.warning(f"Failed to read from Organic cache file: {str(e)}")
                    raw_data = None
            else:
                logger.info(f"Organic file is older than 1 day ({file_age}), fetching fresh data")
        
        # If no valid cached data, extract from website
        if raw_data is None:
            logger.info("Fetching fresh Organic data from MyGAP website...")
            raw_data = extract_mygap_organic_data(save_to_file=True)  # Save fresh data to file
            data_source = "fresh"
            
            if raw_data is None:
                logger.error("Failed to extract Organic data from MyGAP website")
                raise HTTPException(
                    status_code=500, 
                    detail="Failed to extract Organic data from MyGAP website. The website might be unavailable."
                )
        
        # Convert raw data to Pydantic models
        records = []
        for item in raw_data:
            # Create record with only fields that exist in OrganicRecord
            cleaned_item = {k: v for k, v in item.items() if k in OrganicRecord.model_fields}
            record = OrganicRecord(**cleaned_item)
            records.append(record)
        
        message = f"Successfully loaded {len(records)} MyGAP Organic certification records from {data_source}"
        response = OrganicResponse(
            success=True,
            message=message,
            total_records=len(records),
            timestamp=datetime.now().isoformat(),
            data=records
        )
        
        logger.info(message)
        return response
        
    except Exception as e:
        logger.error(f"Error loading MyGAP Organic data: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/data/tanaman", response_model=TanamanResponse)
async def get_mygap_tanaman_data():
    """
    Fetch MyGAP Tanaman certification data - reads from JSON file first, 
    only fetches new data if file is older than 1 day
    
    Returns:
        MyGAPResponse: Complete dataset with all Tanaman certification records
    """
    try:
        # First try to read from existing JSON file
        raw_data = None
        data_source = "cache"
        
        # Find the most recent Tanaman JSON file
        json_files = glob.glob("mygap_data_tanaman_*.json")
        if json_files:
            # Sort by modification time, get the newest
            latest_file = max(json_files, key=os.path.getmtime)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            file_age = datetime.now() - file_mtime
            
            logger.info(f"Found existing Tanaman file: {latest_file}, age: {file_age}")
            
            # If file is less than 1 day old, read from it
            if file_age < timedelta(days=1):
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if isinstance(file_data, list):
                            raw_data = file_data
                        elif isinstance(file_data, dict) and 'data' in file_data:
                            raw_data = file_data['data']
                        else:
                            raw_data = file_data
                    logger.info(f"Successfully loaded {len(raw_data) if raw_data else 0} Tanaman records from cache")
                except Exception as e:
                    logger.warning(f"Failed to read from Tanaman cache file: {str(e)}")
                    raw_data = None
            else:
                logger.info(f"Tanaman file is older than 1 day ({file_age}), fetching fresh data")
        
        # If no valid cached data, extract from website
        if raw_data is None:
            logger.info("Fetching fresh Tanaman data from MyGAP website...")
            raw_data = extract_mygap_tanaman_data(save_to_file=True)  # Save fresh data to file
            data_source = "fresh"
            
            if raw_data is None:
                logger.error("Failed to extract Tanaman data from MyGAP website")
                raise HTTPException(
                    status_code=500, 
                    detail="Failed to extract Tanaman data from MyGAP website. The website might be unavailable."
                )
        
        # Convert raw data to Pydantic models
        records = []
        for item in raw_data:
            # Create record with only fields that exist in TanamanRecord
            cleaned_item = {k: v for k, v in item.items() if k in TanamanRecord.model_fields}
            record = TanamanRecord(**cleaned_item)
            records.append(record)
        
        message = f"Successfully loaded {len(records)} MyGAP Tanaman certification records from {data_source}"
        response = TanamanResponse(
            success=True,
            message=message,
            total_records=len(records),
            timestamp=datetime.now().isoformat(),
            data=records
        )
        
        logger.info(message)
        return response
        
    except Exception as e:
        logger.error(f"Error loading MyGAP Tanaman data: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/stats", response_model=StatsResponse)
async def get_mygap_stats():
    """
    Get statistics about the MyGAP data including field completion rates
    
    Returns:
        StatsResponse: Statistics about data completeness and field completion rates
    """
    try:
        logger.info("Extracting MyGAP data for statistics...")
        
        # Extract data using our scraping function
        raw_data = extract_mygap_tbm_data(save_to_file=False)
        
        if raw_data is None:
            logger.error("Failed to extract data from MyGAP website")
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website. The website might be unavailable."
            )
        
        # Calculate field statistics
        field_stats = []
        total_records = len(raw_data)
        
        # Use the output field names (kategori_pemohon instead of projek)
        for field in PF_DATA_FIELDS:
            if field == 'projek':
                output_field = 'kategori_pemohon'
            else:
                output_field = field
            completed_count = sum(1 for record in raw_data if record.get(output_field, '').strip())
            completion_percentage = (completed_count / total_records * 100) if total_records > 0 else 0
            
            stat = FieldStats(
                field_name=output_field,
                completed_count=completed_count,
                total_count=total_records,
                completion_percentage=round(completion_percentage, 1)
            )
            field_stats.append(stat)
        
        response = StatsResponse(
            success=True,
            message=f"Statistics for {total_records} MyGAP certification records",
            total_records=total_records,
            timestamp=datetime.now().isoformat(),
            field_statistics=field_stats
        )
        
        logger.info(f"Generated statistics for {total_records} records")
        return response
        
    except Exception as e:
        logger.error(f"Error generating MyGAP statistics: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/download/tbm")
async def download_json():
    """
    Download MyGAP data as JSON file
    
    Returns:
        JSONResponse: Raw JSON data for download
    """
    try:
        logger.info("Preparing JSON download...")
        
        # Extract data
        raw_data = extract_mygap_tbm_data(save_to_file=False)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website"
            )
        
        # Prepare download response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mygap_data_{timestamp}.json"
        
        return JSONResponse(
            content={
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "total_records": len(raw_data),
                    "fields": [field if field != 'projek' else 'kategori_pemohon' for field in PF_DATA_FIELDS]
                },
                "data": raw_data
            },
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/json"
            }
        )
        
    except Exception as e:
        logger.error(f"Error preparing JSON download: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/download/am")
async def download_json():
    """
    Download MyGAP data as JSON file
    
    Returns:
        JSONResponse: Raw JSON data for download
    """
    try:
        logger.info("Preparing JSON download...")
        
        # Extract data
        raw_data = extract_mygap_am_data(save_to_file=False)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website"
            )
        
        # Prepare download response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mygap_data_{timestamp}.json"
        
        # Create the actual field names used in the AM output data
        am_output_fields = []
        for field in AM_DATA_FIELDS:
            if field == 'projek':
                am_output_fields.append('kategori_pemohon')
            else:
                am_output_fields.append(field)
        
        return JSONResponse(
            content={
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "total_records": len(raw_data),
                    "fields": am_output_fields
                },
                "data": raw_data
            },
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/json"
            }
        )
        
    except Exception as e:
        logger.error(f"Error preparing JSON download: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/download/organic")
async def download_json():
    """
    Download MyGAP data as JSON file
    
    Returns:
        JSONResponse: Raw JSON data for download
    """
    try:
        logger.info("Preparing JSON download...")
        
        # Extract data
        raw_data = extract_mygap_organic_data(save_to_file=False)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website"
            )
        
        # Prepare download response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mygap_data_{timestamp}.json"
        
        return JSONResponse(
            content={
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "total_records": len(raw_data),
                    "fields": [field if field != 'projek' else 'kategori_pemohon' for field in PF_DATA_FIELDS]
                },
                "data": raw_data
            },
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/json"
            }
        )
        
    except Exception as e:
        logger.error(f"Error preparing JSON download: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/download/tanaman")
async def download_json():
    """
    Download MyGAP data as JSON file
    
    Returns:
        JSONResponse: Raw JSON data for download
    """
    try:
        logger.info("Preparing JSON download...")
        
        # Extract data
        raw_data = extract_mygap_tanaman_data(save_to_file=False)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website"
            )
        
        # Prepare download response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mygap_data_{timestamp}.json"
        
        return JSONResponse(
            content={
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "total_records": len(raw_data),
                    "fields": [field if field != 'projek' else 'kategori_pemohon' for field in PF_DATA_FIELDS]
                },
                "data": raw_data
            },
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/json"
            }
        )
        
    except Exception as e:
        logger.error(f"Error preparing JSON download: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/download/pf")
async def download_json():
    """
    Download MyGAP data as JSON file
    
    Returns:
        JSONResponse: Raw JSON data for download
    """
    try:
        logger.info("Preparing JSON download...")
        
        # Extract data
        raw_data = extract_mygap_pf_data(save_to_file=False)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website"
            )
        
        # Prepare download response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mygap_data_{timestamp}.json"
        
        return JSONResponse(
            content={
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "total_records": len(raw_data),
                    "fields": [field if field != 'projek' else 'kategori_pemohon' for field in PF_DATA_FIELDS]
                },
                "data": raw_data
            },
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/json"
            }
        )
        
    except Exception as e:
        logger.error(f"Error preparing JSON download: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/scheduler/status", response_model=SchedulerStatusResponse)
async def get_scheduler_status():
    """
    Get the current status of the scheduler
    
    Returns:
        SchedulerStatusResponse: Current scheduler status and next run time
    """
    try:
        status = get_scheduler_status()
        
        return SchedulerStatusResponse(
            success=True,
            message="Scheduler status retrieved successfully",
            timestamp=datetime.now().isoformat(),
            scheduler_running=status["running"],
            next_run_time=status["next_run"],
            available_scrapers=status["available_scrapers"]
        )
    except Exception as e:
        logger.error(f"Error getting scheduler status: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.post("/scheduler/run-now", response_model=ManualScrapingResponse)
async def run_manual_scraping_endpoint():
    """
    Manually trigger scraping of all data sources
    
    Returns:
        ManualScrapingResponse: Confirmation that manual scraping has started
    """
    try:
        # Start manual scraping in background thread to avoid blocking
        import threading
        
        start_time = datetime.now()
        
        def run_scraping():
            run_manual_scraping()
        
        thread = threading.Thread(target=run_scraping, daemon=True)
        thread.start()
        
        return ManualScrapingResponse(
            success=True,
            message="Manual scraping of all data sources has been started in background",
            timestamp=datetime.now().isoformat(),
            started_at=start_time.isoformat()
        )
    except Exception as e:
        logger.error(f"Error starting manual scraping: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.post("/scheduler/run-single/{scraper_name}")
async def run_single_scraper_endpoint(scraper_name: str):
    """
    Manually trigger a single scraper
    
    Args:
        scraper_name: Name of the scraper ('TBM', 'PF', 'AM', 'Organic', 'Tanaman')
    
    Returns:
        dict: Result of the scraping operation
    """
    try:
        # Validate scraper name
        valid_scrapers = ['TBM', 'PF', 'AM', 'Organic', 'Tanaman']
        if scraper_name not in valid_scrapers:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid scraper name. Must be one of: {valid_scrapers}"
            )
        
        start_time = datetime.now()
        
        # Run single scraper in background thread
        def run_scraper():
            return scraping_scheduler.run_single_scraper(scraper_name)
        
        import threading
        thread = threading.Thread(target=run_scraper, daemon=True)
        thread.start()
        
        return {
            "success": True,
            "message": f"Manual scraping of {scraper_name} data source has been started in background",
            "timestamp": datetime.now().isoformat(),
            "started_at": start_time.isoformat(),
            "scraper": scraper_name
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting {scraper_name} scraper: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    scheduler_status = get_scheduler_status()
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "MyGAP Data Scraper API",
        "scheduler": {
            "running": scheduler_status["running"],
            "next_run": scheduler_status["next_run"]
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
