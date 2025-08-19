# MyGAP Data Scraper API

A FastAPI-based web scraper for extracting Malaysian Good Agricultural Practice (MyGAP) certification data from the official government website with automated daily scheduling.

## Features

- 🌐 Web API for accessing MyGAP certification data
- ⏰ **Automated daily scraping at 12:00 AM (midnight)**
- 📊 Data caching (refreshes automatically after 24 hours)
- 📈 Statistics endpoint for data analysis
- 📥 JSON download functionality
- 🔍 Support for 5 data categories (TBM, PF, AM, Organic, Tanaman)
- 📝 Automatic data validation and formatting
- 🎯 Manual scraping triggers via API
- 📋 Real-time scheduler status monitoring
- 🔧 Individual scraper control

## Installation

### Prerequisites
- Python 3.7 or higher
- Internet connection for web scraping

### Step 1: Clone the repository
```bash
git clone <repository-url>
cd scrap-sayur
```

### Step 2: Set up virtual environment (recommended)
```bash
# On Windows
python -m venv myenv
myenv\Scripts\activate

# On macOS/Linux
python -m venv myenv
source myenv/bin/activate
```

### Step 3: Install dependencies
```bash
pip install -r requirement.txt
```

## Usage

### Running the API Server
```bash
python main.py
```

The server will start on `http://localhost:8000` and **automatically begin daily scraping at 12:00 AM**.

### API Endpoints

#### Data Endpoints
| Endpoint | Description |
|----------|-------------|
| `/` | API information and available endpoints |
| `/mygap/data/tbm` | Get MyGAP TBM certification data |
| `/mygap/data/pf` | Get MyGAP PF (Plant & Fresh) certification data |
| `/mygap/data/am` | Get MyGAP AM (Apiary Management) certification data |
| `/mygap/data/organic` | Get MyGAP Organic certification data |
| `/mygap/data/tanaman` | Get MyGAP Tanaman certification data |
| `/mygap/stats` | Get statistics about the data |

#### Download Endpoints
| Endpoint | Description |
|----------|-------------|
| `/mygap/download/tbm` | Download TBM data as JSON file |
| `/mygap/download/pf` | Download PF data as JSON file |
| `/mygap/download/am` | Download AM data as JSON file |
| `/mygap/download/organic` | Download Organic data as JSON file |
| `/mygap/download/tanaman` | Download Tanaman data as JSON file |


#### System Endpoints
| Endpoint | Description |
|----------|-------------|
| `/health` | Health check endpoint (includes scheduler status) |
| `/docs` | Interactive API documentation (Swagger UI) |
| `/redoc` | Alternative API documentation |

### Example Usage

**Get certification data:**
```bash
# Get TBM data
curl http://localhost:8000/mygap/data/tbm

# Get PF data
curl http://localhost:8000/mygap/data/pf

# Get AM data  
curl http://localhost:8000/mygap/data/am

# Get Organic data
curl http://localhost:8000/mygap/data/organic

# Get Tanaman data
curl http://localhost:8000/mygap/data/tanaman
```

**Get data statistics:**
```bash
curl http://localhost:8000/mygap/stats
```


**Access interactive documentation:**
Open `http://localhost:8000/docs` in your browser

## Data Fields

The scraper extracts the following fields from MyGAP certification records:

- `no_pensijilan` - Certification Number
- `kategori_pemohon` - Applicant Category
- `nama` - Name of certificate holder
- `negeri` - State
- `daerah` - District
- `jenis_tanaman` - Plant Type
- `kategori_komoditi` - Commodity Category
- `kategori_tanaman` - Plant Category
- `luas_ladang` - Farm Area (Hectares)
- `bil_haif` - Number of Hives (AM-specific)
- `tahun_pensijilan` - Certification Year
- `tarikh_pensijilan` - Certification Date
- `tempoh_sah_laku` - Expiry Date

## Data Sources

The system supports 5 different MyGAP certification categories:

1. **TBM** - Ternakan Biologi Marin (Marine Biology Farming)
2. **PF** - Plant & Fresh (Plant and Fresh Produce)
3. **AM** - Apiary Management (Beekeeping/Honey Production)
4. **Organic** - Organic Certification
5. **Tanaman** - Plant Certification (General Plant Cultivation)

## Automated Scheduling

The system includes a sophisticated scheduler that:

- **Runs automatically** when you start the API server
- **Scrapes all 5 data sources** daily at 12:00 AM (midnight)
- **Saves data** to timestamped JSON files
- **Logs all operations** with detailed information
- **Handles errors gracefully** with retry mechanisms
- **Operates in background** without blocking the API

### Scheduler Features

- ✅ Automatic daily execution
- ✅ Manual trigger capabilities
- ✅ Individual scraper control
- ✅ Real-time status monitoring
- ✅ Comprehensive error handling
- ✅ Background processing
- ✅ Detailed logging

## Project Structure

```
scrap-sayur/
├── main.py              # FastAPI application with scheduler integration
├── scheduler.py         # Automated scheduling system
├── scrap_tbm.py         # TBM (Marine Biology) data scraper
├── scrap_pf.py          # PF (Plant & Fresh) data scraper
├── scrap_am.py          # AM (Apiary Management) data scraper
├── scrap_my_organic.py  # Organic certification data scraper
├── scrap_tanaman.py     # Tanaman (Plant) data scraper
├── requirement.txt      # Python dependencies
├── README.md           # Project documentation
└── myenv/              # Virtual environment (optional)
```

## Data Caching

- The API automatically caches scraped data as JSON files
- Cache files are named with timestamps for each data source:
  - `mygap_data_tbm_YYYYMMDD_HHMMSS.json`
  - `mygap_data_pf_YYYYMMDD_HHMMSS.json`
  - `mygap_data_am_YYYYMMDD_HHMMSS.json`
  - `mygap_data_organic_YYYYMMDD_HHMMSS.json`
  - `mygap_data_tanaman_YYYYMMDD_HHMMSS.json`
- Data is automatically refreshed if cache is older than 24 hours
- Fresh data is fetched from the source website when needed
- Scheduled scraping creates new timestamped files daily

## Dependencies

- `requests` - HTTP requests for web scraping
- `beautifulsoup4` - HTML parsing and extraction
- `selenium` - Browser automation (for complex scraping)
- `lxml` - Fast XML/HTML processing
- `fastapi` - Modern web framework for APIs
- `uvicorn` - ASGI server for running FastAPI
- `pydantic` - Data validation and settings management
- `schedule` - Task scheduling capabilities
- `pyautogui` - GUI automation support

## Development

### Running in development mode
```bash
python main.py
```
The server runs with auto-reload enabled for development and starts the scheduler automatically.

### Running individual scrapers
```bash
# Run scrapers directly
python scrap_tbm.py
python scrap_pf.py
python scrap_am.py
python scrap_my_organic.py
python scrap_tanaman.py

# This will save data to timestamped files
```

### Testing the scheduler
```bash
# Test all scrapers immediately
python scheduler.py test

# Start scheduler with custom time (standalone)
python scheduler.py schedule 02:30  # 2:30 AM
```

### Manual API Testing
```bash
# Test scheduler status
curl http://localhost:8000/scheduler/status

# Trigger manual scraping
curl -X POST http://localhost:8000/scheduler/run-now

# Test individual scraper
curl -X POST http://localhost:8000/scheduler/run-single/TBM
```

## Notes

- The scraper targets the official MyGAP website: `https://carianmygapmyorganic.doa.gov.my/`
- Data is extracted in real-time from the government database
- The API includes error handling for website unavailability
- All dates and times are in ISO format for consistency
- **Automatic scheduling ensures fresh data every day at midnight**
- The scheduler runs in background and doesn't block API operations
- Each scraper can be triggered individually or collectively
- All operations are logged with detailed timestamps and performance metrics

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirement.txt
   ```

2. **Run the application:**
   ```bash
   python main.py
   ```

3. **Access the API:**
   - Main API: `http://localhost:8000`
   - Documentation: `http://localhost:8000/docs`
   - Scheduler status: `http://localhost:8000/scheduler/status`

4. **The scheduler will automatically:**
   - Start running in the background
   - Schedule daily scraping at 12:00 AM
   - Save data to timestamped JSON files
   - Log all operations

That's it! Your MyGAP data scraper with automated scheduling is now running. 🚀

