# TruePeopleSearch Scraper

A robust web scraping tool designed to extract contact information from TruePeopleSearch.com. This tool handles proxies, captchas, and saves data to a SQLite database for further processing.

## Features

- **Web Scraping**: Extracts contact information from TruePeopleSearch.com
- **Proxy Support**: Rotates through a list of proxies to avoid IP blocking
- **Captcha Handling**: Automatically detects and solves various types of captchas
- **Data Persistence**: Saves scraped data to a SQLite database
- **Progress Tracking**: Resumes from the last processed row in case of interruptions
- **Logging**: Comprehensive logging for debugging and monitoring
- **Export Capability**: Exports scraped data to CSV format

## Project Structure

```
.
├── cap.py                      # Main scraping script with captcha handling
├── main.py                     # Alternative entry point with similar functionality
├── requirements.txt            # Python package dependencies
├── tps_data.db                 # SQLite database for storing scraped data
├── proxies.txt                 # List of proxies (one per line)
├── scraper.log                 # Log file with execution details
├── downloaded_files/           # Directory for downloaded files
├── scraped_texts/              # Directory for storing scraped text data
└── README.md                   # This file
```

## Prerequisites

- Python 3.7+
- Chrome or Firefox browser
- Internet connection

## Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd cap
   ```

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   .\venv\Scripts\activate  # On Windows
   source venv/bin/activate  # On macOS/Linux
   ```

3. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

1. Prepare your input CSV file with the following columns:
   - `name`: Full name of the person to search for
   - `address`: Address information for the search

2. Add your proxies to `proxies.txt` (one proxy per line in the format `ip:port` or `username:password@ip:port`)

## Usage

1. Place your input CSV file in the project directory
2. Run the scraper:
   ```bash
   python cap.py
   ```
   or
   ```bash
   python main.py
   ```

3. The script will:
   - Process each row in the input CSV
   - Search for matching profiles on TruePeopleSearch
   - Extract contact information (phone numbers, emails)
   - Save results to the SQLite database (`tps_data.db`)
   - Generate a CSV export of the results

## Database Schema

### scraped_data
- `id`: Primary key
- `input_row_id`: Original row ID from input CSV
- `tps_verified_name`: Verified name from TruePeopleSearch
- `tps_address`: Address from TruePeopleSearch
- `phone1`-`phone4`: Up to 4 phone numbers
- `email1`-`email3`: Up to 3 email addresses
- `remarks`: Any additional notes
- `used_proxy`: Proxy used for the request

### blocked_proxies
- `proxy`: Proxy address
- `blocked_time`: Timestamp when the proxy was blocked

### scraping_progress
- `id`: Primary key
- `last_processed_row`: Last processed row number
- `input_file`: Name of the input file
- `timestamp`: When the progress was recorded

## Troubleshooting

### Common Issues

1. **Proxy Blocked**:
   - The script automatically detects and skips blocked proxies
   - Check `scraper.log` for details

2. **Captcha Not Solved**:
   - Ensure you have a valid proxy list
   - The script includes basic captcha solving, but manual intervention may be needed

3. **Slow Performance**:
   - Increase delay between requests in the code
   - Use higher quality proxies

### Logs
Check `scraper.log` for detailed execution logs and error messages.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Disclaimer

This tool is for educational purposes only. Ensure you comply with the terms of service of any website you scrape and respect robots.txt files. The developers are not responsible for any misuse of this tool.
