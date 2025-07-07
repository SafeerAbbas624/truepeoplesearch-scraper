from seleniumbase import SB
import pandas as pd
import random
import sqlite3
import time
import os
from bs4 import BeautifulSoup
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create or connect to SQLite database
def setup_database():
    conn = sqlite3.connect('tps_data.db')
    cursor = conn.cursor()
    
    # Create tables if they don't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS scraped_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        input_row_id INTEGER,
        tps_verified_name TEXT,
        tps_address TEXT,
        phone1 TEXT,
        phone2 TEXT,
        phone3 TEXT,
        phone4 TEXT,
        email1 TEXT,
        email2 TEXT,
        email3 TEXT,
        remarks TEXT,
        used_proxy TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS blocked_proxies (
        proxy TEXT PRIMARY KEY,
        blocked_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create a new table to track progress
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS scraping_progress (
        id INTEGER PRIMARY KEY,
        last_processed_row INTEGER,
        input_file TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    return conn

def get_last_processed_row(conn, input_file):
    cursor = conn.cursor()
    cursor.execute("SELECT last_processed_row FROM scraping_progress WHERE input_file = ? ORDER BY timestamp DESC LIMIT 1", (input_file,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return -1  # Return -1 if no previous progress found

def update_progress(conn, row_index, input_file):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO scraping_progress (last_processed_row, input_file) VALUES (?, ?)", 
                  (row_index, input_file))
    conn.commit()

def address_to_url_conv(name, address):
    return f'https://www.truepeoplesearch.com/results?name={name.replace(" ", "%20")}&citystatezip={address.replace(" ", "%20")}'

def is_proxy_blocked(proxy, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM blocked_proxies WHERE proxy = ?", (proxy,))
    return cursor.fetchone() is not None

def add_blocked_proxy(proxy, conn):
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO blocked_proxies (proxy) VALUES (?)", (proxy,))
    conn.commit()
    logger.warning(f"Proxy {proxy} marked as blocked")

def detect_if_blocked(sb):
    try:
        # Get the page source
        page_source = sb.get_page_source()
        
        # Parse the page source with BeautifulSoup
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Check for common block indicators in the parsed HTML
        if soup.find(string="Access Denied") or soup.find(string="Sorry, you have been blocked"):
            logger.warning("Proxy is blocked: Access Denied or Sorry message found.")
            return True
        
        # Check for "This site can't be reached" message
        if soup.find(string="This site can't be reached"):
            logger.warning("Detected 'This site can't be reached' message.")
            return True
        
        
        # If none of the conditions are met, the proxy is likely not blocked
        return False
    except Exception as e:
        logger.error(f"Error during block detection: {str(e)}")
        # If an exception occurs, assume the proxy is blocked
        return True

def handle_popups(sb):
    """
    Handle various types of popups that might interfere with clicking
    """
    try:
        # Try different methods to remove overlays and popups
        scripts = [
            # Remove all iframe elements
            "var iframes = document.getElementsByTagName('iframe'); for(var i = 0; i < iframes.length; i++) { iframes[i].remove(); }",
            # Remove elements with 'popup' in class or id
            "var popups = document.querySelectorAll('[class*=popup], [id*=popup], [class*=modal], [id*=modal], [class*=overlay], [id*=overlay]'); popups.forEach(e => e.remove());",
            # Set body overflow to visible
            "document.body.style.overflow = 'visible';",
            # Remove fixed positioning that might create overlays
            "var fixed = document.querySelectorAll('div[style*=\"position: fixed\"]'); fixed.forEach(e => e.remove());"
        ]
        
        for script in scripts:
            sb.execute_script(script)
            
        # Small delay to let changes take effect
        time.sleep(1)
        return True
    except Exception as e:
        logger.error(f"Error handling popups: {str(e)}")
        return False

def click_details_with_retry(sb, max_attempts=3):
    """
    Attempt to click the details button multiple times with popup handling
    """
    for attempt in range(max_attempts):
        try:
            # Handle any popups first
            handle_popups(sb)
            
            # Try multiple selector methods to find and click the details button
            selectors = [
                "//a[contains(@class, 'btn btn-success btn-lg detail-link shadow-form shadow-button')][contains(text(), 'View Details')]",
                "//a[contains(text(), 'View Details')]",
                "//*[contains(text(), 'View Details')]",
                "/html/body/div[3]/div/div[2]/div[5]/div[1]/div[2]/a"
                "//a[contains(@aria-label, 'View All Details')]"
            ]
            
            for selector in selectors:
                try:
                    # Try to find and click the element
                    element = sb.find_element('xpath', selector)
                    sb.click(selector)
                    # sb.execute_script("arguments[0].click();", element)
                    return True
                except:
                    continue
                    
            logger.warning(f"Click attempt {attempt + 1} failed, retrying...")
            
        except Exception as e:
            logger.error(f"Error during click attempt {attempt + 1}: {str(e)}")
            
    return False

def scrape_person_data(sb, name, address, current_proxy, conn):
    url = address_to_url_conv(name, address)
    
    sb.activate_cdp_mode(url)
    sb.sleep(12)  # Reduced from 10
    sb.uc_gui_click_captcha()
    sb.sleep(10)  # Reduced from 5
    sb.execute_script("window.stop();")
    try:    
        # Check if proxy is blocked immediately after loading the page
        if detect_if_blocked(sb):
            logger.warning(f"Proxy {current_proxy} is blocked")
            add_blocked_proxy(current_proxy, conn)
            return None, True  # Return None for data and True for blocked status
        
    except Exception as e:
        logger.error(f"Error accessing URL: {str(e)}")
        if "proxy" in str(e).lower() or "connection" in str(e).lower():
            logger.warning(f"Proxy {current_proxy} appears to be blocked")
            add_blocked_proxy(current_proxy, conn)
            return None, True
        return None, False
    
    # Initialize data dictionary with empty values
    data = {
        'TPS Verified Name': '',
        'TPS Address': '',
        'Phone 1': '',
        'Phone 2': '',
        'Phone 3': '',
        'Phone 4': '',
        'Email 1': '',
        'Email 2': '',
        'Email 3': '',
        'Remarks': 'No record found',
        'Used Proxy': current_proxy  # Add the used proxy to the data
    }
    
    try:
        found_or_not = sb.get_text('/html/body/div[2]/div/div[2]/div[1]/div[1]') #/html/body/div[2]/div/div[2]/div[1]/div[1]
        logger.info(found_or_not)
        
        # Check if found_or_not has content before accessing it
        if found_or_not.split():
            number_found = int(found_or_not.split()[0])
        else:
            number_found = 0
        
        data['Remarks'] = found_or_not
    except:
        not_found = f'Record Not Found against {name}.'
        logger.info(not_found)
        number_found = 0
        data['Remarks'] = not_found
    
    if number_found <= 6 and number_found != 0:
        try:
            # Replace the simple click with our new retry function
            if not click_details_with_retry(sb):
                raise Exception("Failed to click details button after multiple attempts")
            sb.sleep(3)  # Reduced from 5
            sb.execute_script("window.stop();")

            # Extract person details
            tps_verified_name = sb.get_text('//*[@id="personDetails"]/div[1]/div/h1')
            logger.info(f'Truepeoplesearch name = {tps_verified_name}')
            data['TPS Verified Name'] = tps_verified_name

            tps_address = sb.get_text('//*[@id="personDetails"]/div[1]/div/span[2]')
            tps_address = tps_address.replace("Lives in ", "").strip()
            logger.info(f'Truepeoplesearch address = {tps_address}')
            data['TPS Address'] = tps_address


            phone_number_found = False
            # Extract phone numbers
            phone_xpaths1 = [
                 
                ('//*[@id="personDetails"]/div[9]/div[2]/div[2]/div[1]/div/span', 
                 '//*[@id="personDetails"]/div[9]/div[2]/div[2]/div[1]/div/a/span', 'Phone 1'),
                ('//*[@id="personDetails"]/div[9]/div[2]/div[2]/div[2]/div/span', 
                 '//*[@id="personDetails"]/div[9]/div[2]/div[2]/div[2]/div/a/span', 'Phone 2'),
                ('//*[@id="personDetails"]/div[9]/div[2]/div[3]/div[1]/div/span', 
                 '//*[@id="personDetails"]/div[9]/div[2]/div[3]/div[1]/div/a/span', 'Phone 3'), 
                ('//*[@id="personDetails"]/div[9]/div[2]/div[3]/div[2]/div/span', 
                 '//*[@id="personDetails"]/div[9]/div[2]/div[3]/div[2]/div/a/span', 'Phone 4') 
            ]

            for check_xpath, phone_xpath, key in phone_xpaths1:
                try:
                    mobile_check = sb.get_text(check_xpath)
                    if 'Wireless' in mobile_check:
                        phone = sb.get_text(phone_xpath)
                        logger.info(f'{key} = {phone}')
                        data[key] = phone
                        phone_number_found = True
                except Exception as e:
                    logger.error(f"Error checking phone path for: {key}")
                    # Continue to the next iteration without stopping the loop

            if not phone_number_found:
                phone_xpaths2 = [
                    ('//*[@id="personDetails"]/div[7]/div[2]/div[2]/div[1]/div/span', 
                    '//*[@id="personDetails"]/div[7]/div[2]/div[2]/div[1]/div/a/span', 'Phone 1'),
                    ('//*[@id="personDetails"]/div[7]/div[2]/div[2]/div[2]/div/span', 
                    '//*[@id="personDetails"]/div[7]/div[2]/div[2]/div[2]/div/a/span', 'Phone 2'),
                    ('//*[@id="personDetails"]/div[7]/div[2]/div[3]/div[1]/div/span', 
                    '//*[@id="personDetails"]/div[7]/div[2]/div[3]/div[1]/div/a/span', 'Phone 3'),
                    ('//*[@id="personDetails"]/div[7]/div[2]/div[3]/div[2]/div/span', 
                    '//*[@id="personDetails"]/div[7]/div[2]/div[3]/div[2]/div/a/span', 'Phone 4')
                ]

                
                for check_xpath, phone_xpath, key in phone_xpaths2:
                    try:
                        mobile_check = sb.get_text(check_xpath)
                        if 'Wireless' in mobile_check:
                            phone = sb.get_text(phone_xpath)
                            logger.info(f'{key} = {phone}')
                            data[key] = phone
                    except Exception as e:
                        logger.error(f"Error checking phone path for: {key}")
                        # Continue to the next iteration without stopping the loop
                
            # Flag to check if we found any emails
            found_email = False
            # Extract emails
            email_xpaths = [
                ('//*[@id="personDetails"]/div[12]/div[2]/div[2]/div/div', 'Email 1'),
                ('//*[@id="personDetails"]/div[12]/div[2]/div[3]/div/div', 'Email 2'),
                ('//*[@id="personDetails"]/div[12]/div[2]/div[4]/div/div', 'Email 3')
            ]
            
            for xpath, key in email_xpaths:
                try:
                    email = sb.get_text(xpath)
                    if '@' in email:
                        logger.info(f'{key} = {email}')
                        data[key] = email
                        found_email = True
                except:
                    pass

            

            if not found_email:
                email_xpaths = [
                ('//*[@id="personDetails"]/div[10]/div[2]/div[2]/div/div','Email 1'),
                ('//*[@id="personDetails"]/div[10]/div[2]/div[3]/div/div','Email 2'),
                ('//*[@id="personDetails"]/div[10]/div[2]/div[4]/div/div','Email 3')
                ]
                for xpath, key in email_xpaths:
                    try:
                        email = sb.get_text(xpath)
                        if '@' in email:
                            logger.info(f'{key} = {email}')
                            data[key] = email
                    except:
                        pass

                
            logger.info('Record found! Going to next...')
        except Exception as e:
            logger.error(f'Error extracting details: {str(e)}')
    
    return data, False  # Return data and False for not blocked

def shift_data_left(data):
    """Shift non-empty phone numbers and emails to the left"""
    # Process phone numbers
    phones = [data[f'Phone {i}'] for i in range(1, 5)]
    non_empty_phones = [p for p in phones if p]  # Filter out empty strings
    shifted_phones = non_empty_phones + [''] * (4 - len(non_empty_phones))  # Pad with empty strings
    
    # Process emails
    emails = [data[f'Email {i}'] for i in range(1, 4)]
    non_empty_emails = [e for e in emails if e]  # Filter out empty strings
    shifted_emails = non_empty_emails + [''] * (3 - len(non_empty_emails))  # Pad with empty strings
    
    # Update the data dictionary with shifted values
    for i in range(1, 5):
        data[f'Phone {i}'] = shifted_phones[i-1] if i-1 < len(shifted_phones) else ''
    
    for i in range(1, 4):
        data[f'Email {i}'] = shifted_emails[i-1] if i-1 < len(shifted_emails) else ''
    
    return data

def save_to_database(conn, row_id, data):
    data = shift_data_left(data)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO scraped_data 
    (input_row_id, tps_verified_name, tps_address, phone1, phone2, phone3, phone4, email1, email2, email3, remarks, used_proxy)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        row_id,
        data['TPS Verified Name'],
        data['TPS Address'],
        data['Phone 1'],
        data['Phone 2'],
        data['Phone 3'],
        data['Phone 4'],
        data['Email 1'],
        data['Email 2'],
        data['Email 3'],
        data['Remarks'],
        data['Used Proxy']
    ))
    conn.commit()

def export_to_csv(conn, input_data):
    # Query all data from the database
    cursor = conn.cursor()
    cursor.execute('''
    SELECT * FROM scraped_data ORDER BY input_row_id
    ''')
    scraped_data = cursor.fetchall()
    
    # Convert to DataFrame
    columns = ['id', 'input_row_id', 'TPS Verified Name', 'TPS Address', 'Phone 1', 'Phone 2', 
               'Phone 3', 'Phone 4', 'Email 1', 'Email 2', 'Email 3', 'Remarks', 'Used Proxy']
    df_scraped = pd.DataFrame(scraped_data, columns=columns)
    
    # Combine with input data
    df_scraped = df_scraped.drop(['id'], axis=1)
    df_scraped = df_scraped.rename(columns={'input_row_id': 'original_index'})
    
    # Add original index to input_data
    input_data['original_index'] = input_data.index
    
    # Merge the dataframes
    combined_data = pd.merge(input_data, df_scraped, on='original_index', how='left')
    combined_data = combined_data.drop(['original_index'], axis=1)
    
    # Export to CSV
    output_file = "TPS_output_data_ready_for_call_tools.csv"
    combined_data.to_csv(output_file, index=False)
    logger.info(f"Data exported to {output_file}")


def main():
    # Setup database
    conn = setup_database()
    
    # Load proxies
    with open('proxies.txt', 'r') as file:
        all_proxies = [line.strip() for line in file if line.strip()]
    
    # Filter out blocked proxies
    cursor = conn.cursor()
    cursor.execute("SELECT proxy FROM blocked_proxies")
    blocked = [row[0] for row in cursor.fetchall()]
    proxies = [p for p in all_proxies if p not in blocked]
    
    if not proxies:
        logger.error("All proxies are blocked. Please add new proxies.")
        return
    
    # Get input file
    input_file_name = input('What is input file name?? should be csv file format.\n: ')
    input_data = pd.read_csv(input_file_name)
    
    # Check for previous progress
    last_processed_row = get_last_processed_row(conn, input_file_name)
    
    # Ask user if they want to resume from last position
    start_row = 0
    if last_processed_row >= 0:
        resume = input(f"Previous session stopped at row {last_processed_row + 1}. Resume from there? (y/n): ").lower()
        if resume == 'y':
            start_row = last_processed_row + 1
            logger.info(f"Resuming from row {start_row + 1}")
        else:
            logger.info("Starting from the beginning")
    
    # Process data
    current_proxy = None
    proxy_use_count = 0
    max_proxy_uses = 4  # Use each proxy for 4 rows

    total_rows = len(input_data)
    
    for index, row in input_data.iloc[start_row:].iterrows():
        name = row['Name (Formatted)']
        address = row['Contact Address (City, State)']

        logger.info(f"\nProcessing row {index + 1} of {total_rows}")
        logger.info(f"Name: {name}, Address: {address}")
        
        # Check if we need a new proxy
        if current_proxy is None or proxy_use_count >= max_proxy_uses:
            if proxies:
                current_proxy = random.choice(proxies)
                proxy_use_count = 0
            else:
                print("No more proxies available. Exiting.")
                break
        
        # Format proxy
        ip, port, username, password = current_proxy.split(':')
        formatted_proxy = f"{username}:{password}@{ip}:{port}"
        
        # Try up to 5 times with different proxies if blocked
        success = False
        retries = 0
        
        while not success and retries < 5:
            try:
                logger.info(f"Processing row {index + 1}: {name} at {address} with proxy {formatted_proxy}")
                
                with SB(uc=True, 
                        test=True, 
                        locale="en", 
                        proxy=formatted_proxy, 
                        headless=False, 
                        headless1=False, 
                        headless2=False, 
                        incognito=True, 
                        multi_proxy=True, 
                        do_not_track=True, 
                        ad_block=True, 
                        browser='chrome', 
                        disable_csp=True,
                        undetectable=True,
                        ad_block_on=True,
                        headed=True, 
                        
                    ) as sb:
                    # Scrape data - now includes proxy blocking detection
                    data, is_blocked = scrape_person_data(sb, name, address, current_proxy, conn)
                    
                    if is_blocked:
                        # Proxy is blocked, remove it and try another
                        proxies.remove(current_proxy)
                        
                        if proxies:
                            current_proxy = random.choice(proxies)
                            ip, port, username, password = current_proxy.split(':')
                            formatted_proxy = f"{username}:{password}@{ip}:{port}"
                            proxy_use_count = 0
                            retries += 1
                            continue
                        else:
                            logger.error("No more proxies available. Exiting.")
                            break
                    
                    if data is not None:
                        # Save to database
                        logger.info(f"Data saved for row {index + 1}")
                        save_to_database(conn, index, data)
                        success = True
                        proxy_use_count += 1
                        
                        # Update progress after each successful scrape
                        update_progress(conn, index, input_file_name)
                    else:
                        retries += 1
                
            except Exception as e:
                logger.error(f"Error: {str(e)}")
                retries += 1
                
                # Check if it might be a proxy issue
                if "proxy" in str(e).lower() or "connection" in str(e).lower():
                    logger.warning(f"Proxy {current_proxy} might be blocked")
                    add_blocked_proxy(current_proxy, conn)
                    
                    if current_proxy in proxies:
                        proxies.remove(current_proxy)
                    
                    if proxies:
                        current_proxy = random.choice(proxies)
                        ip, port, username, password = current_proxy.split(':')
                        formatted_proxy = f"{username}:{password}@{ip}:{port}"
                        proxy_use_count = 0
                    else:
                        print("No more proxies available. Exiting.")
                        break
                
                time.sleep(2)  # Short delay before retry
            
            # Save progress after each attempt, even if it failed
            # This ensures we don't lose track of where we are if the script crashes
            update_progress(conn, index, input_file_name)
        
        if not success:
            logger.error(f"Failed to process row {index + 1}: {name} after {retries} retries")
            # Save empty data to database
            empty_data = {
                'TPS Verified Name': '',
                'TPS Address': '',
                'Phone 1': '',
                'Phone 2': '',
                'Phone 3': '',
                'Phone 4': '',
                'Email 1': '',
                'Email 2': '',
                'Email 3': '',
                'Remarks': f'Failed after {retries} retries',
                'Used Proxy': current_proxy
            }
            save_to_database(conn, index, empty_data)
    
    # Export final results to CSV
    logger.info("\nAll rows processed. Exporting results to CSV...")
    export_to_csv(conn, input_data)
    
    # Close database connection
    conn.close()

if __name__ == "__main__":
    main()