from seleniumbase import SB
import pandas as pd
import random
import sqlite3
import time
import os
from bs4 import BeautifulSoup
import logging
import pyautogui
import re
from datetime import datetime

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

def setup_database():
    conn = sqlite3.connect('tps_data.db')
    cursor = conn.cursor()
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
    return -1

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
        page_source = sb.get_page_source()
        soup = BeautifulSoup(page_source, 'html.parser')
        text = soup.get_text()
        if "Access Denied" in text or "Sorry, you have been blocked" in text or "This site can't be reached" in text:
            logger.warning("Proxy is blocked: Access Denied or Sorry message found.")
            return True
        return False
    except Exception as e:
        logger.error(f"Error during block detection: {str(e)}")
        return True

def handle_popups(sb):
    try:
        scripts = [
            "var iframes = document.getElementsByTagName('iframe'); for(var i = 0; i < iframes.length; i++) { iframes[i].remove(); }",
            "var popups = document.querySelectorAll('[class*=popup], [id*=popup], [class*=modal], [id*=modal], [class*=overlay], [id*=overlay]'); popups.forEach(e => e.remove());",
            "document.body.style.overflow = 'visible';",
            "var fixed = document.querySelectorAll('div[style*=\"position: fixed\"]'); fixed.forEach(e => e.remove());"
            "/html/body/div[16]/div[2]/div[2]/div[2]/div[2]/button[1]/p"
        ]
        for script in scripts:
            sb.execute_script(script)
        time.sleep(1)
        return True
    except Exception as e:
        logger.error(f"Error handling popups: {str(e)}")
        return False

def handle_consent_dialog_if_present(sb):
    """
    Handles the consent dialog for truepeoplesearch.com if present.
    """
    consent_dialog_selector = ".fc-dialog"
    consent_button_selector = "button.fc-cta-consent"
    logger.info("Checking for consent dialog...")

    if sb.is_element_visible(consent_dialog_selector):
        logger.info("Consent dialog detected.")
        try:
            # Try scrolling the button into view
            sb.execute_script("""
                var btn = document.querySelector('button.fc-cta-consent');
                if(btn) { btn.scrollIntoView({behavior: 'smooth', block: 'center'}); }
            """)
            time.sleep(0.5)
            try:
                sb.click(consent_button_selector)
            except Exception as e:
                logger.warning(f"Normal click failed: {e}, trying JS click.")
                sb.execute_script("document.querySelector('button.fc-cta-consent').click();")
            time.sleep(1.2)
            logger.info("Consent dialog handled.")
            return True
        except Exception as e:
            logger.error(f"Error handling consent dialog: {e}")
            return False
    else:
        logger.info("Consent dialog not found.")
        return False

def solve_press_and_hold_captcha_if_present(sb, hold_time=12):
    """Detect and solve press & hold captcha if present. Uses pyautogui if block page detected."""
    try:
        # Check for block message in page source
        page_source = sb.get_page_source()
        soup = BeautifulSoup(page_source, "html.parser")
        text = soup.get_text()

        if "Access to this page has been denied" in text:
            sb.sleep(5)
            logger.info("Block page detected! Using pyautogui to solve press & hold captcha.")
            # pyautogui.moveTo(x=661, y=585, duration=0.4) # for windows 11 Safeer PC 
            pyautogui.moveTo(x=662, y=564, duration=0.4) # for windows 10 Umair laptop
            pyautogui.mouseDown()
            time.sleep(hold_time)
            pyautogui.mouseUp()
            sb.sleep(10)
            pyautogui.moveTo(x=1150, y=77, duration=0.4)
            logger.info("Press & Hold captcha solved via pyautogui.")
            return True
    except Exception as e:
        logger.error(f"Error solving press & hold captcha: {e}")
    return False

def solve_click_captcha_if_present(sb):
    """Detect and solve click captcha if present."""
    page_source = sb.get_page_source()
    soup = BeautifulSoup(page_source, "html.parser")
    text = soup.get_text()
    if "Just a moment..." in text or "Captcha" in text:
        logger.info("Click captcha detected, attempting to solve...")
        try:
            sb.uc_gui_click_captcha()
            sb.sleep(10)
            logger.info("Click captcha clicked.")
            return True
        except Exception as e:
            logger.error(f"Error clicking click captcha: {e}")
    return False

def handle_captchas(sb):
    """Detect and solve click or press & hold captchas, as many times as needed."""
    for _ in range(2):  # Sometimes a captcha can reload once - try twice
        solved = False
        page_source = sb.get_page_source()
        soup = BeautifulSoup(page_source, "html.parser")
        text = soup.get_text()
        # Try press & hold
        if "Access to this page has been denied" in text:
            solved |= solve_press_and_hold_captcha_if_present(sb)
        if "Just a moment..." in text or "Captcha" in text:
            solved |= solve_click_captcha_if_present(sb)
        if not solved:
            break
        sb.sleep(2)

def scrape_person_data(sb, name, address, current_proxy, conn):
    url = address_to_url_conv(name, address)
    sb.activate_cdp_mode(url)
    sb.sleep(15)
    handle_captchas(sb)
    handle_captchas(sb)
    handle_captchas(sb)
    sb.execute_script("window.stop();")
    handle_consent_dialog_if_present(sb)

    try:
        page_source = sb.get_page_source()
        soup = BeautifulSoup(page_source, 'html.parser')
        text = soup.get_text()
        if "Access Denied" in text or "Sorry, you have been blocked" in text or "This site can't be reached" in text:
            logger.warning(f"Proxy {current_proxy} is blocked")
            add_blocked_proxy(current_proxy, conn)
            return None, True
    except Exception as e:
        logger.error(f"Error accessing URL: {str(e)}")
        if "proxy" in str(e).lower() or "connection" in str(e).lower():
            logger.warning(f"Proxy {current_proxy} appears to be blocked")
            add_blocked_proxy(current_proxy, conn)
            return None, True
        return None, False

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
        'Used Proxy': current_proxy
    }
    xpaths = [
        '/html/body/div[3]/div/div[2]/div[3]/div[1]',
        '/html/body/div[3]/div/div[2]/div[1]/div[1]',
    ]
    sb.execute_script("window.stop();")
    try:
        for xpath in xpaths:
            script = f"""
            var element = document.evaluate("{xpath}", document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
            return element ? element.textContent : null;
            """
            try:
                found_or_not = sb.execute_script(script)
                if found_or_not is not None:
                    break
            except:
                continue
        logger.info(found_or_not)
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
            handle_consent_dialog_if_present(sb)
            try:
                selectors = ['body > div:nth-child(3) > div > div.content-center > div:nth-child(4) > div:nth-child(1) > div.col-md-4.hidden-mobile.text-center.align-self-center > a',
                'body > div:nth-child(3) > div > div.content-center > div:nth-child(4) > div:nth-child(1) > div.col-md-4.hidden-mobile.text-center.align-self-center > a'
                ]
                for selector in selectors:
                    sb.execute_script(f"""
                            var btn = document.querySelector('{selector}');
                            if(btn) {{ btn.scrollIntoView({{behavior: 'smooth', block: 'center'}}); }}
                        """)
                    try:
                        clicked = sb.execute_script(f"document.querySelector('{selector}').click();")
                        sb.sleep(8)
                        break
                    except:
                        continue
            except Exception as e:
                logger.error(f"Error clicking link to view data: {str(e)}")
                return None, False
            handle_captchas(sb)
            handle_consent_dialog_if_present(sb)

            page_source = sb.get_page_source()
            soup = BeautifulSoup(page_source, "html.parser")
            text = soup.get_text()
            if 'Access to this page has been denied' in text:
                logger.warning('"Please try again" message encountered, but continuing to process the row.')
                return None, True

            # Extract data from text using pattern matching
            data = extract_data_from_text(text, current_proxy)
            
            logger.info('Record found! Going to next...')

        except Exception as e:
            logger.error(f'Error extracting details: {str(e)}')

    return data, False

def extract_data_from_text(text, current_proxy):
    """Extract person data from scraped text using pattern matching"""
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
        'Remarks': 'Record found',
        'Used Proxy': current_proxy
    }
    
    try:
        # Extract Name (everything before first comma)
        name_match = re.search(r'^([^,]+),', text.strip())
        if name_match:
            tps_verified_name = name_match.group(1).strip()
            logger.info(f'Truepeoplesearch name = {tps_verified_name}')
            data['TPS Verified Name'] = tps_verified_name
        else:
            logger.warning("Could not find TPS Verified Name")

        # Extract Current Address
        current_address_pattern = r'Current Address.*?This is the most recently reported address.*?\n\n([^\n]+)'
        address_match = re.search(current_address_pattern, text, re.DOTALL)
        if address_match:
            tps_address = address_match.group(1).strip()
            # Clean up the address by removing extra info
            tps_address = re.sub(r'\$.*', '', tps_address).strip()
            logger.info(f'Truepeoplesearch address = {tps_address}')
            data['TPS Address'] = tps_address
        else:
            logger.warning("Could not find TPS Address")

        # Extract Phone Numbers (only wireless)
        # Revised section pattern to robustly capture the content block
        phone_section_pattern = r'(Phone Numbers[\s\S]*?Includes the current and past phone numbers[\s\S]*?)([\s\S]*?)(?=\s*Email Addresses|\s*Background Report|$)'
        phone_section_match = re.search(phone_section_pattern, text, re.DOTALL)
        
        if phone_section_match:
            # Group 2 captures the content block after the introductory text
            phone_section_content = phone_section_match.group(2) 
            
            # Find all phone entries with "Wireless" designation within the extracted section content
            phone_pattern = r'\((\d{3})\) (\d{3})-(\d{4}) - Wireless'
            phone_matches = re.findall(phone_pattern, phone_section_content)
            
            phone_keys = ['Phone 1', 'Phone 2', 'Phone 3', 'Phone 4']
            for i, phone_match in enumerate(phone_matches[:4]):  # Only first 4 phones
                if i < len(phone_keys):
                    # Reconstruct the phone number in the desired format
                    phone_number = f"({phone_match[0]}) {phone_match[1]}-{phone_match[2]}"
                    logger.info(f'{phone_keys[i]} = {phone_number}')
                    data[phone_keys[i]] = phone_number
        else:
            logger.warning("Could not find Phone Numbers section or its content.")

        # Extract Email Addresses
        # Revised section pattern to robustly capture the content block
        email_section_pattern = r'(Email Addresses[\s\S]*?Includes all known email addresses[\s\S]*?)([\s\S]*?)(?=\s*Current Address Property Details|$)'
        email_section_match = re.search(email_section_pattern, text, re.DOTALL)
        
        if email_section_match:
            # Group 2 captures the content block after the introductory text
            email_section_content = email_section_match.group(2)
            
            # Find all email addresses within the extracted section content
            email_pattern = r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
            email_matches = re.findall(email_pattern, email_section_content)
            
            email_keys = ['Email 1', 'Email 2', 'Email 3']
            for i, email in enumerate(email_matches[:3]):  # Only first 3 emails
                if i < len(email_keys):
                    logger.info(f'{email_keys[i]} = {email}')
                    data[email_keys[i]] = email
        else:
            logger.warning("Could not find Email Addresses section or its content.")

    except Exception as e:
        logger.error(f'Error in extract_data_from_text: {str(e)}')
    
    return data

def shift_data_left(data):
    phones = [data[f'Phone {i}'] for i in range(1, 5)]
    non_empty_phones = [p for p in phones if p]
    shifted_phones = non_empty_phones + [''] * (4 - len(non_empty_phones))
    emails = [data[f'Email {i}'] for i in range(1, 4)]
    non_empty_emails = [e for e in emails if e]
    shifted_emails = non_empty_emails + [''] * (3 - len(non_empty_emails))
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
    cursor = conn.cursor()
    cursor.execute('''
    SELECT * FROM scraped_data ORDER BY input_row_id
    ''')
    scraped_data = cursor.fetchall()
    columns = ['id', 'input_row_id', 'TPS Verified Name', 'TPS Address', 'Phone 1', 'Phone 2', 
               'Phone 3', 'Phone 4', 'Email 1', 'Email 2', 'Email 3', 'Remarks', 'Used Proxy']
    df_scraped = pd.DataFrame(scraped_data, columns=columns)
    df_scraped = df_scraped.drop(['id'], axis=1)
    df_scraped = df_scraped.rename(columns={'input_row_id': 'original_index'})
    input_data['original_index'] = input_data.index
    combined_data = pd.merge(input_data, df_scraped, on='original_index', how='left')
    combined_data = combined_data.drop(['original_index'], axis=1)
    output_file = "TPS_output_data_ready_for_call_tools.csv"
    combined_data.to_csv(output_file, index=False)
    logger.info(f"Data exported to {output_file}")

def main():
    conn = setup_database()
    with open('proxies.txt', 'r') as file:
        all_proxies = [line.strip() for line in file if line.strip()]
    cursor = conn.cursor()
    cursor.execute("SELECT proxy FROM blocked_proxies")
    blocked = [row[0] for row in cursor.fetchall()]
    proxies = [p for p in all_proxies if p not in blocked]
    if not proxies:
        logger.error("All proxies are blocked. Please add new proxies.")
        return
    input_file_name = input('What is input file name?? should be csv file format.\n: ')
    input_data = pd.read_csv(input_file_name)
    last_processed_row = get_last_processed_row(conn, input_file_name)
    start_row = 0
    if last_processed_row >= 0:
        resume = input(f"Previous session stopped at row {last_processed_row + 1}. Resume from there? (y/n): ").lower()
        if resume == 'y':
            start_row = last_processed_row + 1
            logger.info(f"Resuming from row {start_row + 1}")
        else:
            logger.info("Starting from the beginning")
    current_proxy = None
    proxy_use_count = 0
    max_proxy_uses = 15
    total_rows = len(input_data)
    for index, row in input_data.iloc[start_row:].iterrows():
        name = row['Name (Formatted)']
        address = row['Contact Address (City, State)']
        logger.info(f"\nProcessing row {index + 1} of {total_rows}")
        logger.info(f"Name: {name}, Address: {address}")
        if current_proxy is None or proxy_use_count >= max_proxy_uses:
            if proxies:
                current_proxy = random.choice(proxies)
                proxy_use_count = 0
            else:
                print("No more proxies available. Exiting.")
                break
        ip, port, username, password = current_proxy.split(':')
        formatted_proxy = f"{username}:{password}@{ip}:{port}"
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
                    data, is_blocked = scrape_person_data(sb, name, address, current_proxy, conn)
                    if is_blocked:
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
                        logger.info(f"Data saved for row {index + 1}")
                        save_to_database(conn, index, data)
                        success = True
                        proxy_use_count += 1
                        update_progress(conn, index, input_file_name)
                    else:
                        retries += 1
            except Exception as e:
                logger.error(f"Error: {str(e)}")
                retries += 1
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
                time.sleep(2)
            update_progress(conn, index, input_file_name)
        if not success:
            logger.error(f"Failed to process row {index + 1}: {name} after {retries} retries")
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
    logger.info("\nAll rows processed. Exporting results to CSV...")
    export_to_csv(conn, input_data)
    conn.close()

if __name__ == "__main__":
    main()