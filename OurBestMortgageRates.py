import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
import time
import random
import os
import datetime as dt
import schedule

# List of multiple realistic User-Agent strings
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/112.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 9; SM-G960F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'Mozilla/5.0 (iPad; CPU OS 13_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.185 Mobile Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.80 Safari/537.36'
]

# Function to scrape rates from a given URL and return a DataFrame
def scrape_mortgage_rates(url, rate_column_name):
    headers = {'User-Agent': random.choice(user_agents)}
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    header = [th.get_text(strip=True) for th in soup.find('thead').find_all('th') if th.get_text(strip=True)]
    data_rows = []
    for row in soup.find_all('tbody'):
        for tr in row.find_all('tr'):
            columns = tr.find_all('td')
            if len(columns) >= 3:
                rate = columns[0].get_text(strip=True)
                provider = columns[1].get_text(strip=True)
                data_rows.append([rate, provider])

    df = pd.DataFrame(data_rows, columns=header[:2])  # Only 'Rate' and 'Provider' are needed
    df.rename(columns={'Rate': rate_column_name}, inplace=True)
    return df

def get_value(data, column):
    return data[column].values[0] if not data.empty and column in data.columns else np.nan

def schedule_scraping():
    # Generate a random minute between 0 and 59
    random_minute = random.randint(0, 59)
    # Generate the time in 24-hour format (e.g., 14:XX)
    schedule_time = f"16:{random_minute:02d}"
    # Schedule the job at the randomly generated time
    schedule.every().day.at(schedule_time).do(run_scraping)
    print(f"Scraping job scheduled for {schedule_time}.")

# Define the main function to execute the scraping and data processing
def run_scraping():
    print("Scraping job started at", dt.datetime.now())
    urls = {
        'Variable rate 5y': 'https://www.ratehub.ca/best-mortgage-rates/5-year/variable',
        'Variable rate 3y': 'https://www.ratehub.ca/best-mortgage-rates/3-year/variable',
        'Fixed rate 1y': 'https://www.ratehub.ca/best-mortgage-rates/1-year/fixed',
        'Fixed rate 2y': 'https://www.ratehub.ca/best-mortgage-rates/2-year/fixed',
        'Fixed rate 3y': 'https://www.ratehub.ca/best-mortgage-rates/3-year/fixed',
        'Fixed rate 4y': 'https://www.ratehub.ca/best-mortgage-rates/4-year/fixed',
        'Fixed rate 5y': 'https://www.ratehub.ca/best-mortgage-rates/5-year/fixed'
    }
    
    result_df = pd.DataFrame()
    for rate_name, url in urls.items():
        try:
            df = scrape_mortgage_rates(url, rate_name)
            result_df = df if result_df.empty else result_df.merge(df, on='Provider', how='outer')
            time.sleep(random.uniform(10, 30))
        except requests.RequestException as req_err:
            print(f"Network error while scraping {rate_name} at {url}: {req_err}")
        except Exception as e:
            print(f"Error processing data for {rate_name}: {e}")

    if result_df.empty:
        print("No data scraped. Exiting process.")
        return

    try:
        result_df = result_df[['Provider'] + list(urls.keys())]
        OurBestMortgageRates = result_df.drop(columns=['Provider'])
        for col in OurBestMortgageRates.columns:
            OurBestMortgageRates[col] = OurBestMortgageRates[col].map(
                lambda x: float(x.strip('%')) if isinstance(x, str) and x.strip('%').replace('.', '', 1).isdigit() else np.nan
            )
        for col in OurBestMortgageRates.columns:
            min_value = OurBestMortgageRates[col].min()
            OurBestMortgageRates[col] = OurBestMortgageRates[col].apply(lambda x: x if x == min_value else np.nan)
        for col in OurBestMortgageRates.columns:
            OurBestMortgageRates[col] = OurBestMortgageRates[col].map(lambda x: f"{x}%" if pd.notnull(x) else "")
        first_row_data = {col: OurBestMortgageRates[col][OurBestMortgageRates[col] != ""].iloc[0] for col in OurBestMortgageRates.columns}
        OurBestMortgageRates = pd.DataFrame([first_row_data])
        OurBestMortgageRates = OurBestMortgageRates.T.reset_index()
        OurBestMortgageRates.columns = ['Mortgage Type', 'Best Rate']
    except Exception as e:
        print(f"Error processing best mortgage rates: {e}")
        return

    try:
        OurBestMortgageRates['Year'] = OurBestMortgageRates['Mortgage Type'].str.extract(r'(\d+\s*year|\d+y)').replace({'1y': '1 year', '2y': '2 year', '3y': '3 year', '4y': '4 year', '5y': '5 year'})
        OurBestMortgageRates['Rate Type'] = OurBestMortgageRates['Mortgage Type'].apply(lambda x: 'Variable' if 'Variable' in x else 'Fixed')
        OurBestMortgageRates = OurBestMortgageRates.pivot(index='Year', columns='Rate Type', values='Best Rate').reset_index()
        OurBestMortgageRates.columns.name = None
        OurBestMortgageRates = OurBestMortgageRates.rename(columns={'Year': 'TERMS', 'Fixed': 'FIXED', 'Variable': 'VARIABLE'})
        OurBestMortgageRates.sort_values('TERMS', ascending=False, inplace=True)
    except Exception as e:
        print(f"Error formatting final mortgage rates table: {e}")
        return

    banks = [
        ("TD Bank", "td_bank"),
        ("RBC Royal Bank", "RBC"),
        ("Bank of Montreal", "Bank_of_Montreal"),
        ("Scotiabank", "Scotiabank"),
        ("CIBC", "CIBC"),
        ("National Bank of Canada", "National_Bank")
    ]
    
    bank_tables = {}
    
    for bank_name, variable_name in banks:
        try:
            bank_data = result_df[result_df["Provider"] == bank_name]
            bank_tables[variable_name] = pd.DataFrame({
                "TERMS": OurBestMortgageRates['TERMS'],
                "FIXED": [get_value(bank_data, f"Fixed rate {term}") for term in ["5y", "4y", "3y", "2y", "1y"]],
                "VARIABLE": [get_value(bank_data, f"Variable rate {term}") for term in ["5y", "4y", "3y", "2y", "1y"]]
            })
        except KeyError as e:
            print(f"Error extracting data for {bank_name}: {e}")
        except Exception as e:
            print(f"Unexpected error for bank {bank_name}: {e}")

    output_dir = 'mortgage_rates_files/'
    os.makedirs(output_dir, exist_ok=True)
    data_frames = {
        'result_df': result_df,
        'OurBestMortgageRates': OurBestMortgageRates,
        **bank_tables
    }
    
    for sheet_name, df in data_frames.items():
        try:
            file_path = os.path.join(output_dir, f'{sheet_name}.xlsx')
            df.to_excel(file_path, index=False)
            print(f"DataFrame '{sheet_name}' has been successfully saved to {file_path} {dt.datetime.now()}")
        except Exception as e:
            print(f"An error occurred while saving DataFrame '{sheet_name}': {e}")

run_scraping()

# Schedule the first job
schedule_scraping()

# Keep the script running and reschedule every day
while True:
    schedule.run_pending()
    time.sleep(10)
    # Reschedule the job every day at midnight to change the time for the next day
    if time.localtime().tm_hour == 0 and time.localtime().tm_min == 0:
        schedule.clear()  # Clear the previous schedule
        schedule_scraping()  # Reschedule for the new day