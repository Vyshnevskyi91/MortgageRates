import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
import time
import random
import os

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
    # Select a random User-Agent
    headers = {
        'User-Agent': random.choice(user_agents)
    }
    
    # Fetch and parse HTML content with headers
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    # Extract table headers and data rows
    header = [th.get_text(strip=True) for th in soup.find('thead').find_all('th') if th.get_text(strip=True)]
    data_rows = []
    for row in soup.find_all('tbody'):
        for tr in row.find_all('tr'):
            columns = tr.find_all('td')
            if len(columns) >= 3:  # Ensure sufficient columns
                rate = columns[0].get_text(strip=True)
                provider = columns[1].get_text(strip=True)
                # Collect only necessary data (omit 'Payment' column)
                data_rows.append([rate, provider])

    # Create and return DataFrame with renamed rate column
    df = pd.DataFrame(data_rows, columns=header[:2])  # Only 'Rate' and 'Provider' are needed
    df.rename(columns={'Rate': rate_column_name}, inplace=True)
    return df

def get_value(data, column):
    if not data.empty and column in data.columns:
        return data[column].values[0]
    else:
        return np.nan

# URLs and column names for each mortgage type
urls = {
    'Variable rate 5y': 'https://www.ratehub.ca/best-mortgage-rates/5-year/variable',
    'Variable rate 3y': 'https://www.ratehub.ca/best-mortgage-rates/3-year/variable',
    'Fixed rate 1y': 'https://www.ratehub.ca/best-mortgage-rates/1-year/fixed',
    'Fixed rate 2y': 'https://www.ratehub.ca/best-mortgage-rates/2-year/fixed',
    'Fixed rate 3y': 'https://www.ratehub.ca/best-mortgage-rates/3-year/fixed',
    'Fixed rate 4y': 'https://www.ratehub.ca/best-mortgage-rates/4-year/fixed',
    'Fixed rate 5y': 'https://www.ratehub.ca/best-mortgage-rates/5-year/fixed'
}

# Initialize an empty DataFrame to store the results
result_df = pd.DataFrame()

# Loop through each URL, scrape data, and merge into result_df
for rate_name, url in urls.items():
    # Scrape data and merge
    df = scrape_mortgage_rates(url, rate_name)
    result_df = df if result_df.empty else result_df.merge(df, on='Provider', how='outer')
    
    # Wait for a random interval between 10-30 seconds
    time.sleep(random.uniform(10, 30))

# Final result contains all providers and their rates
result_df = result_df[['Provider'] + list(urls.keys())]

OurBestMortgageRates = result_df.drop(columns=['Provider'])

# Convert percentages to float for comparison, processing each column individually
for col in OurBestMortgageRates.columns:
    OurBestMortgageRates[col] = OurBestMortgageRates[col].map(
        lambda x: float(x.strip('%')) if isinstance(x, str) and x.strip('%').replace('.', '', 1).isdigit() else np.nan
    )

# Set all values in each column to NaN except the minimum
for col in OurBestMortgageRates.columns:
    min_value = OurBestMortgageRates[col].min()
    OurBestMortgageRates[col] = OurBestMortgageRates[col].apply(lambda x: x if x == min_value else np.nan)

# Convert back to percentage strings
for col in OurBestMortgageRates.columns:
    OurBestMortgageRates[col] = OurBestMortgageRates[col].map(lambda x: f"{x}%" if pd.notnull(x) else "")

# Select only the first non-empty row per column
first_row_data = {col: OurBestMortgageRates[col][OurBestMortgageRates[col] != ""].iloc[0] for col in OurBestMortgageRates.columns}

# Convert the dictionary into a single-row DataFrame
OurBestMortgageRates = pd.DataFrame([first_row_data])

# Reset the index for clean display
OurBestMortgageRates = OurBestMortgageRates.T.reset_index()
OurBestMortgageRates.columns = ['Mortgage Type', 'Best Rate']

OurBestMortgageRates['Year'] = OurBestMortgageRates['Mortgage Type'].str.extract(r'(\d+\s*year|\d+y)').replace({'1y': '1 year', '2y': '2 year', '3y': '3 year', '4y': '4 year', '5y': '5 year'})
OurBestMortgageRates['Rate Type'] = OurBestMortgageRates['Mortgage Type'].apply(lambda x: 'Variable' if 'Variable' in x else 'Fixed')

# Pivot to get the desired columns
OurBestMortgageRates = OurBestMortgageRates.pivot(index='Year', columns='Rate Type', values='Best Rate').reset_index()

# Rename columns for clarity
OurBestMortgageRates.columns.name = None
OurBestMortgageRates = OurBestMortgageRates.rename(columns={'Year': 'TERMS', 'Fixed': 'FIXED', 'Variable': 'VARIABLE'})
OurBestMortgageRates.sort_values('TERMS', ascending=False, inplace=True)

banks = [
    ("TD Bank", "td_bank"),
    ("RBC Royal Bank", "RBC"),
    ("Bank of Montreal", "Bank_of_Montreal"),
    ("Scotiabank", "Scotiabank"),
    ("CIBC", "CIBC"),
    ("National Bank of Canada", "National_Bank")
]

# Dictionary to store each bank's DataFrame
bank_tables = {}

# Loop to create DataFrames for each bank
for bank_name, variable_name in banks:
    bank_data = result_df[result_df["Provider"] == bank_name]
    bank_tables[variable_name] = pd.DataFrame({
        "TERMS": OurBestMortgageRates['TERMS'],
        "FIXED": [
            get_value(bank_data, f"Fixed rate {term}") for term in ["5y", "4y", "3y", "2y", "1y"]
        ],
        "VARIABLE": [
            get_value(bank_data, f"Variable rate {term}") for term in ["5y", "4y", "3y", "2y", "1y"]
        ]
    })
    
td_bank = bank_tables["td_bank"]
RBC = bank_tables["RBC"]
Bank_of_Montreal = bank_tables["Bank_of_Montreal"]
Scotiabank = bank_tables["Scotiabank"]
CIBC = bank_tables["CIBC"]
National_Bank = bank_tables["National_Bank"]

# List of DataFrames
data_frames = {
    'result_df': result_df,
    'OurBestMortgageRates': OurBestMortgageRates,
    'td_bank': td_bank,
    'Bank_of_Montreal': Bank_of_Montreal,
    'Scotiabank': Scotiabank,
    'CIBC': CIBC,
    'National_Bank': National_Bank
}

# Directory where the files will be saved
output_dir = 'mortgage_rates_files/'

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Try to save DataFrames as individual Excel files
try:
    for sheet_name, df in data_frames.items():
        file_path = os.path.join(output_dir, f'{sheet_name}.xlsx')  # Create unique file name
        df.to_excel(file_path, index=False)
        print(f"DataFrame '{sheet_name}' has been successfully saved to {file_path}")
except Exception as e:
    print(f"An error occurred while saving the DataFrames: {e}")
