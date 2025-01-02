import requests
import pandas as pd
from sqlalchemy import create_engine

def extract() -> dict:
    """Extracts the relevant information from the input data."""
    API_URL = 'https://restcountries.com/v3.1/all?fields=name,currencies,languages,continents,capital'
    
    data = requests.get(API_URL).json()
    return data

def transform(data:dict)-> pd.DataFrame:
    """Transforms the extracted data into a pandas DataFrame."""
    
    df = pd.DataFrame(data)
    
    print(f'Total number of Countries from API is {len(df)}')
    
    # Columns transformation
    df['name'] = df['name'].apply(lambda x: x['official'] if isinstance(x, dict) else None)
    df['languages'] = df['languages'].apply(lambda x: list(x.values())[0] if len(x) > 0 else None)
    df['currencies'] = df['currencies'].apply(lambda x : list(x.values())[0]['name'] if len(x) > 0 else None)
    df['continents'] = [','.join(map(str,l)) for l in df['continents']]
    df['capital'] = [','.join(map(str, l)) for l in df['capital']]
    
    #reset index
    df.reset_index(inplace=True)
    
    return df[['name', 'capital', 'languages', 'currencies', 'continents']]

def load(df:pd.DataFrame) -> None:
    """Loads the extracted data into a SQLite."""
    
    # Create a connection to the SQLite database
    disk_engine = create_engine('sqlite:///countries.db')
    df.to_sql('countries', disk_engine, if_exists='replace')

# initalize etl functions
extracted_data = extract()
transformed_data = transform(extracted_data)
data = load(transformed_data)