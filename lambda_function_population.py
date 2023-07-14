import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import pytz
from datetime import datetime

def lambda_handler(event, context):
  schema="schema name"
  host="host"
  user="user"
  password="password"
  port=3306
  con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
  
  
#population table
  def get_population(soup):
    population_elem = soup.select_one('th.infobox-header:-soup-contains("Population")')
    return population_elem.parent.find_next_sibling().find(text=re.compile(r'\d+')) if population_elem else None

  
  def get_city_info(city, city_id):
    url = f'https://en.wikipedia.org/wiki/{city}'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')

    try:
        response_dict = {
            'city_id': city_id,
            'population': get_population(soup),
            'latitude': soup.select_one(".latitude").get_text(),
            'longitude': soup.select_one(".longitude").get_text()
        }
    except AttributeError:
        print(f'Failed to get data for {city}')
        return None

    return response_dict


  def recreate_wiki(cities):
    tz = pytz.timezone('Europe/Berlin')
    now = datetime.now().astimezone(tz).date()

    city_ids = list(range(1, len(cities) + 1))

    city_data = [get_city_info(city, city_id) for city, city_id in zip(cities, city_ids)]

    cities_df = pd.DataFrame(city_data)
    cities_df["timestamp_population"] = now.strftime("%Y/%m/%d")

    return cities_df
   
    
  
  list_of_cities = ['Hamburg', 'Berlin', 'Munich']
  pop_df = recreate_wiki(list_of_cities)
  
  pop_df['timestamp_population'] = pop_df['timestamp_population'].apply(pd.to_datetime)
  
  pop_df.to_sql('population', 
              if_exists='append', 
              con=con, 
              index=False)
  
    
  return {
  'statusCode': 200,
  'body': json.dumps('Hello from Lambda!')}
  
 