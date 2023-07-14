import pandas as pd
import requests
from bs4 import BeautifulSoup
import pytz
import re
from datetime import datetime, date, timedelta
from pytz import timezone

# local df collection

# city_table

# creates a dictionary with city_name keys and country code values
city_dic = {
    "city_name": ["Hamburg", "Berlin", "Munich"],
    "country_code": ["DE", "DE", "DE"]
}

city_df = pd.DataFrame(city_dic)
city_df 

City_df.info()


#population_table
def get_population(soup):
    # Locate the table header element with the text 'Population'
    population_elem = soup.select_one('th.infobox-header:-soup-contains("Population")')
    # If found, find the next sibling and extract the first numerical data
    return population_elem.parent.find_next_sibling().find(text=re.compile(r'\d+')) if population_elem else None

# Function to extract city info from its Wikipedia page
def get_city_info(city, city_id):
    # Construct the URL
    url = f'https://en.wikipedia.org/wiki/{city}'
    # Send a GET request
    r = requests.get(url)
    # Parse the response content with BeautifulSoup
    soup = BeautifulSoup(r.content, 'html.parser')

    try:
        # Construct a dictionary with necessary details
        response_dict = {
            'city_id': city_id,
            'population': get_population(soup),
            'latitude': soup.select_one(".latitude").get_text(),
            'longitude': soup.select_one(".longitude").get_text()
        }
    except AttributeError:
        # If any data is missing, print an error message and return None
        print(f'Failed to get data for {city}')
        return None

    return response_dict

# Function to scrape data for a list of cities and return a DataFrame
def recreate_wiki(cities):
    # Set timezone to Europe/Berlin
    tz = pytz.timezone('Europe/Berlin')
    # Get current date and time
    now = datetime.now().astimezone(tz).date()

    # Generate city IDs
    city_ids = list(range(1, len(cities) + 1))

    # Get info for each city with corresponding ID
    city_data = [get_city_info(city, city_id) for city, city_id in zip(cities, city_ids)]

    # Convert the list of dictionaries to a DataFrame
    cities_df = pd.DataFrame(city_data)
    cities_df["timestamp_population"] = now.strftime("%Y/%m/%d")

    # Return the DataFrame
    return cities_df

list_of_cities = ['Hamburg', 'Berlin', 'Munich']
pop_df = recreate_wiki(list_of_cities)

pop_df['timestamp_population'] = pop_df['timestamp_population'].apply(pd.to_datetime)

pop_df.info()


# weather_table

cities = ["Hamburg", "Berlin", "Munich"]
API_key = "API KEY"

def create_df_weather(cities):

  # Set timezone to Europe/Berlin
  tz = pytz.timezone('Europe/Berlin')
  # Get current date and time
  now = datetime.now().astimezone(tz)

  # create an empty dictionary
  data_dict = {"city_id": [],
             "time": [],
             "weather_description": [],
             "temperature": [],
             "feels_like_temperature": [],
             "humidity": [],
             "wind_speed": [],
             'information_retrieved_at': []
             }

  # each weather entry is assigned a corresponding city ID, such as 1 for Hamburg, 2 for Berlin and so on
  city_ids = list(range(1, len(cities) + 1))


  # the for loop iterates through the list of cities and the list of corresponding city IDs, and then zips them together
  for city, city_id in zip(cities, city_ids):
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API_key}&units=metric"
    weather = requests.get(url)
    weather_json = weather.json()

    # the second loop retrieves data for each city and stores the information in the empty dictionary
    for i in weather_json['list']:
          data_dict["city_id"].append(city_id)
          data_dict['time'].append(i['dt_txt'])
          data_dict['weather_description'].append(i['weather'][0]['description'])
          data_dict['temperature'].append(i['main']['temp'])
          data_dict['feels_like_temperature'].append(i['main']['feels_like'])
          data_dict["humidity"].append(i["main"]["humidity"])
          data_dict["wind_speed"].append(i["wind"]["speed"])
          data_dict['information_retrieved_at'].append(now.strftime("%Y-%m-%d %H:%M:%S"))

  # the filled up dictionary is returned
  return data_dict

weather_df = pd.DataFrame(create_df_weather(cities))
weather_df

weather_df[['time', 'information_retrieved_at']] = weather_df[['time', 'information_retrieved_at']].apply(pd.to_datetime)

weather_df.info()


#icao_table

icao_dic = {
    "city_id": [1, 2, 3],
    "airport_icao": ["EDDH", "EDDB", "EDDM"],
}

icao_df = pd.DataFrame(icao_dic)
icao_df

icao_df.info()


#airport_table

airport_dic = {
    "airport_icao": ["EDDH", "EDDB", "EDDM"],
    "airport_name": ["Flughafen Hamburg", "Flughafen Berlin Brandenburg", "Flughafen München"]
}

airport_df = pd.DataFrame(airport_dic)
airport_df

airport_df.info()


#flights_Table
def icao_flight_arr_info(icao):

    # creates the date the information was tetrieved
    information_retrieved_at = datetime.now().astimezone(timezone('Europe/Berlin'))
    retrieve_at_day = information_retrieved_at.strftime("%d/%m/%Y %H:%M:%S")

    # create timeframes for each half of the day
    today = datetime.now().astimezone(timezone('Europe/Berlin')).date()
    # Calculate tomorrow's date
    tomorrow = (today + timedelta(days=1))
    times = [f"{tomorrow}T00:00/{tomorrow}T11:59",f"{tomorrow}T12:00/{tomorrow}T23:59"]

    # new column names
    new_columns = {
    "number": "flight_num",
    "movement.airport.icao": "departure_icao",
    "movement.airport.name": "departure_City",
    "movement.scheduledTime.local": "scheduled_time_local"
    }

    # API baggage
    querystring = {"direction":"Arrival","withCancelled":"false"}
    headers = {
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
        "X-RapidAPI-Key": "API KEY"
    }

    # list to return
    list_for_df = []

    # loop over icao entries
    for value in icao:
        # for each icao entry loop over timeframes
        for frame in times:
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{value}/{frame}"

            response = requests.get(url, headers=headers, params=querystring)

            df = pd.json_normalize(response.json()["arrivals"])[["number", "movement.airport.icao", "movement.airport.name", "movement.scheduledTime.local"]]
            arrival_df = df.rename(columns=new_columns)
            arrival_df["arrival_icao"] = value
            arrival_df['information_retrieved_at'] = retrieve_at_day
            list_for_df.append(arrival_df)

    return pd.concat(list_for_df, ignore_index=True)

# coordinates for Hamburg, Berlin, Munich
icao = ["EDDH", "EDDB", "EDDM"]
icao_flight_arr_info_df = icao_flight_arr_info(icao)

icao_flight_arr_info_df[['scheduled_time_local', 'information_retrieved_at']] = icao_flight_arr_info_df[['scheduled_time_local', 'information_retrieved_at']].apply(pd.to_datetime)

icao_flight_arr_info_df.info()


# SQLAlchemy

import sqlalchemy

get_ipython().system('pip install sqlalchemy')

pip install pymysql

import pymysql

# connecting to MySQL
schema="MySQL schema name" 
host="host"
user="user"
password="password"
port=3306
con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'


city_df.to_sql('cities', 
              if_exists='append', 
              con=con, 
              index=False)


pop_df.to_sql('population', 
              if_exists='append', 
              con=con, 
              index=False)


weather_df.to_sql('weather', 
              if_exists='append', 
              con=con, 
              index=False)


icao_df.to_sql('icao', 
              if_exists='append', 
              con=con, 
              index=False)


airport_df.to_sql('airports', 
              if_exists='append', 
              con=con, 
              index=False)


icao_flight_arr_info_df.to_sql('flights', 
              if_exists='append', 
              con=con, 
              index=False)

