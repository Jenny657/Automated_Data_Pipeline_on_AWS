import json
import pandas as pd
import requests
import pytz
from pytz import timezone
from datetime import datetime, date, timedelta

def lambda_handler(event, context):
    schema="schema name"
    host="host"
    user="user"
    password="password"
    port=3306
    con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'


# weather   
    cities = ["Hamburg", "Berlin", "Munich"]
    API_key = "API KEY"

    def create_df_weather(cities):
        tz = pytz.timezone('Europe/Berlin')
        now = datetime.now().astimezone(tz)

        data_dict = {"city_id": [],
                     "time": [],
                     "weather_description": [],
                     "temperature": [],
                     "feels_like_temperature": [],
                     "humidity": [],
                     "wind_speed": [],
                     'information_retrieved_at': []
                     }

        city_ids = list(range(1, len(cities) + 1))

        for city, city_id in zip(cities, city_ids):
            url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API_key}&units=metric"
            weather = requests.get(url)
            weather_json = weather.json()

            for i in weather_json['list']:
                data_dict["city_id"].append(city_id)
                data_dict['time'].append(i['dt_txt'])
                data_dict['weather_description'].append(i['weather'][0]['description'])
                data_dict['temperature'].append(i['main']['temp'])
                data_dict['feels_like_temperature'].append(i['main']['feels_like'])
                data_dict["humidity"].append(i["main"]["humidity"])
                data_dict["wind_speed"].append(i["wind"]["speed"])
                data_dict['information_retrieved_at'].append(now.strftime("%Y-%m-%d %H:%M:%S"))

        return data_dict

    weather_df = pd.DataFrame(create_df_weather(cities))
    weather_df[['time', 'information_retrieved_at']] = weather_df[['time', 'information_retrieved_at']].apply(pd.to_datetime)

    weather_df.to_sql('weather',
                      if_exists='append',
                      con=con,
                      index=False)

# flights

    def icao_flight_arr_info(icao):
    
        information_retrieved_at = datetime.now().astimezone(timezone('Europe/Berlin'))
        retrieve_at_day = information_retrieved_at.strftime("%d/%m/%Y %H:%M:%S")
    
        today = datetime.now().astimezone(timezone('Europe/Berlin')).date()
        tomorrow = (today + timedelta(days=1))
        times = [f"{tomorrow}T00:00/{tomorrow}T11:59",f"{tomorrow}T12:00/{tomorrow}T23:59"]
    
        new_columns = {
        "number": "flight_num",
        "movement.airport.icao": "departure_icao",
        "movement.airport.name": "departure_City",
        "movement.scheduledTime.local": "scheduled_time_local"
        }
    
        querystring = {"direction":"Arrival","withCancelled":"false"}
        headers = {
            "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
            "X-RapidAPI-Key": "API KEY"
        }
    
        list_for_df = []
    
        for value in icao:
            for frame in times:
                url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{value}/{frame}"
    
                response = requests.get(url, headers=headers, params=querystring)
    
                df = pd.json_normalize(response.json()["arrivals"])[["number", "movement.airport.icao", "movement.airport.name", "movement.scheduledTime.local"]]
                arrival_df = df.rename(columns=new_columns)
                arrival_df["arrival_icao"] = value
                arrival_df['information_retrieved_at'] = retrieve_at_day
                list_for_df.append(arrival_df)
    
        return pd.concat(list_for_df, ignore_index=True)

    
    
    # icao for Hamburg, Berlin, Munich
    icao = ["EDDH", "EDDB", "EDDM"]
    icao_flight_arr_info_df = icao_flight_arr_info(icao)
    
    icao_flight_arr_info_df[['scheduled_time_local', 'information_retrieved_at']] = icao_flight_arr_info_df[['scheduled_time_local', 'information_retrieved_at']].apply(pd.to_datetime)
    
    icao_flight_arr_info_df.to_sql('flights', 
              if_exists='append', 
              con=con, 
              index=False)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
