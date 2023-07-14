import json
import pandas as pd

def lambda_handler(event, context):
  schema="schema name"
  host="host"
  user="user"
  password="password"
  port=3306
  con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
  
# city table
  city_dic = {
    "city_name": ["Hamburg", "Berlin", "Munich"],
    "country_code": ["DE", "DE", "DE"]
  }
  
  city_df = pd.DataFrame(city_dic)
  city_df 
  
  city_df.to_sql('cities', 
              if_exists='append', 
              con=con, 
              index=False)

# airport table
  airport_dic = {
    "airport_icao": ["EDDH", "EDDB", "EDDM"],
    "airport_name": ["Flughafen Hamburg", "Flughafen Berlin Brandenburg", "Flughafen München"]
  }
 
  airport_df = pd.DataFrame(airport_dic)
  airport_df
  
  airport_df.to_sql('airports', 
              if_exists='append', 
              con=con, 
              index=False)
 
# icao table
  icao_dic = {
    "city_id": [1, 2, 3],
    "airport_icao": ["EDDH", "EDDB", "EDDM"],
  }
  
  icao_df = pd.DataFrame(icao_dic)
  icao_df
  
  icao_df.to_sql('icao', 
              if_exists='append', 
              con=con, 
              index=False)


    
  return {
  'statusCode': 200,
  'body': json.dumps('Hello from Lambda!')}


