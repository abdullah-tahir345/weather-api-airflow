# Required Libraries
from datetime import datetime, timezone, timedelta
import psycopg2
from urllib.parse import urlparse
import pandas as pd
import requests


def weather_data_get(lat, long, api_key):

    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        #'q': {"lon":[74.3436], "lat":[31.5497]},
        'lon' : long,
        'lat' : lat,
        'appid': api_key
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as http_err:
        return f"HTTP error occurred: {http_err}"
    except Exception as err:
        return f"Other error occurred: {err}"


def convert_timestamp(datetime):
    # Create a Pandas Series from the Unix timestamp
    timestamp_series = pd.Series([datetime])
    # Convert the Unix timestamp to a datetime (UTC)
    utc_time = pd.to_datetime(timestamp_series, unit='s')
    # Convert to Pakistan Standard Time (UTC+5)
    pakistan_time = utc_time.dt.tz_localize('UTC').dt.tz_convert('Asia/Karachi')
    # Output the Pakistan time in a readable format
    date_time = pakistan_time.dt.strftime('%Y-%m-%d %H:%M:%S')[0]
    return date_time


def convert_sunrise(datetime):
    # Create a Pandas Series from the Unix timestamp
    timestamp_series = pd.Series([datetime])
    # Convert the Unix timestamp to a datetime (UTC)
    utc_time = pd.to_datetime(timestamp_series, unit='s')
    # Convert to Pakistan Standard Time (UTC+5)
    pakistan_time = utc_time.dt.tz_localize('GMT')#.dt.tz_convert('Asia/Karachi')
    # Output the Pakistan time in a readable format
    date_time = pakistan_time.dt.strftime('%Y-%m-%d %H:%M:%S')[0]
    return date_time

def convert_to_dataframe(weather_data, city_name):
    if isinstance(weather_data, dict):  # Check if it's valid data
        # Extract relevant weather data and append to the list
        Timestamp = convert_timestamp(weather_data['dt'])
        Sunrise = convert_sunrise(weather_data['sys']['sunrise'])
        Sunset = convert_timestamp(weather_data['sys']['sunset'])
        
        weather_data_list = dict({
            "City Name": city_name,
            "Longitude" : weather_data['coord']['lon'],
            "Latitude" : weather_data['coord']['lat'],
            "Weather ID" : weather_data['weather'][0]['id'],
            "Weather Condition" : weather_data['weather'][0]['main'],
            "Weather Description": weather_data['weather'][0]['description'],
            "Weather Data Base" : weather_data['base'],
            "Temperature (°C)": weather_data['main']['temp'],
            "Perceived Temperature (°C)" : weather_data['main']['feels_like'],
            "Minimum Temperature (°C)" : weather_data['main']['temp_min'],
            "Maximum Temperature (°C)" : weather_data['main']['temp_max'],
            "Atmospheric Pressure (Hectopascal)" : weather_data['main']['pressure'],
            "Humidity (%)": weather_data['main']['humidity'],
            "Sea Level Pressure (Hectopascal)" : weather_data['main']['sea_level'],
            "Ground Level Pressure (Hectopascal)" : weather_data['main']['grnd_level'],
            "Visibility (Meters)" : weather_data['visibility'],
            "Wind Direction (Degree)" : weather_data['wind']['deg'],
            "Wind Speed (m/s)": weather_data['wind']['speed'],
            "Cloudiness Percentage": weather_data['clouds']['all'],
            "Timestamp": Timestamp,
            "Sunrise" : Sunrise,
            "Sunset" : Sunset,
            "Timezone" : weather_data['timezone']
        })
        return weather_data_list
    else:
        return f"Failed to get data for {city_name}: {weather_data}"

def main_data():
    api_key = "55751ff3c3290f83fab38715544760c1"
    cities_dictionary = {
        #City : [Latitude, Longitude]
        'Lahore' : [ 74.410326 , 31.520170 ],
        'Allah Abad' : [ 74.051870 , 30.877741 ],
        'Pattoki' : [ 73.853086, 31.022899 ],
        'Okara' : [ 73.446185, 30.808930 ],
        'Islamabad' : [ 73.000260, 33.694881 ]
    }
    
    weather_data_list = []
    
    for cities in cities_dictionary.items():
        city_name = cities[0]
        weather_data = weather_data_get({cities[1][0]}, {cities[1][1]}, api_key)
        weather_data_list.append(convert_to_dataframe(weather_data, city_name))
    
    df = pd.DataFrame(weather_data_list)
    return df

def load_on_database(df):
    # Your PostgreSQL connection URL
    database_url = "postgresql://weather_database_owner:RBDW8TZkX9Jq@ep-noisy-math-a5adxemf.us-east-2.aws.neon.tech/weather_database?sslmode=require"
    
    # Parse the database URL
    parsed_url = urlparse(database_url)
    
    # Extract connection parameters from the URL
    dbname = parsed_url.path[1:]  # Remove the leading '/'
    user = parsed_url.username
    password = parsed_url.password
    host = parsed_url.hostname
    port = parsed_url.port
    
    # Extract only the required columns from the DataFrame for insertion
    data_to_insert = df.values.tolist()
    
    # Establish the connection
    try:
        connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            sslmode="require",  # Ensure SSL is used for the connection
            sslrootcert='root.crt'
        )
        print("Connection successful!")
    
        # Create a cursor to interact with the database
        cursor = connection.cursor()
    
        # Insert query
        insert_query = """
            INSERT INTO public.weather_data 
            (city_name, longitude, latitude, weather_id, weather_condition, weather_description, weather_data_base, temperature, perceived_temperature, minimum_temperature, maximum_temperature, atmospheric_pressure, humidity, sea_level_pressure, ground_level_pressure, visibility, wind_direction, wind_speed, cloudiness_percentage, timestamp, sunrise, sunset, timezone)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
    
        # Insert multiple rows of data into the weather_data table
        cursor.executemany(insert_query, data_to_insert)
    
        # Commit the changes to the database
        connection.commit()
    
        print("Data inserted successfully!")
    
        # Close the cursor and the connection
        cursor.close()
        connection.close()
    
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")



data = main_data()
load_on_database(data)