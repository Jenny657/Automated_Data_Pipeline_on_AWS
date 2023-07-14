CREATE DATABASE Gans_FlightWeather_data;

USE Gans_FlightWeather_data;

-- DROP TABLE cities;
CREATE TABLE cities (
		city_id INT NOT NULL AUTO_INCREMENT,
        city_name VARCHAR(50),
        country_code VARCHAR(50),
        PRIMARY KEY(city_id)
);

CREATE TABLE population (
		city_id int,
        population VARCHAR(50),
        latitude VARCHAR(50),
        longitude VARCHAR(50),
        timestamp_population DATETIME,
        FOREIGN KEY (city_id) REFERENCES cities(city_id)
);


-- DROP TABLE weather;
CREATE TABLE weather (
		id INT NOT NULL AUTO_INCREMENT,
        city_id int,
        time DATETIME,
        weather_description VARCHAR(50),
        temperature float(6),
        feels_like_temperature float(6),
        humidity  int,
        wind_speed float(6),
        information_retrieved_at DATETIME,
        PRIMARY KEY(id),
        FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE airports (
		airport_icao VARCHAR(50),
        airport_name VARCHAR(50),
        PRIMARY KEY(airport_icao)
);

-- DROP TABLE icao;
CREATE TABLE icao (
		city_id int,
		airport_icao VARCHAR(50),
        FOREIGN KEY (airport_icao) REFERENCES airports(airport_icao),
        FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE flights (
		flight_id INT NOT NULL AUTO_INCREMENT,
        flight_num VARCHAR(50),
        departure_icao VARCHAR(50),
        departure_City VARCHAR(50),
        scheduled_time_local DATETIME,
        arrival_icao VARCHAR(50),
        information_retrieved_at DATETIME,
        PRIMARY KEY(flight_id),
        FOREIGN KEY (arrival_icao) REFERENCES airports(airport_icao)
);

SELECT * FROM icao