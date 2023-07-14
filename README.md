# Introduction

A fictional German company called 'Gans' rents out e-scooters and is in need of automated marketassessment. My task was to assemble and automate a data pipeline in the cloud in order to anticipate as much as possible scooter movements. To limit the scope of the project and to maximize efficient learning of the basics, the project included only initial collection of data, its transformation and storage.

To collect data for an automated data pipeline on AWS, I followed these steps:

- Gathered general city information by scraping data from [Wikipedia](https://de.wikipedia.org/wiki/Wikipedia:Hauptseite). Specifically focusing on Hamburg, Berlin, and Munich.
- Obtained weather information using an API. The API provided weather data for the next 5 days, with updates available every 3 hours.
- Leveraged another API to retrieve real-time information about arriving flights scheduled for the next day.

With the collected data, I created dataframes, which were then transferred to a MySQL RDS: `city_table`, `population_table`, `weather_table`, `icao_table`, `airport_table` and `flights_table`

# Setting up the project

Set up the necessary API keys.
   - Obtain API keys from [OpenWeather](https://openweathermap.org/) and [AeroDataBox](https://rapidapi.com/aedbx-aedbx/api/aerodatabox/).

Create an AWS account if you don't have one already.
   - Visit the [AWS website](https://aws.amazon.com/) and click on "Create an AWS Account" to get started.
   - I used a Free Tier version to host my MySQL database in the cloud, make sure to choose the right plan.

Create a MySQL database and ensure that the tables align with your created dataframes.

Set up an RDS instance on AWS to host your MySQL database.
   - In the AWS Management Console, navigate to the RDS service and follow the instructions to create a new RDS instance.
   - Make note of the RDS endpoint, username, and password for later use.

For the lambda functions, ensure that you add layers for the libraries you use:
- Utilize AWS Data Wrangler + KLayers, which includes the necessary libraries used in this project.
- For SQL Alchemy, I utilized the ARN from this [GitHub account](https://github.com/keithrozario/Klayers)

# Usage

With `Local_DataFrames_with_connection_to_MySQL`, you will find the necessary code to set up a local MySQL database to test the functions. Ensure that you create a suitable MySQL database before attempting to connect the scripts with MySQL (For this see `MySQL_schema_set_up`). You can also skip this part and go straight to creating lambda functions on AWS.

I created three separate lambda functions to fill up different tables in my database:
 - The first function is responsible for populating static tables such as `cities`, `airports` and `icao` (For this see   )
 - The second function is designed to populate the `population` table with information about the population of each city.
 - The final function is responsible for populating the relevant weather and flights data into their respective tables.


