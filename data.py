import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry


class OpenmeteoAPI():
    
	def __init__(self):
		# Setup the Open-Meteo API client with cache and retry on error
		self.__cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
		self.__retry_session = retry(self.__cache_session, retries = 5, backoff_factor = 0.2)
		self.__openmeteo = openmeteo_requests.Client(session = self.__retry_session)

		# Attributes
		self.url = "https://air-quality-api.open-meteo.com/v1/air-quality"
      
  
	def getPM2_5(self, lat: float, lon: float, start_date: str, end_date: str):
		"""Get PM2.5 readings from Open Metreo 

		Args:
			lat (float): latitude of the location
			lon (float): longitude of the location
			start_date (str): the date from which you want to start getting the data
			end_date (str): the end end of the data

		Returns:
			DataFrame:  A data frame contining pm2.5 reading and with timestamps
		"""
		params = {
			"latitude": lat,
			"longitude": lon,
			"hourly": ["pm2_5"],
			"timezone": "auto",
			"start_date": start_date,
			"end_date": end_date
		}
      
		try:
			responses = self.__openmeteo.weather_api(url=self.url, params=params)
			# Process first location. Add a for-loop for multiple locations or weather models
			response = responses[0]
			# Process hourly data. The order of variables needs to be the same as requested.
			hourly = response.Hourly()
			hourly_pm2_5 = hourly.Variables(0).ValuesAsNumpy()

			hourly_data = {"date": pd.date_range(
				start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
				end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
				freq = pd.Timedelta(seconds = hourly.Interval()),
				inclusive = "left"
			)}
			hourly_data["pm2_5"] = hourly_pm2_5

			result = pd.DataFrame(data=hourly_data).set_index("date")
			
		except Exception as e:
			result = str(e)
		return result

        
class SQLRespository():
	def __init__(self, connection):
		self.connection = connection

	def insert_table(self, table_name: str, records: pd.DataFrame, if_exists: str = "replace") -> dict:
		"""Insert DataFrame into SQLite database as table

		Args:
		table_name (str): _description_
		records (pd.DataFrame): _description_
		if_exists (str, optional): _description_. Defaults to "replace".

		Returns:
		dict: record write status and numbers of records
		"""

		n_inserted = records.to_sql(name=table_name, con=self.connection, if_exists=if_exists)

		return {
		"transaction_successful" : True,
		"records_inserted" : n_inserted
		}

	def __wrangle(self, df: pd.DataFrame):
		# Set the index of the data to date and fill Nan rows
		df = df.set_index("date").ffill()
		return df
     
	def read_table(self, table_name: str):
		"""Read table from SQLite database into a DataFrame

		Args:
			table_name (_type_): SQL tabel name

		Returns:
			pd.DataFrame: _description_
		"""
		query = f"SELECT * FROM {table_name}"
  
		df = self.__wrangle(pd.read_sql(sql=query, con=self.connection))

		return df
	
    

 
    
        