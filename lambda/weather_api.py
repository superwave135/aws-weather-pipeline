import aiohttp

class WeatherAPI:
  def __init__(self, api_token):
    """
    Initializes the WeatherAPI class.

    Args:
      api_token (str): Your Weather API token.
    """
    self.api_token = api_token
    self.base_url = "https://api.weatherapi.com/v1"

  async def _make_request(self, endpoint, params=None):
    """
    Internal method to make requests to the Weather API.

    Args:
      endpoint (str): The API endpoint (e.g., "/data/obs/region/recent").
      params (dict, optional): Query parameters for the request. Defaults to None.

    Returns:
      dict or None: The JSON response from the API, or None if an error occurs. Returns an empty dictionary if the API returns no data.
    """
    url = f"{self.base_url}{endpoint}"
    if params is None:
      params = {}
    params.update({"key": self.api_token})
    headers = {}

    try:
      async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:
          response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
          if "application/json" in response.headers.get('Content-Type', ''): # Safe content type check
            data = await response.json()
          else:
            text = await response.text()
            raise ValueError(f"The response is not in json format. Content: {text}")
          return data
    except aiohttp.ClientError as e:
      print(f"Error fetching data from Weather API: {e}")
      return None
    except ValueError as e: # Handle cases where the API returns non-json data
      print(f"Error decoding JSON response: {e}. Check if the API token is valid and the API is returning the expected data structure.")
      return None


  async def get_current_weather(self, q, **kwargs):
   """
    Retrieves recent bird observation data from the Weather API for a specific region.

    Args:
      q: Pass US Zipcode, UK Postcode, Canada Postalcode, IP address, Latitude/Longitude (decimal degree) or city name.
      **kwargs: Other optional parameters that the Weather API might accept for this endpoint.

    Returns:
      dict or None: A dictionary containing the JSON response, or None on error. Returns an empty dictionary if the API returns no data.
    """
   endpoint = f"/current.json"
   params = {"q":q}
   params.update(kwargs) # Add any other keyword arguments to the parameter dictionary.
   return await self._make_request(endpoint, params)
  

  async def get_forecast_weather(self, q, days, **kwargs):
    """
    Retrieves weather forecast data from the Weather API for a specific region.

    Args:
      q: Pass US Zipcode, UK Postcode, Canada Postalcode, IP address, Latitude/Longitude (decimal degree) or city name.
      **kwargs: Other optional parameters that the Weather API might accept for this endpoint.
          These include:
            days (int): Number of days of weather forecast (Value ranges from 1 to 14).

    Returns:
      dict or None: A dictionary containing the JSON response, or None on error. Returns an empty dictionary if the API returns no data.
    """
    endpoint = f"/forecast.json"
    params = {"q": q,
         "days": days
         }
    params.update(kwargs) # Add any other keyword arguments to the parameter dictionary.
    return await self._make_request(endpoint, params)
  
  
  async def get_actual_weather(self, q, dt, **kwargs):
    """
    Retrieves historical weather data from the Weather API for a specific region and date.

    Args:
      q: Pass US Zipcode, UK Postcode, Canada Postalcode, IP address, Latitude/Longitude (decimal degree) or city name.
      dt (str): Date on or after 1st Jan, 2015 in YYYY-MM-dd format.
      **kwargs: Other optional parameters that the Weather API might accept for this endpoint.

    Returns:
      dict or None: A dictionary containing the JSON response, or None on error. Returns an empty dictionary if the API returns no data.
    """
    endpoint = f"/history.json"
    params = {"q": q, "dt": dt}
    params.update(kwargs) # Add any other keyword arguments to the parameter dictionary.
    return await self._make_request(endpoint, params)
  

  async def search_location_metadata(self, q, **kwargs):
    """
    Retrieves metadata info of location

    Args:
      q: Pass US Zipcode, UK Postcode, Canada Postalcode, IP address, Latitude/Longitude (decimal degree) or city name.
    Returns:
      dict or None: A dictionary containing the JSON response, or None on error. Returns an empty dictionary if the API returns no data.
    """
    endpoint = f"/search.json"
    params = {"q": q}
    params.update(kwargs) # Add any other keyword arguments to the parameter dictionary.
    return await self._make_request(endpoint, params)