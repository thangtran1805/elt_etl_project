import requests
import datetime
import json

def crawl_ohlc():
    # Get date_crawl by yesterday and format as YYYY-MM-DD
    date_crawl = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    # Define params
    adjusted = 'true'
    include_otc = 'true'

    # Define keys
    api_key = 'RzQKZ3z6PAcQf86Jawe6lARGjqXhBWUn'

    # API url
    url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_crawl}?adjusted={adjusted}&include_otc={include_otc}&apiKey={api_key}'

    # Make request to the API
    r = requests.get(url)

    # Parse the data
    data = r.json()
    data = data.get('results',[])

    total = len(data)
    # Serialize the json_object to a formatted string

    json_object = json.dumps(data,indent=4)

    # Get yesterday date formatted as YYYY_MM_DD
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y_%m_%d')

    # Define the path to save data
    path = r'/home/thangtranquoc/stock_elt_project/elt/Data/raw/ohlc/crawl_ohlc_' + f'{date}.json'

    # Write the json data to a file
    with open(path,'w') as outfile:
        outfile.write(json_object)

    # Print the messages
    print(f'The process of crawing {total} ohlc was successful')
    print(f'Saving to {path}')
# crawl_ohlc()