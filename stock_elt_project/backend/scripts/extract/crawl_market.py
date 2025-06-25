import requests
import json
import datetime
def crawl_markets():
    # define params and api_key
    api_key = 'NP337CRUIXPO3URQ'
    function = 'MARKET_STATUS'
    # API url
    url = f'https://www.alphavantage.co/query?function={function}&apikey={api_key}'

    # Make request to get data
    r = requests.get(url)
    data = r.json()

    # Parse the data
    data = data.get('markets',[])

    # Serialize the json_object to a formatted string
    json_object = json.dumps(data,indent=4)

    # Get yesterday format as YYYY_MM_DD
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y_%m_%d')

    # Define path to save data
    path = r'/home/thangtranquoc/stock_elt_project/backend/data/raw/markets/crawl_markets_' + f'{date}.json'

    # Write the json_object to a file
    with open(path,'w') as outfile:
        outfile.write(json_object)

    # Print the messages
    print(f'The process of crawling {len(data)} news was successful')
    print(f'Saving to {path}')
# crawl_markets()