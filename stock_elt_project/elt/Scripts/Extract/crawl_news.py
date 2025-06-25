import requests
import json
import datetime

## Get the current time to get data format as (YYYYMMDDTHHMM)
def get_data_by_time_range(time_zone):
    ''' Get data based on current time zone'''
    yesterday = (datetime.date.today() - datetime.timedelta(days=1))
    if time_zone == 1:
        time_from = yesterday.strftime("%Y%m%dT" + "0000")
        time_to = yesterday.strftime("%Y%m%dT" + "2359")
    elif time_zone == 2:
        time_from = yesterday.strftime("%Y%m%dT" + "0000")
        time_to = yesterday.strftime("%Y%m%dT" + "1200")
    else:
        time_from = yesterday.strftime("%Y%m%dT" + "1201")
        time_to = yesterday.strftime("%Y%m%dT" + "2359")
    return time_from,time_to

def crawl_news():
    # Define params
    function = "NEWS_SENTIMENT"
    sort = "LATEST"
    limit = "1000"
    api_key = "NP337CRUIXPO3URQ"

    # Define list for result and length
    json_object = []
    total = 0

    # Call the function to get data by timezone
    for time_zone in [1,2,3]:
        time_from,time_to = get_data_by_time_range(time_zone)
        print(time_from,time_to)

        # API url
        url = f"https://www.alphavantage.co/query?function={function}&time_from={time_from}&time_to={time_to}&limit={limit}&apikey={api_key}"

        # Make request to the API
        r = requests.get(url)
    
        # Parse the data
        data = r.json()['feed']

        # Increment the total count of the news
        total += len(data)

        if total == 1000 and time_zone == 1:
            continue

        # Append the data to json_object list
        json_object += data

        if total < 1000 and time_zone == 1:
            break

    # Serialize the json object to a formatted string
    json_object = json.dumps(json_object,indent=4)

    # Get yesterday's date formatted as YYYY_MM_DD
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")

    # Define the path to save data
    path = r'/home/thangtranquoc/stock_elt_project/elt/Data/raw/news/crawl_news_' + f'{date}.json'

    # Write the json data to a file
    with open(path, 'w') as outfile:
        outfile.write(json_object)

    # Print the messages
    print(f'The process of crawling {total} news was successful')
    print(f'Saving at {path}')

# crawl_news()