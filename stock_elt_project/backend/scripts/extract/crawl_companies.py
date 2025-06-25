import requests
import json
import datetime

def crawl_companies():
    # Define params and apikey
    exchanges = ['nasdaq','nyse']
    api_key = 'cd7917ff4add7ac9a061009483f22985287130f4823e71b7c3f95c105c2e03b9'

    # Initialize a list to append the company data
    companies_list = []

    # Iterate over each exchange and fetch company data
    for exchange in exchanges:
        url = f'https://api.sec-api.io/mapping/exchange/{exchanges}?token={api_key}'
        r = requests.get(url)
        data = r.json()
        companies_list.extend(data)
        print(f'Extracted {len(data)} companies from the {exchange.upper()} stock exchange.')
    
    # Serialize the json_object to a formatted string
    json_object = json.dumps(companies_list,indent=4)

    # Get the current date for file name 
    date = (datetime.date.today()).strftime('%Y_%m_%d')
    path = r'/home/thangtranquoc/projects/stock_elt_project/backend/data/raw/companies/crawl_companies_' + f'{date}.json'

    # Write the json data to a file
    with open(path,'w') as outfile:
        outfile.write(json_object)

    # Print the messages
    print(f'Saving to {path}')
# crawl_companies() 

    
