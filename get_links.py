import requests
from bs4 import BeautifulSoup
import os
from supabase import create_client, Client


SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def extract_event_info(row):
    event_info = {}
    # extract the link from the onclick attribute
    onclick = row.get('onclick', '')
    # onclick attribute is like: document.location='/events/....';
    # extract the URL
    start = onclick.find("'") + 1
    end = onclick.rfind("'")
    link = onclick[start:end]
    link = 'https://www.sherdog.com' + link
    event_info['link'] = link

    # extract date
    date_td = row.find('td')
    date_divs = date_td.find('div', class_='calendar-date').find_all('div')
    event_info['month'] = date_divs[0].text.strip()
    event_info['day'] = date_divs[1].text.strip()
    event_info['year'] = date_divs[2].text.strip()

    # extract name
    name_td = date_td.find_next_sibling('td')
    event_name_a = name_td.find('a')
    event_info['name'] = event_name_a.text.strip()

    return event_info

def scrape_events(url, month, day, year, name, headers):
    event_found = False
    event_links = []

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch page {url}")
        return False, [], None

    soup = BeautifulSoup(response.content, 'html.parser')

    # on the main events page, the table is under div with id 'recentfights_tab'
    if 'sherdog.com/events/' in url:
        events_div = soup.find('div', class_='single_tab', id='recentfights_tab')
        if not events_div:
            print("No events div found on main events page")
            return False, [], None
        events_table = events_div.find('table', class_='new_table event')
    else:
        events_table = soup.find('table', class_='new_table event')

    if not events_table:
        print("No events table found")
        return False, [], None

    event_rows = events_table.find_all('tr', onclick=True)

    if not event_rows:
        print("No event rows found")
        return False, [], None

    latest_event = None
    for idx, row in enumerate(event_rows):
        event_info = extract_event_info(row)
        if latest_event is None and 'sherdog.com/events/' in url:
            latest_event = event_info  # update the latest event with the first event from the main page

        # check if this is the matching event
        if (event_info['month'] == month and
            event_info['day'] == day and
            event_info['year'] == year and
            event_info['name'] == name):
            event_found = True
            break  # stop processing
        else:
            # collect the event link
            event_links.append(event_info['link'])

    return event_found, event_links, latest_event

def read_initial_variables():
    response = supabase.table('initial_variables').select('*').execute()
    variables = response.data[0]  # Assuming there's at least one row
    return variables

def write_event_links(links):
    try:
        supabase.table('event_links').delete().execute()
        print("Cleared existing records in 'event_links' table.")
    except Exception as e:
        print(f"Error deleting existing records: {e}")
        exit(1)
    
    data = [{'link': link} for link in links]
    
    try:
        supabase.table('event_links').insert(data).execute()
        print(f"Inserted {len(data)} new links into 'event_links' table.")
    except Exception as e:
        print(f"Error inserting new links: {e}")
        exit(1)

def write_initial_variables(latest_event):
    supabase.table('initial_variables').update({
        'month': latest_event['month'],
        'day': latest_event['day'],
        'year': latest_event['year'],
        'name': latest_event['name']
    }).eq('id', 1).execute()

#read initial variables from Supabase
variables = read_initial_variables()


month = variables.get('month')
day = variables.get('day')
year = variables.get('year')
name = variables.get('name')

if not all([month, day, year, name]):
    print("Error: One or more initial variables are missing.")
    print(f"Please ensure the 'initial_variables' table contains month, day, year, and name.")
    exit(1)

# user agent header
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
                  'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                  'Chrome/115.0.0.0 Safari/537.36'
}

# initialize variables
event_links = []
latest_event = None

# step1 check the main events page
url_main = 'https://www.sherdog.com/events/'
print(f"Processing main events page: {url_main}")
event_found_main, event_links_main, latest_event_main = scrape_events(url_main, month, day, year, name, headers)

if latest_event_main:
    latest_event = latest_event_main

event_links.extend(event_links_main)

if event_found_main:
    print("Event found on main events page.")
else:
    print("Event not found on main events page.")
    # proceed to page 2
    url_page2 = 'https://www.sherdog.com/events/recent/2-page'
    print(f"Processing URL: {url_page2}")
    event_found_page2, event_links_page2, _ = scrape_events(url_page2, month, day, year, name, headers)

    event_links.extend(event_links_page2)

    if event_found_page2:
        print("Event found on page 2.")
    else:
        print("Event not found on page 2.")
        print("Error: Specified event not found.")

if event_links:
    # reverse the list to have events from oldest to newest
    event_links.reverse()
    print("\nEvent links:")
    for link in event_links:
        print(link)

    # write event links to Supabase
    write_event_links(event_links)
    print(f"\nEvent links have been written to Supabase 'event_links' table.")
else:
    print("No new events found")

if latest_event:
    print("\nUpdated initial variables:")
    print(f"month: {latest_event['month']}")
    print(f"day: {latest_event['day']}")
    print(f"year: {latest_event['year']}")
    print(f"name: {latest_event['name']}")

    # Update initial variables in Supabase
    write_initial_variables(latest_event)
else:
    print("No latest event found")