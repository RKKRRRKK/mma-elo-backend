import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import re
import os
from supabase import create_client, Client

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# initialize the lists for missing data
yet_to_come = []
empty_page = []

# define df columns
columns = [
    'id', 'winner_id', 'winner_name', 'final_result', 'loser_name', 'loser_id',
    'event_name', 'event_date', 'winby', 'referee', 'round'
]
results_list = []

# read links from database
response = supabase.table('event_links').select('link').execute()
event_links = [item['link'] for item in response.data]

# Function to extract fighter id from href  careful gpt written regex...
def get_fighter_id(fighter_url):
    last_part = fighter_url.split('/')[-1]
    match = re.search(r'(\d+)$', last_part)
    if match:
        return match.group(1)
    else:
        return None

# useragent headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
                  '(KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36'
}

# Function to get detail after label in fight details table
def get_detail(fight_detail):
    texts = list(fight_detail.stripped_strings)
    if len(texts) >= 2:
        return texts[-1]
    elif len(texts) == 1:
        return texts[0]
    else:
        return ''

# scrape each event
for link in tqdm(event_links):
    fights_found = 0  # Initialize fights found for this event
    try:
        response = requests.get(link, headers=headers)
        print(f"Processing {link} - Status Code: {response.status_code}")
        if response.status_code != 200:
            print(f"Failed to retrieve {link}")
            empty_page.append(link)
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        event_detail_div = soup.find('div', class_='event_detail')
        if not event_detail_div:
            print(f"No event_detail found in {link}")
            empty_page.append(link)
            continue

        # get even name
        event_name_tag = event_detail_div.find('h1')
        if event_name_tag:
            event_name = event_name_tag.get_text(separator=" ", strip=True)
            print(f"Event Name: {event_name}")
        else:
            event_name = None
            print(f"No event name found in {link}")

        # get event date
        event_date_meta = soup.find('meta', itemprop="startDate")
        if event_date_meta:
            event_date = event_date_meta.get('content')
            print(f"Event Date: {event_date}")
        else:
            event_date = None
            print(f"No event date found in {link}")
            empty_page.append(link)
            continue

        # yet to come check
        if soup.find('span', class_='final_result yet_to_come'):
            print(f"Results yet to come for {link}")
            yet_to_come.append(link)
            continue

        # get main event 
        main_event = soup.find('div', itemprop='subEvent')
        if main_event:
            print(f"Found main event in {link}")
            try:
                winner = main_event.find('div', class_='fighter left_side')
                loser = main_event.find('div', class_='fighter right_side')

                if winner and loser:
                 
                    winner_name = winner.find('span', itemprop='name').get_text(strip=True)
                    winner_href = winner.find('a', itemprop='url')['href']
                    winner_id = get_fighter_id(winner_href)
                    final_result_tag = winner.find('span', class_='final_result')
                    final_result = final_result_tag.get_text(strip=True) if final_result_tag else None

                  
                    loser_name = loser.find('span', itemprop='name').get_text(strip=True)
                    loser_href = loser.find('a', itemprop='url')['href']
                    loser_id = get_fighter_id(loser_href)

                    # fight details
                    fight_details_table = main_event.find('table', class_='fight_card_resume')
                    if fight_details_table:
                        fight_details = fight_details_table.find_all('td')
           
                        match_number = get_detail(fight_details[0])
                        winby = get_detail(fight_details[1])
                        referee_text = get_detail(fight_details[2])
                        referee_tag = fight_details[2].find('a')
                        if referee_tag:
                            referee = referee_tag.get_text(strip=True)
                        else:
                            referee = referee_text
                        round_ = get_detail(fight_details[3])
                 

                        fight_id = len(results_list) + 1  # unique ascending fight id

                        # appending main event
                        results_list.append({
                            'id': fight_id, 'winner_id': winner_id, 'winner_name': winner_name,
                            'final_result': final_result, 'loser_name': loser_name, 'loser_id': loser_id,
                            'event_name': event_name, 'event_date': event_date, 'winby': winby,
                            'referee': referee if referee != 'N/A' else None, 'round': round_
                        })
                        fights_found += 1  # increment fights found
                        print(f"Added main event: {winner_name} vs {loser_name}")
                    else:
                        print(f"Fight details table not found in main event in {link}")
                else:
                    print(f"Main event fighters not found in {link}")
            except Exception as e:
                print(f"Error parsing main event in {link}: {e}")
        else:
            print(f"No main event found in {link}")

        # scrape other fights
        fight_rows = soup.select('table.new_table.result tr[itemprop="subEvent"]')
        print(f"Found {len(fight_rows)} fights in {link}")

        for fight in fight_rows:
            try:
                cols = fight.find_all('td')
                if len(cols) < 7:
                    print(f"Skipping fight due to insufficient columns in {link}")
                    continue  # skip if columns are missing

                match_number = cols[0].get_text(strip=True)
                winner_col = cols[1]
                loser_col = cols[3]
                winby_col = cols[4]
                round_col = cols[5]
                # time_col = cols[6]

                # winner details
                winner_tag = winner_col.find('a', itemprop='url')
                if winner_tag:
                    winner_name = winner_col.find('span', itemprop='name').get_text(separator=' ', strip=True)
                    winner_href = winner_tag['href']
                    winner_id = get_fighter_id(winner_href)
                    final_result = winner_col.find('span', class_='final_result').get_text(strip=True)
                else:
                    print(f"Missing winner info in fight {match_number} in {link}")
                    continue  # skip if winner info is missing

                # Loser details
                loser_tag = loser_col.find('a', itemprop='url')
                if loser_tag:
                    loser_name = loser_col.find('span', itemprop='name').get_text(separator=' ', strip=True)
                    loser_href = loser_tag['href']
                    loser_id = get_fighter_id(loser_href)
                else:
                    print(f"Missing loser info in fight {match_number} in {link}")
                    continue  # skip if loser info is missing

                # method and referee
                winby_text_tag = winby_col.find('b')
                if winby_text_tag:
                    winby_text = winby_text_tag.get_text(strip=True)
                else:
                    winby_text = winby_col.get_text(strip=True).strip()
                referee_tag = winby_col.find('a')
                if referee_tag:
                    referee = referee_tag.get_text(strip=True)
                else:
                    referee_span = winby_col.find('span', class_='sub_line')
                    referee = referee_span.get_text(strip=True) if referee_span else None

                # round
                round_ = round_col.get_text(strip=True)


                fight_id = len(results_list) + 1  # fight id 

                # appending non-main event 
                results_list.append({
                    'id': fight_id, 'winner_id': winner_id, 'winner_name': winner_name,
                    'final_result': final_result, 'loser_name': loser_name, 'loser_id': loser_id,
                    'event_name': event_name, 'event_date': event_date, 'winby': winby_text,
                    'referee': referee if referee != 'N/A' else None, 'round': round_
                })
                fights_found += 1  # increment fights found
                print(f"Added fight {match_number}: {winner_name} vs {loser_name}")
            except Exception as e:
                print(f"Error parsing fight {match_number} in {link}: {e}")
                continue

        # after processing both main event and other fights
        if fights_found == 0:
            print(f"No fights found in {link}")
            empty_page.append(link)

    except Exception as e:
        print(f"Error processing {link}: {e}")
        empty_page.append(link)
        continue


results_df = pd.DataFrame(results_list, columns=columns)

data = results_df.to_dict(orient='records')
supabase.table('mma_fight_results').insert(data).execute()

supabase.table('yet_to_come').insert([{'link': link} for link in yet_to_come]).execute()

supabase.table('empty_page').insert([{'link': link} for link in empty_page]).execute()

 
print("Run Finished")
