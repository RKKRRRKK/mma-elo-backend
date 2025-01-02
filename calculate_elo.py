import pandas as pd
import numpy as np
import os
import sys
from supabase import create_client, Client
import time

#define batch function

def batch_insert(supabase_table, data, batch_size=10000):
    total_records = len(data)
    print(f"Starting batch insert into '{supabase_table}' with {total_records} records.")
    for i in range(0, total_records, batch_size):
        batch = data[i:i + batch_size]
        print(f"Inserting records {i + 1} to {i + len(batch)}...")
        retries = 3
        while retries > 0:
            try:
                supabase.table(supabase_table).insert(batch).execute()
                break  # Break the retry loop if successful
            except Exception as e:
                retries -= 1
                print(f"Error inserting records {i + 1} to {i + len(batch)}: {e}")
                if retries > 0:
                    print(f"Retrying... ({3 - retries} retries left)")
                    time.sleep(1)  # Wait a bit before retrying
                else:
                    print("Failed to insert batch after retries. Exiting.")
                    sys.exit(1)
    print(f"Finished batch insert into '{supabase_table}'.")


def batch_delete(table_name, batch_size=10000):

        offset = 0
        deleted_count = 0

        while True:
            response = supabase.table(table_name).select('id').neq('name', 'unknown').range(offset, offset + batch_size - 1).execute()

            data = response.data
            if not data:
                break

            chunk_ids = [row['id'] for row in data if 'id' in row]

            if chunk_ids:
                supabase.table(table_name).delete().in_('id', chunk_ids).execute()
                deleted_count += len(chunk_ids)
                print(f"Deleted {len(chunk_ids)} records (offset {offset})")

                if len(data) < batch_size:
                    break

                offset += batch_size
                time.sleep(0.1)
            else:
                break

        print(f"Deleted a total of {deleted_count} records from '{table_name}'.")


 
 

# Define cleanup function
def clean_fighter_id(fid):
    if fid is None or fid == '':
        return None
    try:
        fid_int = int(float(fid))
        return str(fid_int)
    except (ValueError, TypeError):
        return None

# Initialize Supabase client
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Scraped in previous script
response = supabase.table('mma_fight_results').select('*').execute()
new_fights_df = pd.DataFrame(response.data)

# Conditions for 'dom'
conditions = [
    new_fights_df['winby'].str.contains('TKO|KO', case=False, regex=True, na=False),
    new_fights_df['winby'].str.contains('Submission', case=False, regex=True, na=False)
]

# Outputs
choices = ['ko', 'sub']

# Apply conditions, create new column 'dom'
new_fights_df['dom'] = np.select(conditions, choices, default='dec')

# Previously finished table to update
records = []
limit = 50000
offset = 0

while True:
  
    response = supabase.table('fighters_enriched_new').select('''
        name,
        COALESCE(peak_elo, peak_elo_dom, 0) AS peak_elo,
        COALESCE(peak_elo_dom, peak_elo, 0) AS peak_elo_dom,
        COALESCE(current_elo, current_elo_dom, 0) AS current_elo,
        COALESCE(current_elo_dom, current_elo, 0) AS current_elo_dom,
        COALESCE(days_peak_dom, 0) AS days_peak_dom,
        COALESCE(days_peak, 0) AS days_peak,
        COALESCE(best_win_dom, 'unknown') AS best_win_dom,
        COALESCE(best_win, 'unknown') AS best_win,
        COALESCE(nationality, 'unknown') AS nationality,
        COALESCE(birthplace, 'unknown') AS birthplace,
        COALESCE(birth_date, 'unknown') AS birth_date,
        COALESCE(association, 'unknown') AS association,
        COALESCE(weight_class, 'unknown') AS weight_class,
        COALESCE(age, 'unknown') AS age,
        COALESCE(weight, 'unknown') AS weight,
        COALESCE(height, 'unknown') AS height,
        COALESCE(nickname, 'unknown') AS nickname,
        fighter_id
    ''').range(offset, offset + limit - 1).execute()
    
    data = response.data
    
    if not data:
        break
    
    records.extend(data)
    offset += limit

final_df = pd.DataFrame(records)
print(f"Fetched {len(final_df)} rows from fighters_enriched_new.")
print(f"Number of fighters in final_df right after creation: {final_df['fighter_id'].nunique()}")

# Apply the cleaning function to fighter IDs
final_df['fighter_id'] = final_df['fighter_id'].apply(clean_fighter_id)
new_fights_df['winner_id'] = new_fights_df['winner_id'].apply(clean_fighter_id)
new_fights_df['loser_id'] = new_fights_df['loser_id'].apply(clean_fighter_id)

# Ensure there are no None or NaN fighter_ids
final_df = final_df[final_df['fighter_id'].notnull()]
new_fights_df = new_fights_df[new_fights_df['winner_id'].notnull()]
new_fights_df = new_fights_df[new_fights_df['loser_id'].notnull()]

# Check for duplicates in final_df before any processing
initial_duplicates = final_df[final_df['fighter_id'].duplicated(keep=False)]

if not initial_duplicates.empty:
    print("Duplicates found in final_df before processing:")
    print(initial_duplicates)
    # Drop duplicates, keeping the first occurrence
    final_df = final_df.drop_duplicates(subset='fighter_id', keep='first')
    print("Duplicates have been removed from final_df.")

# Initialize elo dictionaries
current_elos_normal = {}
current_elos_dom = {}

# List to collect new fighters
new_fighters = []

# MATCHING ERROR CHECKING LOGS
print("\nSample fighter_ids from final_df:")
print(final_df['fighter_id'].head(5).tolist())

print("\nSample winner_ids from new_fights_df:")
print(new_fights_df['winner_id'].head(5).tolist())

print("\nSample loser_ids from new_fights_df:")
print(new_fights_df['loser_id'].head(5).tolist())
# END MATCHING ERROR CHECKING LOGS

# Map existing fighters to their current elos
for _, row in final_df.iterrows():
    fighter_id = row['fighter_id']
    current_elos_normal[fighter_id] = row['current_elo']
    current_elos_dom[fighter_id] = row['current_elo_dom']

# Identifying new fighters and assigning elo
for fighter_id, fighter_name in zip(
    pd.concat([new_fights_df['winner_id'], new_fights_df['loser_id']]),
    pd.concat([new_fights_df['winner_name'], new_fights_df['loser_name']])
):
    if fighter_id not in current_elos_normal:
        current_elos_normal[fighter_id] = 1200.0
        current_elos_dom[fighter_id] = 1200.0
        new_fighters.append({'fighter_id': fighter_id, 'name': fighter_name})

print(f"Number of fighters in final_df after cleaning: {final_df['fighter_id'].nunique()}")
print(f"Number of fighters in current_elos_normal: {len(current_elos_normal)}")

# Define elo calculation functions
def expected_score(elo_a, elo_b):
    return 1 / (1 + 10 ** ((elo_b - elo_a) / 400))

def update_elo(winner_elo, loser_elo, k_factor, is_ko_or_sub, is_round_one, variation):
    if variation in ['dom']:
        if is_ko_or_sub:
            k_factor *= 2 if is_round_one else 1.5
    expected_win = expected_score(winner_elo, loser_elo)
    new_winner_elo = winner_elo + k_factor * (1 - expected_win)
    new_loser_elo = loser_elo + k_factor * (0 - (1 - expected_win))
    return round(new_winner_elo, 2), round(new_loser_elo, 2)

# Initialize elo ratings for calculations
elo_ratings_normal = current_elos_normal.copy()
elo_ratings_dom = current_elos_dom.copy()

# Initialize lists to store fight results
results_normal = []
results_dom = []

# Sort fights by date
new_fights_df['event_date'] = pd.to_datetime(new_fights_df['event_date'])
new_fights_df.sort_values('event_date', inplace=True)

# Process each fight
for _, fight in new_fights_df.iterrows():
    fight_data = {
        'id': fight['id'],
        'winner_id': fight['winner_id'],
        'winner_name': fight['winner_name'],
        'loser_id': fight['loser_id'],
        'loser_name': fight['loser_name'],
        'event_name': fight['event_name'],
        'event_date': fight['event_date'],
        'winby': fight['winby'],
        'referee': fight['referee'],
        'round': fight['round'],
        'dom': fight['dom']
    }
    is_ko_or_sub = fight['dom'] in ['ko', 'sub']
    is_round_one = str(fight['round']) == '1'

    for variation in ['normal', 'dom']:
        k_factor = 60
        if variation == 'normal':
            winner_elo_before = elo_ratings_normal[fight['winner_id']]
            loser_elo_before = elo_ratings_normal[fight['loser_id']]
        elif variation == 'dom':
            winner_elo_before = elo_ratings_dom[fight['winner_id']]
            loser_elo_before = elo_ratings_dom[fight['loser_id']]

        winner_elo_after, loser_elo_after = update_elo(
            winner_elo_before, loser_elo_before, k_factor, is_ko_or_sub, is_round_one, variation
        )

        # Update ELO ratings
        if variation == 'normal':
            elo_ratings_normal[fight['winner_id']] = winner_elo_after
            elo_ratings_normal[fight['loser_id']] = loser_elo_after
            results_normal.append({**fight_data,
                                   'winner_elo_before': winner_elo_before,
                                   'winner_elo_after': winner_elo_after,
                                   'loser_elo_before': loser_elo_before,
                                   'loser_elo_after': loser_elo_after})
        elif variation == 'dom':
            elo_ratings_dom[fight['winner_id']] = winner_elo_after
            elo_ratings_dom[fight['loser_id']] = loser_elo_after
            results_dom.append({**fight_data,
                                'winner_elo_before': winner_elo_before,
                                'winner_elo_after': winner_elo_after,
                                'loser_elo_before': loser_elo_before,
                                'loser_elo_after': loser_elo_after})


df_normal = pd.DataFrame(results_normal)
df_dom = pd.DataFrame(results_dom)

df_normal['event_date'] = df_normal['event_date'].astype(str)
df_dom['event_date'] = df_dom['event_date'].astype(str)

# Get rid of id columns, let it be handled by Supabase
data_normal = df_normal.drop(columns=['id'], errors='ignore').to_dict(orient='records')
data_dom = df_dom.drop(columns=['id'], errors='ignore').to_dict(orient='records')

# Insert fight results into Supabase tables
supabase.table('fighters_regular_raw').insert(data_normal).execute()
supabase.table('fighters_dom_raw').insert(data_dom).execute()

# Create dataframe of new elos
elo_updates = pd.DataFrame({
    'fighter_id': list(elo_ratings_normal.keys()),
    'current_elo': list(elo_ratings_normal.values()),
    'current_elo_dom': list(elo_ratings_dom.values()),
})

# Ensure fighter_id is of string type
final_df['fighter_id'] = final_df['fighter_id'].astype(str)
elo_updates['fighter_id'] = elo_updates['fighter_id'].astype(str)

# Set fighter_id as the index for both dataframes
final_df.set_index('fighter_id', inplace=True)
elo_updates.set_index('fighter_id', inplace=True)

# Update final_df with elo_updates
final_df.update(elo_updates)

# Reset index to turn fighter_id back into a column
final_df.reset_index(inplace=True)

# Ensure unique fighter_ids when adding new fighters
existing_fighter_ids = set(final_df['fighter_id'])

# Filter out any new_fighters that already exist in final_df
filtered_new_fighters = []
for new_fighter in new_fighters:
    fighter_id = new_fighter['fighter_id']
    if fighter_id not in existing_fighter_ids:
        # Add missing fields with default values
        new_fighter.update({
            'current_elo': current_elos_normal[fighter_id],
            'peak_elo': current_elos_normal[fighter_id],
            'current_elo_dom': current_elos_dom[fighter_id],
            'peak_elo_dom': current_elos_dom[fighter_id],
            'days_peak': 0,
            'days_peak_dom': 0,
            'best_win_dom': 'unknown',
            'best_win': 'unknown',
            'nationality': 'unknown',
            'birthplace': 'unknown',
            'birth_date': 'unknown',
            'association': 'unknown',
            'weight_class': 'unknown',
            'age': 'unknown',
            'weight' : 'unknown',
            'height' : 'unknown',
            'nickname' : 'unknown'
        })
        filtered_new_fighters.append(new_fighter)
    else:
        print(f"Skipping fighter_id {fighter_id} as it already exists in final_df.")

new_fighters = filtered_new_fighters  # Update the new_fighters list

# Create a df from the list of new fighters
new_fighters_df = pd.DataFrame(new_fighters)

# Concatenate new_fighters_df with final_df
final_df = pd.concat([final_df, new_fighters_df], ignore_index=True, sort=False)

# After concatenation, remove any duplicates
final_df = final_df.drop_duplicates(subset='fighter_id', keep='first')

# Fill NaN values appropriately

# Define numeric and string columns
numeric_cols = [
    'days_peak', 'days_peak_dom','peak_elo_dom', 'peak_elo', 'current_elo',
    'current_elo_dom'
]

string_cols = [
    'name', 'best_win_dom', 'best_win', 'nationality',
    'birthplace', 'birth_date', 'association', 'weight_class', 'age', 'weight', 'height', 'nickname'
]

# Remove duplicates from column lists
numeric_cols = list(dict.fromkeys(numeric_cols))
string_cols = list(dict.fromkeys(string_cols))

# Verify that all columns exist in final_df
existing_numeric_cols = [col for col in numeric_cols if col in final_df.columns]
missing_numeric_cols = [col for col in numeric_cols if col not in final_df.columns]

if missing_numeric_cols:
    print(f"Warning: The following numeric columns are missing in final_df and will be skipped: {missing_numeric_cols}")

existing_string_cols = [col for col in string_cols if col in final_df.columns]
missing_string_cols = [col for col in string_cols if col not in final_df.columns]

if missing_string_cols:
    print(f"Warning: The following string columns are missing in final_df and will be skipped: {missing_string_cols}")

# Perform the fillna operation only on existing columns
final_df[existing_numeric_cols] = final_df[existing_numeric_cols].fillna(0)
final_df[existing_string_cols] = final_df[existing_string_cols].fillna('unknown')

# Ensure 'rn' column is present if required
if 'rn' in final_df.columns:
    final_df['rn'] = final_df['rn'].fillna(1).astype(int)

data_final = final_df.to_dict(orient='records')

# Final duplicate check
final_duplicates = final_df[final_df['fighter_id'].duplicated(keep=False)]

if not final_duplicates.empty:
    print("Duplicates found in final_df after processing:")
    print(final_duplicates)
    sys.exit(1)
else:
    print("No duplicates found in final_df after processing.")

# Update Supabase tables
# Delete existing data (if needed)
# supabase.table('fighters_enriched_new').delete().neq('name', 'unknown').execute()
batch_delete('fighters_enriched_new', batch_size=10000)

# Insert updated data
data_final_records = data_final 
batch_insert('fighters_enriched_new', data_final_records, batch_size=10000)

# Insert new fighters into 'new_fighters' table
if new_fighters:
    supabase.table('new_fighters').insert(new_fighters).execute()