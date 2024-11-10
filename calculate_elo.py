import pandas as pd
import numpy as np
import os
import sys
from supabase import create_client, Client


SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

#scraped in previous script
response = supabase.table('mma_fight_results').select('*').execute()
new_fights_df = pd.DataFrame(response.data)

#conditions for 'dom'
conditions = [
    new_fights_df['winby'].str.contains('TKO|KO', case=False, regex=True, na=False),
    new_fights_df['winby'].str.contains('Submission', case=False, regex=True, na=False)
]

#outputs
choices = ['ko', 'sub']

# apply conditions, create new column 'dom'
new_fights_df['dom'] = np.select(conditions, choices, default='dec')

#previously finished table to update
response = supabase.table('fighters_enriched').select('''
    name,
    COALESCE(peak_elo, peak_elo_dom, peak_elo_dom_jj, 0) AS peak_elo,
    COALESCE(peak_elo_dom, peak_elo, peak_elo_dom_jj, 0) AS peak_elo_dom,
    COALESCE(peak_elo_dom_jj, peak_elo, peak_elo_dom, 0) AS peak_elo_dom_jj,
    COALESCE(current_elo, current_elo_dom, current_elo_dom_jj, 0) AS current_elo,
    COALESCE(current_elo_dom, current_elo, current_elo_dom_jj, 0) AS current_elo_dom,
    COALESCE(current_elo_dom_jj, current_elo, current_elo_dom, 0) AS current_elo_dom_jj,
    
    COALESCE(days_peak_dom_jj, 0) AS days_peak_dom_jj,
    COALESCE(days_peak_dom, 0) AS days_peak_dom,
    COALESCE(days_peak, 0) AS days_peak,

    COALESCE(best_win_dom_jj, 'none') AS best_win_dom_jj,
    COALESCE(best_win_dom, 'none') AS best_win_dom,
    COALESCE(best_win, 'none') AS best_win,
    COALESCE(nationality, 'none') AS nationality,
    COALESCE(birthplace, 'none') AS birthplace,
    COALESCE(birth_date, 'none') AS birth_date,
    COALESCE(association, 'none') AS association,
    COALESCE(weight_class, 'none') AS weight_class,
    COALESCE(ufc_position, 'none') AS ufc_position,
    COALESCE(ufc_class, 'none') AS ufc_class,
    fighter_id
''').execute()

final_df = pd.DataFrame(response.data)

#eensure IDs are strings for consistent merging
final_df['fighter_id'] = final_df['fighter_id'].astype(str)
new_fights_df['winner_id'] = new_fights_df['winner_id'].astype(str)
new_fights_df['loser_id'] = new_fights_df['loser_id'].astype(str)

#Initialize elos
current_elos_normal = {}
current_elos_dom = {}
current_elos_dom_jj = {}

#list to collect new fighters
new_fighters = []

# map existing fighters to their current elos
elo_columns = ['current_elo', 'current_elo_dom', 'current_elo_dom_jj']
for _, row in final_df.iterrows():
    fighter_id = row['fighter_id']
    current_elos_normal[fighter_id] = row['current_elo']
    current_elos_dom[fighter_id] = row['current_elo_dom']
    current_elos_dom_jj[fighter_id] = row['current_elo_dom_jj']

# identifying new fighters and assigning elo, concatenation creates a series with both the winner and loser id (fighter_id) and names (figher_name) to be iterated through.  
for fighter_id, fighter_name in zip(
    pd.concat([new_fights_df['winner_id'], new_fights_df['loser_id']]),
    pd.concat([new_fights_df['winner_name'], new_fights_df['loser_name']])
):
    if fighter_id not in current_elos_normal:
        current_elos_normal[fighter_id] = 1200.0
        current_elos_dom[fighter_id] = 1200.0
        current_elos_dom_jj[fighter_id] = 1200.0
        new_fighters.append({'fighter_id': fighter_id, 'name': fighter_name})

# define elo calculation functions
def expected_score(elo_a, elo_b):
    return 1 / (1 + 10 ** ((elo_b - elo_a) / 400))

def update_elo(winner_elo, loser_elo, k_factor, is_ko_or_sub, is_round_one, variation):
    if variation in ['dom', 'dom_jj']:
        if is_ko_or_sub:
            k_factor *= 2 if is_round_one else 1.5
    expected_win = expected_score(winner_elo, loser_elo)
    new_winner_elo = winner_elo + k_factor * (1 - expected_win)
    new_loser_elo = loser_elo + k_factor * (0 - (1 - expected_win))
    return round(new_winner_elo, 2), round(new_loser_elo, 2)

# initialize elo ratings for calculations
elo_ratings_normal = current_elos_normal.copy()
elo_ratings_dom = current_elos_dom.copy()
elo_ratings_dom_jj = current_elos_dom_jj.copy()

# initialize  lists to store fight results
results_normal = []
results_dom = []
results_dom_jj = []

# sort fights by date
new_fights_df['event_date'] = pd.to_datetime(new_fights_df['event_date'])
new_fights_df.sort_values('event_date', inplace=True)

# process each fight
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

    for variation in ['normal', 'dom', 'dom_jj']:
        k_factor = 60
        winner_elo_before = elo_ratings_normal[fight['winner_id']] if variation == 'normal' else \
                            elo_ratings_dom[fight['winner_id']] if variation == 'dom' else \
                            elo_ratings_dom_jj[fight['winner_id']]

        loser_elo_before = elo_ratings_normal[fight['loser_id']] if variation == 'normal' else \
                           elo_ratings_dom[fight['loser_id']] if variation == 'dom' else \
                           elo_ratings_dom_jj[fight['loser_id']]

        winner_elo_after, loser_elo_after = update_elo(
            winner_elo_before, loser_elo_before, k_factor, is_ko_or_sub, is_round_one, variation
        )

        # update ELO ratings
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
        else:
            elo_ratings_dom_jj[fight['winner_id']] = winner_elo_after
            elo_ratings_dom_jj[fight['loser_id']] = loser_elo_after
            results_dom_jj.append({**fight_data,
                                        'winner_elo_before': winner_elo_before,
                                        'winner_elo_after': winner_elo_after,
                                        'loser_elo_before': loser_elo_before,
                                        'loser_elo_after': loser_elo_after})
            

df_normal = pd.DataFrame(results_normal)
df_dom = pd.DataFrame(results_dom)
df_dom_jj = pd.DataFrame(results_dom_jj)

df_normal['event_date'] = df_normal['event_date'].astype(str)
df_dom['event_date'] = df_dom['event_date'].astype(str)
df_dom_jj['event_date'] = df_dom_jj['event_date'].astype(str)


#get rid of id columns let it e handled by supabase

data_normal = df_normal.drop(columns=['id'], errors='ignore').to_dict(orient='records')
data_dom = df_dom.drop(columns=['id'], errors='ignore').to_dict(orient='records')
data_pico = df_dom_jj.drop(columns=['id'], errors='ignore').to_dict(orient='records')


supabase.table('fighters_regular_raw').insert(data_normal).execute()
supabase.table('fighters_dom_raw').insert(data_dom).execute()
supabase.table('fighters_dom_jj_raw').insert(data_pico).execute()




# create dfs of new elos
elo_updates = pd.DataFrame({
    'fighter_id': list(elo_ratings_normal.keys()),
    'current_elo': list(elo_ratings_normal.values()),
    'current_elo_dom': list(elo_ratings_dom.values()),
    'current_elo_dom_jj': list(elo_ratings_dom_jj.values())
})

# merge with final_df
final_df = final_df.merge(elo_updates, on='fighter_id', how='outer', suffixes=('', '_new'))

# update current and peak elos
for idx, row in final_df.iterrows():
    for variation in ['normal', 'dom', 'dom_jj']:
        curr_elo_col = f'current_elo_{variation}' if variation != 'normal' else 'current_elo'
        peak_elo_col = f'peak_elo_{variation}' if variation != 'normal' else 'peak_elo'
        curr_elo_new = row.get(f'{curr_elo_col}_new', np.nan)

        if not np.isnan(curr_elo_new):
            final_df.at[idx, curr_elo_col] = curr_elo_new
            # convert peak_elo to float
            peak_elo = float(row[peak_elo_col]) if row[peak_elo_col] else 0.0  
            if curr_elo_new > peak_elo:
                final_df.at[idx, peak_elo_col] = curr_elo_new
                final_df.at[idx, f'days_peak_{variation}' if variation != 'normal' else 'days_peak'] = 0  # placeholder value

# handle new fighters
new_rows = []
for new_fighter in new_fighters:
    fighter_id = new_fighter['fighter_id']
    new_row = {
        'fighter_id': fighter_id,
        'name': new_fighter['name'],
        'current_elo': current_elos_normal[fighter_id],
        'peak_elo': current_elos_normal[fighter_id],
        'current_elo_dom': current_elos_dom[fighter_id],
        'peak_elo_dom': current_elos_dom[fighter_id],
        'current_elo_dom_jj': current_elos_dom_jj[fighter_id],
        'peak_elo_dom_jj': current_elos_dom_jj[fighter_id],
        'nationality': 'unknown',
        'birthplace': 'unknown',
        'birth_date': 'unknown',
        'association': 'unknown',
        'weight_class': 'unknown',
        'ufc_position': 0,
        'ufc_class': 'unknown'
    }
    new_rows.append(new_row)

# create a df from the list of new rows
new_fighters_df = pd.DataFrame(new_rows)

# concat new df with the final_df
final_df = pd.concat([final_df, new_fighters_df], ignore_index=True)

# remove temporary '_new' columns
final_df.drop(columns=[col for col in final_df.columns if col.endswith('_new')], inplace=True)



 

data_final = final_df.to_dict(orient='records')

for idx, row in final_df.head(10).iterrows():
    print(f"Row {idx + 1}")
    for col in final_df.columns:
        print(f"{col}: {row[col]}")
    print("\n" + "-" * 50 + "\n")

nan_columns = []
for col in final_df.columns:
    if final_df[col].isna().any():
        nan_columns.append(col)

# If NaN values exist, log the columns and terminate the script
if nan_columns:
    print(f"NaN detected in columns: {nan_columns}")
    sys.exit(1)
 

supabase.table('fighters_enriched').delete().neq('name', 'None').execute()
supabase.table('fighters_enriched').insert(data_final).execute()


supabase.table('new_fighters').insert(new_fighters).execute()