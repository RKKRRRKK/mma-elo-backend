import requests
from bs4 import BeautifulSoup
import pandas as pd
from supabase import create_client, Client


SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def scrape_ufc_rankings(url: str) -> pd.DataFrame:
    response = requests.get(url)
    response.raise_for_status()  

    soup = BeautifulSoup(response.text, "html.parser")
    
    view_groupings = soup.find_all("div", class_="view-grouping")
    
    target_groupings = view_groupings[1:9]
    
    data = []
    
    for grouping in target_groupings:
        weightclass = grouping.find("div", class_="view-grouping-header").get_text(strip=True)
        caption = grouping.find("caption")
        if caption:
            champion_h5 = caption.find("h5")
            if champion_h5:
                champion_link = champion_h5.find("a")
                if champion_link:
                    champion_name = champion_link.get_text(strip=True)
                    
                    # Add champion as rank = 0
                    data.append({
                        "Rank": 0,
                        "Weightclass": weightclass,
                        "Name": champion_name
                    })
        
 
        tbody = grouping.find("tbody")
        if tbody:
            rows = tbody.find_all("tr")
            for row in rows:
                rank_td = row.find("td", class_="views-field views-field-weight-class-rank")
                name_td = row.find("td", class_="views-field views-field-title")
                
                if rank_td and name_td:
                    # Clean up the text
                    rank = rank_td.get_text(strip=True)
                    # The name <a> is inside the name_td
                    name_link = name_td.find("a")
                    if name_link:
                        athlete_name = name_link.get_text(strip=True)
                    else:
                        # Fallback if we can't find <a>
                        athlete_name = name_td.get_text(strip=True)
                    
                    data.append({
                        "Rank": rank,
                        "Weightclass": weightclass,
                        "Name": athlete_name
                    })
    

    df = pd.DataFrame(data, columns=["Rank", "Weightclass", "Name"])
    df.to_csv('test.csv', index = False)
    return df



def main():
    url = "https://www.ufc.com/rankings"

    df_rankings = scrape_ufc_rankings(url)
    supabase.table("ufc_ranks").delete().neq("Rank", -9999).execute()
    data_to_insert = df_rankings.to_dict(orient="records")
    response = supabase.table("ufc_ranks").insert(data_to_insert).execute()
    print("Insert response:", response)

if __name__ == "__main__":
    main()