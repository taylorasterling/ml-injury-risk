# fetch-injuries-to-csv.py
import os, time, json, requests
import pandas as pd
from pathlib import Path
# 1) Your Sportradar API key
API_KEY = os.getenv("SPORTRADAR_API_KEY", "klO4acd3rXA5zSVNgG5QFnwntscHjSAeOOYIZIVJ")

# 2) API endpoint
BASE_URL = "https://api.sportradar.com/mlb/trial/v8/en/league/injuries.json"

# 3) Output paths and optional team filter
OUTPUT_RAW_DIR = "data/raw"
OUTPUT_CSV_PATH = "data/processed/injuries.csv"
TEAM_FILTER = None  # e.g., "NYM" or "Mets" or team UUID; leave None for all teams

def fetch_injuries(api_key: str) -> dict:
   if not api_key or api_key.strip() in {"YOUR_KEY", "{API_KEY}"}:
       raise RuntimeError("Missing API key. Set SPORTRADAR_API_KEY or hardcode API_KEY.")

   url = f"{BASE_URL}?api_key={api_key}"
   r = requests.get(url, headers={"accept": "application/json"}, timeout=30)
   if r.status_code == 403:
       raise RuntimeError(
           "403 Forbidden from Sportradar.\n"
           f"- URL tried: {url}\n"
           f"- Response (first 300 chars): {r.text[:300]}"
       )
   r.raise_for_status()
   ct = (r.headers.get("Content-Type") or "").lower()
   if "application/json" not in ct:
       raise RuntimeError(f"Unexpected content-type: {ct}\nFirst 200 chars: {r.text[:200]}")
   return r.json()

def cache_raw_json(data: dict, folder=OUTPUT_RAW_DIR) -> Path:
   Path(folder).mkdir(parents=True, exist_ok=True)
   ts = time.strftime("%Y%m%d-%H%M%S")
   p = Path(folder) / f"injuries_{ts}.json"
   p.write_text(json.dumps(data, indent=2))
   return p

def flatten_injuries(data: dict) -> pd.DataFrame:
   """
   Your payload: { "league": {...}, "teams": [ { team..., "players": [ { player..., "injuries": [ ... ] } ] } ] }
   """
   rows = []
   for team in (data.get("teams") or []):
       team_id = team.get("id")
       team_market = team.get("market")
       team_name = team.get("name")
       team_abbr = team.get("abbr")

       for player in (team.get("players") or []):
           pid = player.get("id")
           pname = player.get("full_name") or f"{player.get('first_name','')} {player.get('last_name','')}".strip()
           pos = player.get("position")
           primary_pos = player.get("primary_position")
           jersey = player.get("jersey_number")
           player_status = player.get("status")  # e.g., D60

           for inj in (player.get("injuries") or []):
               rows.append({
                   "team_id": team_id,
                   "team_market": team_market,
                   "team_name": team_name,
                   "team_abbr": team_abbr,
                   "player_id": pid,
                   "player_name": pname,
                   "position": pos,
                   "primary_position": primary_pos,
                   "jersey_number": jersey,
                   "player_status": player_status,
                   "injury_id": inj.get("id"),
                   "injury_status": inj.get("status"),     # e.g., D60
                   "injury_type": inj.get("desc"),         # Shoulder/Elbow/etc.
                   "injury_desc": inj.get("comment"),
                   "injury_start": inj.get("start_date"),
                   "injury_update": inj.get("update_date"),
               })

   df = pd.DataFrame(rows)
   if df.empty:
       return df

    # Apply team filter if set
   if TEAM_FILTER:
       if len(TEAM_FILTER) == 3:  # abbr like NYM
           df = df[df["team_abbr"].str.upper() == TEAM_FILTER.upper()]
       elif TEAM_FILTER.count("-") == 4:  # UUID-ish
           df = df[df["team_id"] == TEAM_FILTER]
       else:  # name/market text
           df = df[df["team_name"].str.contains(TEAM_FILTER, case=False, na=False) |
                   df["team_market"].str.contains(TEAM_FILTER, case=False, na=False)]

   # Convert date columns
   for col in ("injury_start", "injury_update"):
       if col in df.columns:
           df[col] = pd.to_datetime(df[col], errors="coerce")

   return (df.drop_duplicates()
             .sort_values(["team_market", "team_name", "player_name", "injury_start"], na_position="last")
             .reset_index(drop=True))

def save_csv(df: pd.DataFrame, path=OUTPUT_CSV_PATH) -> Path:
   out = Path(path)
   out.parent.mkdir(parents=True, exist_ok=True)
   df.to_csv(out, index=False)
   return out

if __name__ == "__main__":
   data = fetch_injuries(API_KEY)
   raw_path = cache_raw_json(data)
   df = flatten_injuries(data)

   print(f"[debug] teams={len(data.get('teams', []))}, rows={len(df)}")
   csv_path = save_csv(df)
   print(f"Saved raw JSON -> {raw_path}")
   print(f"Saved flattened CSV -> {csv_path}")
   if not df.empty:
       print("Columns:", ", ".join(df.columns))
   else:
       print("WARNING: CSV is empty. Open the cached JSON in data/raw/ to inspect the structure.")