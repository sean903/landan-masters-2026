import anvil.server
from anvil.tables import app_tables

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import logging
import unicodedata


logging.basicConfig(level=logging.INFO)


PICKS = {
  "Ben C": ["Jon Rahm", "Si Woo Kim", "Chris Gotterup", "Justin Thomas"],
  "Tom C": ["Ludvig Åberg", "Jordan Spieth", "Jake Knapp", "Patrick Cantlay"],
  "Jamie W": ["Bryson DeChambeau", "Patrick Reed", "Viktor Hovland", "Corey Conners"],
  "Charlie L": ["Rory McIlroy", "Robert MacIntyre", "Akshay Bhatia", "Adam Scott"],
  "Angus M": ["Matt Fitzpatrick", "Hideki Matsuyama", "Russell Henley", "Harris English"],
  "Sean M": ["Tommy Fleetwood", "Brooks Koepka", "Shane Lowry", "Tyrrell Hatton"],
  "Fred W": ["Cameron Young", "Collin Morikawa", "Maverick McNealy", "Sepp Straka"],
  "Sam C": ["Xander Schauffele", "Justin Rose", "Nicolai Højgaard", "Jacob Bridgeman"],
  "Jack O": ["Scottie Scheffler", "Min Woo Lee", "J.J. Spaun", "Jason Day"],
}


NAME_ALIASES = {
  "jj spaun": "jj spaun",
  "ludvig aberg": "ludvig aberg",
  "nicolai hojgaard": "nicolai hojgaard",
}


def normalize_name(name):
  if not name:
    return ""

  name = unicodedata.normalize("NFKD", str(name))
  name = "".join(ch for ch in name if not unicodedata.combining(ch))
  name = name.lower()
  name = name.replace(".", "")
  name = name.replace("-", " ")
  name = name.replace("'", "")
  name = " ".join(name.split())
  return name


def canonical_name(name):
  n = normalize_name(name)
  return NAME_ALIASES.get(n, n)


def safe_int(value):
  try:
    return int(str(value).strip())
  except Exception:
    return None


def parse_player_score(row):
  raw = str(row.get("current_score_raw", "")).strip().upper()

  if raw in {"E", ""}:
    return 0.0

  try:
    return float(raw)
  except Exception:
    pass

  if raw in {"CUT", "MC"}:
    r1 = safe_int(row.get("round_1"))
    r2 = safe_int(row.get("round_2"))

    if r1 is not None and r2 is not None:
      return float((r1 - 72) + (r2 - 72))

    return 999.0

  if raw in {"WD", "DQ"}:
    return 999.0

  # Unknown status (not started, etc.) — treat as even par
  return 0.0


def get_raw_leaderboard():
  url = "https://www.pgatour.com/leaderboard"

  r = requests.get(
    url,
    timeout=20,
    headers={"User-Agent": "Mozilla/5.0"}
  )
  r.raise_for_status()

  soup = BeautifulSoup(r.content, "html.parser")
  script_tag = soup.find("script", {"id": "leaderboard-seo-data"})

  if not script_tag or not script_tag.string:
    raise ValueError("Could not find leaderboard JSON")

  leader_json = json.loads(script_tag.string)
  columns = leader_json["mainEntity"]["csvw:tableSchema"]["csvw:columns"]

  data = []
  for i in range(8):
    data.append([item["csvw:value"] for item in columns[i]["csvw:cells"]])

  leaderboard = pd.DataFrame(data).transpose()
  leaderboard.columns = [
    "position",
    "name",
    "current_score_raw",
    "hole",
    "round_1",
    "round_2",
    "round_3",
    "round_4",
  ]

  leaderboard["canonical_name"] = leaderboard["name"].apply(canonical_name)
  leaderboard["current_score"] = leaderboard.apply(parse_player_score, axis=1)

  return leaderboard


def match_player(leaderboard, player):
  """Match a pick name against the leaderboard.
  Tries full canonical name first, then falls back to surname matching.
  """
  key = canonical_name(player)
  match = leaderboard[leaderboard["canonical_name"] == key]
  if not match.empty:
    return match

  # Leaderboard uses abbreviated names like "S. Lowry"
  # Match by surname, then disambiguate by first initial if needed
  pick_parts = key.split()
  if len(pick_parts) < 2:
    return match  # empty

  surname = pick_parts[-1]
  first_initial = pick_parts[0][0]

  surname_matches = leaderboard[leaderboard["canonical_name"].str.endswith(f" {surname}")]

  if len(surname_matches) == 1:
    return surname_matches

  if len(surname_matches) > 1:
    # Disambiguate by first initial
    initial_matches = surname_matches[surname_matches["canonical_name"].str.startswith(first_initial)]
    if not initial_matches.empty:
      return initial_matches.head(1)
    return surname_matches.head(1)

  return match  # empty


def score_one_person(leaderboard, person, picks):
  player_rows = []

  for i, player in enumerate(picks, start=1):
    match = match_player(leaderboard, player)

    if match.empty:
      actual_name = player
      score = 0.0
      thru = ""
      found = False
    else:
      row = match.iloc[0]
      actual_name = row["name"]
      score = row["current_score"]
      thru = str(row.get("hole", "")).strip()
      found = True

    player_rows.append({
      "slot": i,
      "pick_name": player,
      "matched_name": actual_name,
      "score": score,
      "thru": thru,
      "found": found,
    })

  # Sort by score to find best 3
  sorted_players = sorted(player_rows, key=lambda x: x["score"])
  best_3 = sorted_players[:3]
  avg_score = sum(p["score"] for p in best_3) / 3.0

  out = {
    "person": person,
    "avg_score": avg_score,
  }

  # Store players in sorted order (best first)
  for idx, p in enumerate(sorted_players, start=1):
    out[f"player_{idx}"] = p["pick_name"]
    out[f"matched_player_{idx}"] = p["matched_name"]
    out[f"score_{idx}"] = p["score"]
    out[f"thru_{idx}"] = p["thru"]
    out[f"found_{idx}"] = p["found"]

  return out


def build_person_leaderboard():
  leaderboard = get_raw_leaderboard()
  rows = []

  for person, picks in PICKS.items():
    rows.append(score_one_person(leaderboard, person, picks))

  df = pd.DataFrame(rows)

  df = df.sort_values("avg_score").reset_index(drop=True)

  return df


@anvil.server.callable
def refresh_person_leaderboard():
  df = build_person_leaderboard()

  app_tables.person_leaderboard.delete_all_rows()

  for record in df.to_dict(orient="records"):
    app_tables.person_leaderboard.add_row(**record)

  logging.info("Loaded %s rows into person_leaderboard", len(df))
  return df.to_dict(orient="records")


@anvil.server.background_task
def scheduled_refresh():
  refresh_person_leaderboard()


@anvil.server.callable
def debug_leaderboard():
  try:
    leaderboard = get_raw_leaderboard()
    print(f"Leaderboard has {len(leaderboard)} players")
    for _, row in leaderboard.head(5).iterrows():
      print(f"  Name: '{row['name']}' -> canonical: '{row['canonical_name']}' score_raw: '{row['current_score_raw']}' score: {row['current_score']}")

    test_picks = ["Scottie Scheffler", "Rory McIlroy", "Jon Rahm"]
    for pick in test_picks:
      match = match_player(leaderboard, pick)
      matched_name = match.iloc[0]["name"] if not match.empty else "NOT FOUND"
      print(f"  Pick '{pick}' -> matched: '{matched_name}'")

    return "Check app console for output"
  except Exception as e:
    print(f"ERROR: {e}")
    return f"Error: {e}"


def format_score(x):
  if x is None:
    return ""
  if x == 999 or x == 999.0:
    return "N/A"
  if x == 0:
    return "E"
  return f"{x:+g}"


@anvil.server.callable
def get_live_leaderboard():
  """Fetch live data directly - no table needed."""
  leaderboard = get_raw_leaderboard()
  rows = []

  for person, picks in PICKS.items():
    result = score_one_person(leaderboard, person, picks)
    avg = result["avg_score"]
    rows.append({
      "person": result["person"],
      "avg_score": f"{avg:+.2f}" if avg < 999 else "N/A",
      "avg_score_num": avg,
      "player_1": result["player_1"],
      "score_1": format_score(result["score_1"]),
      "thru_1": result.get("thru_1", ""),
      "player_2": result["player_2"],
      "score_2": format_score(result["score_2"]),
      "thru_2": result.get("thru_2", ""),
      "player_3": result["player_3"],
      "score_3": format_score(result["score_3"]),
      "thru_3": result.get("thru_3", ""),
      "player_4": result["player_4"],
      "score_4": format_score(result["score_4"]),
      "thru_4": result.get("thru_4", ""),
    })

  return sorted(rows, key=lambda x: x["avg_score_num"])
