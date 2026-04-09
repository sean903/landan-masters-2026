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

  if raw == "E":
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

  return 999.0


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


def score_one_person(leaderboard, person, picks):
  player_rows = []

  for i, player in enumerate(picks, start=1):
    key = canonical_name(player)
    match = leaderboard[leaderboard["canonical_name"] == key]

    if match.empty:
      actual_name = player
      score = 999.0
      found = False
    else:
      row = match.iloc[0]
      actual_name = row["name"]
      score = row["current_score"]
      found = True

    player_rows.append({
      "slot": i,
      "pick_name": player,
      "matched_name": actual_name,
      "score": score,
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
def get_person_leaderboard():
  rows = app_tables.person_leaderboard.search()

  data = []
  for row in rows:
    data.append({
      "person": row["person"],
      "avg_score": row["avg_score"],
      "player_1": row["player_1"],
      "matched_player_1": row["matched_player_1"],
      "score_1": row["score_1"],
      "found_1": row["found_1"],
      "player_2": row["player_2"],
      "matched_player_2": row["matched_player_2"],
      "score_2": row["score_2"],
      "found_2": row["found_2"],
      "player_3": row["player_3"],
      "matched_player_3": row["matched_player_3"],
      "score_3": row["score_3"],
      "found_3": row["found_3"],
      "player_4": row["player_4"],
      "matched_player_4": row["matched_player_4"],
      "score_4": row["score_4"],
      "found_4": row["found_4"],
    })

  return sorted(data, key=lambda x: x["avg_score"])
