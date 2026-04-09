from ._anvil_designer import RowTemplate1Template
from anvil import *
import anvil.server


def format_score(x):
  if x is None:
    return ""
  if x == 999 or x == 999.0:
    return "N/A"
  return f"{x:+}"


class RowTemplate1(RowTemplate1Template):
  def __init__(self, **properties):
    self.init_components(**properties)

    self.person.text = self.item["person"]
    self.avg_score.text = f'{self.item["avg_score"]:+.2f}'

    self.player_1.text = self.item["player_1"]
    self.score_1.text = format_score(self.item["score_1"])

    self.player_2.text = self.item["player_2"]
    self.score_2.text = format_score(self.item["score_2"])

    self.player_3.text = self.item["player_3"]
    self.score_3.text = format_score(self.item["score_3"])

    self.player_4.text = self.item["player_4"]
    self.score_4.text = format_score(self.item["score_4"])
