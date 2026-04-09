from ._anvil_designer import Form1Template
from anvil import *
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server


class Form1(Form1Template):
  def __init__(self, **properties):
    self.init_components(**properties)

    data = anvil.server.call('get_live_leaderboard')
    self.repeating_panel_1.items = data
