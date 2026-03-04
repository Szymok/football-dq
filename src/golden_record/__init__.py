"""
Golden Record – rekordy referencyjne i mapowania encji.

Odpowiada za:
  - Słownik zawodników (player_id ↔ fbref_id ↔ understat_id)
  - Słownik drużyn (team_name normalization)
  - Słownik lig i sezonów
  - Entity resolution (fuzzy matching → deterministyczny ID)
"""
