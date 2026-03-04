"""
Extractors – moduł odpowiedzialny za pobieranie danych z zewnętrznych źródeł.

Źródła:
  - FBref (via soccerdata)
  - Understat (via soccerdata)  
  - API-Football / football-data.org (planowane)
  - Transfermarkt (planowane)

Każdy ekstraktor implementuje interfejs BaseExtractor:
  - fetch_player_stats()
  - fetch_match_stats()
  - fetch_team_stats()
"""
