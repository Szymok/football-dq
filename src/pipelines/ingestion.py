import logging
from datetime import datetime
import pandas as pd
import soccerdata as sd
from sqlalchemy.orm import Session
from src.storage.database import SessionLocal, Base, engine
from src.storage.models import Team, Player, Match, PlayerMatchStats, TeamEloRating

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def ingest_elo_data(db: Session, date_str: str = "2024-03-01"):
    """Pobiera dane ClubElo z określonej daty i ładuje do bazy."""
    logger.info(f"Pobieranie danych ClubElo dla daty {date_str}...")
    try:
        clubelo = sd.ClubElo()
        # Pobierz ranking dla wszystkich klubów na dany dzień
        df = clubelo.read_by_date(date_str)
        
        # Resetujemy index dostarczany przez soccerdata
        df = df.reset_index()

        # Tworzymy rekordy
        records = df.to_dict(orient="records")
        elo_date = datetime.strptime(date_str, "%Y-%m-%d")
        
        # Mapowanie i upsert
        count = 0
        for row in records:
            # soccerdata zwraca 'team', 'elo', 'country', 'level', itp.
            # pomińmy braki w rank
            team_name = row.get('team', '')
            if not team_name:
                continue

            existing = db.query(TeamEloRating).filter_by(
                team=team_name, date=elo_date, source="clubelo"
            ).first()

            if not existing:
                new_elo = TeamEloRating(
                    team=team_name,
                    elo=float(row.get('elo', 0.0)),
                    rank=None,  # często nie podają jawnie rankingu
                    date=elo_date,
                    country=row.get('country', ''),
                    level=str(row.get('level', '')),
                    source="clubelo"
                )
                db.add(new_elo)
                count += 1
        
        db.commit()
        logger.info(f"Zapisano {count} nowych ratingów z ClubElo.")
    except Exception as e:
        db.rollback()
        logger.error(f"Błąd podczas pobierania ClubElo: {e}")

def ingest_fbref_match_stats(db: Session, leagues="ENG-Premier League", seasons=2023, proxy=None, no_cache=False, force_cache=False):
    """Pobiera statystyki z FBref (summary) z opcjami z dokumentacji."""
    logger.info(f"Pobieranie statystyk FBref dla {leagues} sezon {seasons}...")
    try:
        # Przekazujemy parametry proxy i cache z dokumentacji
        fbref = sd.FBref(leagues=leagues, seasons=seasons, proxy=proxy, no_cache=no_cache)
        
        # Ograniczamy się do małego wycinka na start.
        df_stats = fbref.read_player_match_stats(stat_type="summary", force_cache=force_cache)
        df_stats = df_stats.reset_index()

        records = df_stats.to_dict(orient="records")
        count = 0
        
        for row in records:
            match_name = str(row.get('game', ''))
            team_name = str(row.get('team', ''))
            player_name = str(row.get('player', ''))
            
            if not match_name or not player_name:
                continue
            
            existing = db.query(PlayerMatchStats).filter_by(
                player_name=player_name, team=team_name, season=str(seasons), source="fbref"
            ).first()
            
            cols = row.keys()
            def find_val(keywords):
                for k in cols:
                    if isinstance(k, tuple):
                        if any(kw in str(k).lower() for kw in keywords):
                            return row[k]
                    elif any(kw in str(k).lower() for kw in keywords):
                        return row[k]
                return 0

            minutes = find_val(["min", "minutes", "time"])
            goals = find_val(["gls", "goals"])
            assists = find_val(["ast", "assists"])
            xg = find_val(["xg"])
            shots = find_val(["sh", "shots"])

            try:
                minutes = int(minutes) if pd.notna(minutes) else 0
                goals = int(goals) if pd.notna(goals) else 0
                assists = int(assists) if pd.notna(assists) else 0
                xg = float(xg) if pd.notna(xg) else 0.0
                shots = int(shots) if pd.notna(shots) else 0
            except:
                pass

            if not existing:
                new_stat = PlayerMatchStats(
                    player_id=1,  # mock
                    match_id=1,   # mock
                    player_name=player_name,
                    team=team_name,
                    minutes=minutes,
                    goals=goals,
                    assists=assists,
                    xg=xg,
                    shots=shots,
                    source="fbref",
                    season=str(seasons)
                )
                db.add(new_stat)
                count += 1
                
        db.commit()
        logger.info(f"Zapisano {count} wierszy statystyk z FBref.")
    except Exception as e:
        db.rollback()
        logger.error("Błąd podczas pobierania FBref.")

def ingest_understat_match_stats(db: Session, leagues="ENG-Premier League", seasons=2023):
    """Pobiera statystyki z Understat jako alternatywę/uzupełnienie, omijając blokady FBref."""
    logger.info(f"Pobieranie statystyk Understat dla {leagues} sezon {seasons}...")
    try:
        understat = sd.Understat(leagues=leagues, seasons=seasons)
        df_stats = understat.read_player_match_stats()
        df_stats = df_stats.reset_index()

        records = df_stats.to_dict(orient="records")
        count = 0
        
        for row in records:
            match_name = str(row.get('game', ''))
            team_name = str(row.get('team', ''))
            player_name = str(row.get('player', ''))
            
            if not match_name or not player_name:
                continue

            existing = db.query(PlayerMatchStats).filter_by(
                player_name=player_name, team=team_name, season=str(seasons), source="understat"
            ).first()

            if not existing:
                new_stat = PlayerMatchStats(
                    player_id=1, 
                    match_id=1,
                    player_name=player_name,
                    team=team_name,
                    minutes=int(row.get('time', 0)) if pd.notna(row.get('time')) else 0,
                    goals=int(row.get('goals', 0)) if pd.notna(row.get('goals')) else 0,
                    assists=int(row.get('assists', 0)) if pd.notna(row.get('assists')) else 0,
                    xg=float(row.get('xG', 0.0)) if pd.notna(row.get('xG')) else 0.0,
                    xg_assist=float(row.get('xA', 0.0)) if pd.notna(row.get('xA')) else 0.0,
                    shots=int(row.get('shots', 0)) if pd.notna(row.get('shots')) else 0,
                    source="understat",
                    season=str(seasons)
                )
                db.add(new_stat)
                count += 1

        db.commit()
        logger.info(f"Zapisano {count} wierszy statystyk z Understat.")
    except Exception as e:
        db.rollback()
        logger.error(f"Błąd podczas pobierania Understat: {e}")

if __name__ == "__main__":
    db = SessionLocal()
    # Inicjalizacja bazy, jeśli nie masz stworzonych tabel
    Base.metadata.create_all(bind=engine)
    
    # 1. Pobieranie danych ClubElo
    ingest_elo_data(db, date_str="2023-10-01")
    
    # 2. Pobieranie statystyk Understat (najlepsza alternatywa FBref)
    ingest_understat_match_stats(db, leagues="ENG-Premier League", seasons=2023)

    # 3. Pobieranie statystyk FBref
    # Przykłady konfiguracji z dokumentacji: 'proxy="tor"', 'no_cache=True'
    # Obecnie dla testu zakomentowane bez proxy, odkomentuj jeśli posiadasz TOR/Proxy.
    # ingest_fbref_match_stats(db, leagues="ENG-Premier League", seasons=2023, proxy=None, no_cache=True)
    
    db.close()
