import os
import json
import logging
import pandas as pd
import soccerdata as sd
from datetime import datetime
from difflib import SequenceMatcher

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_similarity(a: str, b: str) -> float:
    """Zwraca stopień podobieństwa dwóch ciągów znaków (0.0 - 1.0)"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

class SoccerDataExtractor:
    def __init__(self, leagues="ENG-Premier League", seasons="2324"):
        self.leagues = leagues
        self.seasons = seasons

    def fetch_fbref_data(self):
        logger.info("Pobieranie danych z FBref...")
        try:
            fbref = sd.FBref(leagues=self.leagues, seasons=self.seasons)
            # W środowisku produkcyjnym można filtrować po datach/meczach.
            # Metoda pobiera szczegółowe statystyki meczowe graczy.
            stats = fbref.read_player_match_stats(stat_type="summary")
            return stats.reset_index()
        except Exception as e:
            logger.error(f"Błąd podczas pobierania z FBref: {e}")
            return pd.DataFrame()

    def fetch_understat_data(self):
        logger.info("Pobieranie danych z Understat...")
        try:
            understat = sd.Understat(leagues=self.leagues, seasons=self.seasons)
            stats = understat.read_player_match_stats()
            return stats.reset_index()
        except Exception as e:
            logger.error(f"Błąd podczas pobierania z Understat: {e}")
            return pd.DataFrame()

class DataQualityProfiler:
    def __init__(self, fbref_df: pd.DataFrame, understat_df: pd.DataFrame):
        self.fbref_df = fbref_df
        self.understat_df = understat_df
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "completeness": {},
            "consistency": {},
            "accuracy": {}
        }

    def check_completeness(self):
        logger.info("1/3 Uruchamianie testów Completeness...")
        
        # Funkcja pomocnicza do wyszukiwania kolumny zaczynającej/zawierającej 'xg'
        def get_null_pct(df, col_hint):
            if df.empty: return None
            cols = [c for c in df.columns if col_hint in str(c).lower()]
            if not cols: return None
            col = cols[0]
            nulls = df[col].isnull().sum()
            total = len(df)
            return round((nulls / total) * 100, 2) if total else 0

        self.results["completeness"]["fbref_xg_null_pct"] = get_null_pct(self.fbref_df, 'xg')
        self.results["completeness"]["understat_xg_null_pct"] = get_null_pct(self.understat_df, 'xg')

    def check_consistency(self):
        logger.info("2/3 Uruchamianie testów Consistency (Entity Matching)...")
        if self.fbref_df.empty or self.understat_df.empty:
            self.results["consistency"]["status"] = "Brak wystarczających danych do porównania."
            return
            
        # soccerdata zwraca nazwy zawodników zwykle w kolumnie 'player'
        def get_players(df):
            cols = [c for c in df.columns if 'player' in str(c).lower()]
            if cols:
                return df[cols[0]].dropna().unique()
            return []

        fbref_players = get_players(self.fbref_df)
        understat_players = get_players(self.understat_df)

        if len(fbref_players) == 0 or len(understat_players) == 0:
            self.results["consistency"]["status"] = "Nie znaleziono kolumn z zawodnikami."
            return

        mapped_count = 0
        THRESHOLD = 0.8
        
        # Naiwny fuzzy matching per player (Złożoność O(N*M) - dobre dla próbki,
        # w produkcji zalecana baza 'Golden Record' lub deduplikator np. dedupe)
        for fb_p in fbref_players:
            best_score = 0
            for un_p in understat_players:
                score = get_similarity(fb_p, un_p)
                if score > best_score:
                    best_score = score
                    if best_score == 1.0:
                        break # Wczesne wyjście dla idealnego matcha
            if best_score >= THRESHOLD:
                mapped_count += 1

        pct_mapped = round((mapped_count / len(fbref_players)) * 100, 2)
        self.results["consistency"]["fbref_to_understat_fuzzy_map_pct"] = pct_mapped
        self.results["consistency"]["total_fbref_players"] = len(fbref_players)

    def check_accuracy(self):
        logger.info("3/3 Uruchamianie testów Accuracy (Reconciliation)...")
        
        # Średnia różnica (wariancja) xG.
        # Dokładne połączenie wymaga przygotowania wspólnego klucza (Mecz + Zawodnik).
        # Ponieważ struktura zależy od formatu MultiIndex soccerdata, dla PoC notujemy placeholder.
        self.results["accuracy"]["xg_variance_info"] = (
            "Wymaga zaimplementowania słownika 'Golden Record' mapującego ID/nazwiska zawodników "
            "oraz ustandaryzowanych ID meczów między FBref a Understat przed obliczeniem wariancji (różnicy `fbref.xg - understat.xg`)."
        )

    def run_all(self):
        self.check_completeness()
        self.check_consistency()
        self.check_accuracy()
        return self.results

def save_dq_logs(results: dict, filepath: str = "dq_logs.json"):
    logger.info(f"Zapisywanie rezultatów DQ do: {filepath}")
    
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                logs = json.load(f)
            except json.JSONDecodeError:
                logs = []
    else:
        logs = []
        
    logs.append(results)
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=4, ensure_ascii=False)

def main():
    logger.info("=== Start potoku Soccer Data Quality Monitor (PoC) ===")
    
    # 1. Ekstrakcja
    # Dla wydajności w PoC pobieramy bieżący lub ostatni mniejszy zakres (np. sezon 23/24)
    extractor = SoccerDataExtractor(leagues="ENG-Premier League", seasons="2324")
    df_fbref = extractor.fetch_fbref_data()
    df_understat = extractor.fetch_understat_data()
    
    # 2. Profilowanie i metryki DQ
    profiler = DataQualityProfiler(df_fbref, df_understat)
    dq_results = profiler.run_all()
    
    # 3. Zapis
    save_dq_logs(dq_results, filepath="dq_metrics_history.json")
    
    # Podsumowanie dla użytkownika
    logger.info("=== Zakończono z sukcesem ===")
    print("\n[Wyniki Data Quality]:")
    print(json.dumps(dq_results, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
