# ⚽ Football Data Quality (football-dq)

Aplikacja do monitorowania i zarządzania jakością danych piłkarskich (Data Quality).

## Architektura

```
football-dq/
├── src/                        # Kod źródłowy aplikacji
│   ├── extractors/             # Ekstrakcja danych z zewnętrznych źródeł
│   ├── quality/                # Silnik Data Quality (reguły, metryki, profile)
│   ├── reconciliation/         # Uzgadnianie danych między źródłami
│   ├── golden_record/          # Rekordy referencyjne (mapowania encji)
│   ├── storage/                # Warstwa persystencji (DB, pliki)
│   ├── api/                    # REST API (FastAPI)
│   ├── dashboard/              # Frontend / dashboardy DQ
│   └── config/                 # Konfiguracja aplikacji
├── pipelines/                  # Definicje pipeline'ów ETL/DQ
├── rules/                      # Reguły DQ jako pliki YAML/JSON
├── tests/                      # Testy jednostkowe i integracyjne
├── scripts/                    # Skrypty pomocnicze (migracje, seedy)
├── data/                       # Dane lokalne (sample, cache)
├── docs/                       # Dokumentacja projektu
├── notebooks/                  # Jupyter notebooks do eksploracji
└── poc_dq_pipeline.py          # Oryginalny PoC
```

## Wymiary Data Quality

| Wymiar | Opis | Przykład |
|--------|------|---------|
| **Completeness** | Czy dane są kompletne? | Brakujące wartości xG |
| **Consistency** | Czy dane są spójne między źródłami? | Fuzzy matching zawodników |
| **Accuracy** | Czy dane są poprawne? | Rekoncyliacja xG FBref vs Understat |
| **Timeliness** | Czy dane są aktualne? | Opóźnienia w aktualizacji statystyk |
| **Uniqueness** | Czy nie ma duplikatów? | Zduplikowane rekordy meczowe |
| **Validity** | Czy dane spełniają reguły biznesowe? | xG w zakresie [0, ~7] |

## Quick Start

```bash
# Instalacja zależności
pip install -r requirements.txt

# Uruchomienie PoC
python poc_dq_pipeline.py

# Uruchomienie API
uvicorn src.api.main:app --reload

# Testy
pytest tests/
```
