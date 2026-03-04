# Reguły Data Quality
# Pliki YAML definiujące reguły jakości danych.
# Każdy plik odpowiada jednemu wymiarowi DQ lub źródłu danych.
#
# Przykład struktury reguły:
#
# rules:
#   - name: xg_not_null
#     dimension: completeness
#     column: xg
#     check: not_null
#     severity: critical
#
#   - name: xg_range
#     dimension: validity
#     column: xg
#     check: between
#     params: { min: 0.0, max: 7.0 }
#     severity: warning
