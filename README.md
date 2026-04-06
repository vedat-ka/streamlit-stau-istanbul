# 🚦 Istanbul Stau-Analyse

Interaktives Streamlit-Dashboard zur Analyse der Verkehrsdichte in Istanbul.  
Datengrundlage: IBB Open Data Portal – **22 Mio. Messpunkte** von Dezember 2023 bis Januar 2025. Weil die Daten zu groß sin ist nur eine CSV Datei mit dabei, den rest hier herunter laden: https://data.ibb.gov.tr/dataset/hourly-traffic-density-data-set

> **Daten:** Alle `.csv`-Dateien im Ordner `data/` werden automatisch eingelesen. Neue Monatsdateien einfach in `data/` ablegen – beim nächsten App-Start werden **nur die neuen Dateien** in `traffic.duckdb` nachimportiert, bereits importierte Dateien werden übersprungen. Es ist **nicht** nötig, `traffic.duckdb` zu löschen.

---

## 🗄️ Warum DuckDB – Analyse im GB-Bereich ohne Kompromisse

Die Rohdaten umfassen **1,7 GB CSV** über 14 Monatsdateien mit über **22 Millionen Zeilen**.  
Klassische Ansätze stoßen hier schnell an ihre Grenzen:

| Ansatz | Problem |
|---|---|
| `pandas.read_csv()` | Lädt alles in den RAM → bei 1,7 GB oft nicht praktikabel, langsamer Start |
| SQLite | Zeilen müssen erst importiert werden, kein paralleles Lesen, langsame Aggregationen |
| Cloud-Datenbank | Netzwerklatenz, Kosten, Infrastrukturaufwand |

**DuckDB löst diese Probleme direkt:**

- **Zero-Copy CSV-Scan** – DuckDB liest die CSV-Dateien direkt mit `read_csv_auto('data/*.csv')`, ohne vorherigen Import oder Konvertierung.
- **Columnar Engine** – Daten werden spaltenweise verarbeitet; Aggregationen wie `AVG(AVERAGE_SPEED)` laufen nur über die relevante Spalte, nicht über den gesamten Datensatz.
- **Vektorisierte Ausführung** – Abfragen über Millionen Zeilen dauern typischerweise unter einer Sekunde, auch auf Consumer-Hardware.
- **Parallelisierung** – Nutzt automatisch alle CPU-Kerne für parallele Scans der 14 CSV-Dateien.
- **In-Process** – Kein separater Datenbankserver nötig; DuckDB läuft als Python-Bibliothek direkt im Streamlit-Prozess.
- **Streamlit-Cache** – Die DuckDB-Verbindung wird via `@st.cache_resource` einmal aufgebaut und wiederverwendet; jede Filteränderung löst nur eine neue SQL-Abfrage aus, kein erneutes Laden.

> **Ergebnis:** Die gesamte 1,7-GB-Datenbasis wird interaktiv abgefragt – Filteränderungen in der Sidebar aktualisieren alle Charts in Sekunden, ohne dass ein Datenbank-Backend oder ein leistungsstarker Server nötig ist.

---

## ⚡ Apache Arrow – Zero-Copy-Pipeline

Alle Abfrageergebnisse durchlaufen eine Arrow-basierte Pipeline, die unnötige Datenkopien eliminiert:

**Ohne Arrow (klassisch):**
```
DuckDB → serialisiert (CSV/intern) → Python deserialisiert → Pandas → NumPy
         ↑ Kopie                     ↑ Kopie                  ↑ Kopie
```

**Mit Arrow (aktuell):**
```
DuckDB → .to_arrow_table() → .to_pandas() → Plotly
          ↑ nativer Arrow-Buffer (shared memory für numerische Spalten)
```

Konkret in `db.py`: Jede Methode ruft `.to_arrow_table().to_pandas()` statt `.df()` auf.
DuckDB schreibt das Ergebnis direkt als Apache Arrow `RecordBatch` in den Speicher –
Pandas liest numerische Spalten (int, float) daraus **ohne zusätzliche Kopie**.

> **Kein `import pyarrow` nötig:** `.to_arrow_table()` ist eine Methode von DuckDB selbst.
> DuckDB erkennt automatisch, ob `pyarrow` installiert ist, und aktiviert dann intern den
> Zero-Copy-Pfad. `pyarrow` muss nur in `requirements.txt` stehen – ein eigener Import
> im Code ist nicht erforderlich.

| Schritt | Mit `.df()` | Mit `.to_arrow_table().to_pandas()` |
|---|---|---|
| DuckDB → Python | Serialisierung + Kopie | Arrow-Buffer, kein Serialize |
| Python → Pandas | Deserialisierung + Kopie | Zero-Copy für num. Spalten |
| CPU-Overhead | hoch bei großen Results | minimal |

> **Wo ist die DuckDB-Datenbank?** In der Datei `traffic.duckdb` im Projektverzeichnis (wird beim ersten Start automatisch aus den CSVs erstellt). DuckDB nutzt im persistenten Modus einen Buffer-Pool: nur die für eine Abfrage nötigen Seiten werden in den RAM geladen, der Rest bleibt auf der Festplatte. Ab dem zweiten Start entfällt der CSV-Scan komplett.

---

## Funktionen

| Bereich | Beschreibung |
|---|---|
| **KPI-Übersicht** | Gesamtmessungen, Ø-Geschwindigkeit, Stau-Anteil %, Ø Fahrzeuge pro Segment |
| **Monatlicher Trend** | Kombiniertes Linien-/Balkendiagramm: Ø-Geschwindigkeit & Stau-Anteil je Monat |
| **Wochentag-Heatmap** | Stau-Intensität nach Wochentag × Tagesstunde (24 × 7) |
| **Tagesverlauf** | Ø-Geschwindigkeit & Stau-Anteil stündlich über den Tag |
| **Geschwindigkeits-Verteilung** | Histogramm aller Messungen – Stau-Bereich rot hervorgehoben |
| **Top-50 Hotspots** | Tabelle der Geohash-Segmente mit dem höchsten Stau-Anteil |
| **Stau-Karte** | Interaktive Mapbox-Karte der Top-500 Stau-Hotspots (Farbe = Intensität) |
| **Fahrzeugaufkommen** | Balkendiagramm der monatlichen Fahrzeugzahlen |

**Sidebar-Filter:**
- Zeitraum (Datepicker)
- Stau-Schwelle: Ø-Geschwindigkeit in km/h (Standard: 20 km/h) – alle Charts reagieren live

---

## Projektstruktur

```
streamlit-stau-istanbul/
├── app.py                        # Streamlit-Hauptanwendung (UI & Charts)
├── db.py                         # Datenbankschicht (DuckDB, alle SQL-Abfragen)
├── requirements.txt              # Python-Abhängigkeiten
├── traffic.duckdb                # Persistente DuckDB-Datenbank (auto-generiert)
├── .gitignore
├── README.md
└── data/
    ├── traffic_density_202312.csv
    ├── traffic_density_202401.csv
    ├── ...
    └── traffic_density_202501.csv
```

---

## Setup – Virtuelle Umgebung

### 1. Repository klonen / Projektordner anlegen

```bash
git clone <repo-url>
cd streamlit-stau-istanbul
```

### 2. Virtuelle Umgebung erstellen

```bash
python3 -m venv .venv
```

### 3. Virtuelle Umgebung aktivieren

**Linux / macOS**
```bash
source .venv/bin/activate
```

**Windows (PowerShell)**
```powershell
.venv\Scripts\Activate.ps1
```

### 4. Abhängigkeiten installieren

```bash
pip install -r requirements.txt
```

### 5. Daten bereitstellen

CSV-Dateien im Format `traffic_density_YYYYMM.csv` in den Ordner `data/` legen.  
Pflichtfelder:

| Spalte | Typ | Beschreibung |
|---|---|---|
| `DATE_TIME` | DATETIME | Messzeitpunkt (stündlich) |
| `LATITUDE` | FLOAT | Breitengrad |
| `LONGITUDE` | FLOAT | Längengrad |
| `GEOHASH` | STRING | Geohash des Segments |
| `MINIMUM_SPEED` | INT | Mindestgeschwindigkeit (km/h) |
| `MAXIMUM_SPEED` | INT | Höchstgeschwindigkeit (km/h) |
| `AVERAGE_SPEED` | INT | Durchschnittsgeschwindigkeit (km/h) |
| `NUMBER_OF_VEHICLES` | INT | Anzahl Fahrzeuge im Segment |

### 6. App starten

```bash
streamlit run app.py
```

Die App ist dann unter **http://localhost:8501** erreichbar.

---

## Technologie-Stack

| Paket | Zweck |
|---|---|
| [Streamlit](https://streamlit.io) `≥ 1.35` | Web-UI & Interaktivität |
| [DuckDB](https://duckdb.org) `≥ 1.0` | Schnelle SQL-Abfragen direkt auf CSV |
| [Apache Arrow](https://arrow.apache.org/docs/python/) `≥ 14.0` | Zero-Copy-Datentransfer zwischen DuckDB und Pandas |
| [Plotly](https://plotly.com/python/) `≥ 5.22` | Interaktive Charts & Mapbox-Karte |
| [Pandas](https://pandas.pydata.org) `≥ 2.0` | DataFrame-Verarbeitung |

---

## Datenquelle

[IBB Açık Veri Portalı – Trafik Yoğunluğu](https://data.ibb.gov.tr)  
Lizenz: Open Data Commons Open Database License (ODbL)

---

## 🤖 Mögliche ML-Anwendungen mit diesen Daten

Die vorhandenen Features – **Ort (Geohash / LAT / LON), Zeit (Stunde, Wochentag, Monat), Geschwindigkeit (Min/Max/Avg) und Fahrzeuganzahl** – bieten eine solide Grundlage für verschiedene Machine-Learning-Modelle.

### Vorhersage-Anwendungen

**1. Stauvorhersage (Klassifikation)**
> „Wird es an diesem Segment in 1 h / 2 h Stau geben?"
- Input: Geohash + Wochentag + Stunde + historischer Durchschnitt
- Output: Stau / kein Stau (binär) oder Wahrscheinlichkeit
- Modell: XGBoost, LightGBM, Random Forest

**2. Geschwindigkeitsprognose (Regression)**
> „Wie schnell wird der Verkehr auf Route X um 8:30 Uhr sein?"
- Nützlich für Navigationssysteme (ETA-Berechnung)
- Modell: LSTM / GRU für Zeitreihen, Gradient Boosting

**3. Fahrzeugaufkommen-Prognose**
> „Wie viele Fahrzeuge sind morgen früh auf der Brücke?"
- Relevant für Mautplanung und Parkraum-Management
- Modell: Prophet, SARIMA, LSTM

---

### Optimierungs-Anwendungen

**4. Ampelschaltung optimieren**  
Stauvorhersage pro Segment → präventive Grünphasen-Verlängerung (Smart-City-Ansatz)

**5. Dynamische Routenempfehlung**  
Modell bewertet mehrere Routen mit vorhergesagter Verkehrsdichte – Grundlage für eine eigene Navigationslösung

**6. Ereignis-Impact-Analyse**  
Großveranstaltungen in Istanbul → vorhergesagter Staueffekt; Anomalie-Erkennung für ungewöhnliche Verkehrsmuster

---

### Stadtplanung & Reporting

**7. Hotspot-Clustering**  
Unüberwachtes Lernen (K-Means, DBSCAN) auf LAT/LON + Stau-Merkmalen → strukturell problematische Kreuzungen identifizieren

**8. Saisonale Muster-Erkennung**  
Ramadan, Sommerferien, Schulstart → automatisch erkannte wiederkehrende Verkehrsmuster

---

### Empfohlene Modelle je Aufgabe

| Ziel | Empfohlenes Modell |
|---|---|
| Stau Ja/Nein | XGBoost / LightGBM |
| Ø-Geschwindigkeit in X Stunden | LSTM, Prophet |
| Ähnliche Segmente finden | K-Means, DBSCAN |
| Anomalien erkennen | Isolation Forest, Autoencoder |

> **Wichtigste Ergänzung für ein gutes Modell:** Externe Daten hinzufügen – **Wetterdaten, Feiertage/Schulferien, Baustellen, Großveranstaltungen** – da diese Faktoren Stau stark beeinflussen und im aktuellen Datensatz fehlen.
