"""
db.py – Datenbankschicht für die Istanbul Stau-Analyse.

Kapselt alle DuckDB-Zugriffe in der Klasse TrafficDatabase.
app.py importiert ausschließlich diese Klasse und arbeitet nur
mit den zurückgegebenen DataFrames – kein SQL-Code in der UI-Schicht.
"""

import duckdb
import re
import pandas as pd
from pathlib import Path


class TrafficDatabase:
    """
    Verwaltet eine persistente DuckDB-Verbindung (Datei auf der Festplatte)
    und stellt alle SQL-Abfragen für das Stau-Dashboard als Methoden bereit.

    Beim ersten Start werden die CSV-Dateien einmalig in eine DuckDB-Tabelle
    importiert (traffic.duckdb neben app.py). Ab dem zweiten Start wird nur
    noch die bestehende Datei geöffnet – kein CSV-Scan, deutlich weniger RAM,
    da DuckDB einen Buffer-Pool nutzt und nicht alles auf einmal lädt.

    Attributes:
        con (duckdb.DuckDBPyConnection): Aktive DuckDB-Verbindung.
    """

    def __init__(self, data_dir: Path, db_path: Path | None = None) -> None:
        """
        Öffnet (oder erstellt) die persistente DuckDB-Datei.

        Beim ersten Aufruf werden alle CSV-Dateien aus data_dir einmalig
        in die Tabelle 'traffic' importiert. Beim zweiten Aufruf wird die
        bestehende Datei nur geöffnet.

        Args:
            data_dir: Verzeichnis mit den traffic_density_*.csv Dateien.
                      Wird nur beim ersten Start (Import) benötigt.
            db_path:  Pfad zur .duckdb-Datei. Standard: data_dir/../traffic.duckdb

        Raises:
            FileNotFoundError: Wenn kein CSV gefunden wird und die DB noch
                               nicht existiert.
        """
        self.db_file = db_path if db_path is not None else data_dir.parent / "traffic.duckdb"
        self.con = duckdb.connect(str(self.db_file))
        self._init_table(data_dir)

    def _init_table(self, data_dir: Path) -> None:
        """
        Synchronisiert die DuckDB-Tabelle 'traffic' mit dem Inhalt von data_dir.

        Beim Start werden drei Fälle behandelt:
        - Erster Start: Tabellen anlegen, alle CSVs importieren.
        - Neue Datei in data/: nur diese wird per INSERT INTO nachgeladen.
        - Datei aus data/ gelöscht: deren Zeilen werden per DELETE entfernt.
          Der year+month-Wert wird direkt aus dem Dateinamen (YYYYMM) geparst,
          sodass keine zusätzliche source_file-Spalte nötig ist.

        Args:
            data_dir: CSV-Quellverzeichnis.
        """
        # Hilfstabelle für Tracking anlegen (einmalig)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS imported_files (filename VARCHAR PRIMARY KEY)
        """)

        # traffic-Tabelle anlegen falls noch nicht vorhanden
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS traffic (
                DATE_TIME          TIMESTAMP,
                LATITUDE           DOUBLE,
                LONGITUDE          DOUBLE,
                GEOHASH            VARCHAR,
                MINIMUM_SPEED      INTEGER,
                MAXIMUM_SPEED      INTEGER,
                AVERAGE_SPEED      INTEGER,
                NUMBER_OF_VEHICLES INTEGER,
                year               INTEGER,
                month              INTEGER,
                hour               INTEGER,
                dow                INTEGER,
                month_ts           TIMESTAMP,
                day_ts             TIMESTAMP
            )
        """)

        csv_files = sorted(data_dir.glob("*.csv"))
        on_disk = {f.name for f in csv_files}

        already = {
            row[0]
            for row in self.con.execute("SELECT filename FROM imported_files").fetchall()
        }

        # ── gelöschte Dateien: Zeilen aus traffic entfernen ────────────────────
        removed = already - on_disk
        for filename in removed:
            m = re.search(r"(\d{4})(\d{2})", filename)
            if m:
                y, mo = int(m.group(1)), int(m.group(2))
                self.con.execute(
                    "DELETE FROM traffic WHERE year = ? AND month = ?", [y, mo]
                )
            self.con.execute(
                "DELETE FROM imported_files WHERE filename = ?", [filename]
            )

        # ── neue Dateien: importieren ──────────────────────────────────────────
        new_files = [f for f in csv_files if f.name not in already]
        for csv_file in new_files:
            self.con.execute(f"""
                INSERT INTO traffic
                SELECT
                    DATE_TIME,
                    LATITUDE,
                    LONGITUDE,
                    GEOHASH,
                    MINIMUM_SPEED,
                    MAXIMUM_SPEED,
                    AVERAGE_SPEED,
                    NUMBER_OF_VEHICLES,
                    EXTRACT(year  FROM DATE_TIME)::INT  AS year,
                    EXTRACT(month FROM DATE_TIME)::INT  AS month,
                    EXTRACT(hour  FROM DATE_TIME)::INT  AS hour,
                    EXTRACT(dow   FROM DATE_TIME)::INT  AS dow,
                    DATE_TRUNC('month', DATE_TIME)      AS month_ts,
                    DATE_TRUNC('day',   DATE_TIME)      AS day_ts
                FROM read_csv_auto('{csv_file}')
            """)
            self.con.execute(
                "INSERT INTO imported_files VALUES (?)", [csv_file.name]
            )

        if not csv_files and not already:
            raise FileNotFoundError(f"Keine CSV-Dateien in '{data_dir}' gefunden.")


    def get_date_bounds(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        """
        Gibt den frühesten und spätesten Messzeitpunkt der Daten zurück.

        Wird beim Start der App genutzt, um den Datepicker-Bereich dynamisch
        zu befüllen. Neue CSV-Dateien werden damit automatisch berücksichtigt.

        Returns:
            tuple: (d_min, d_max) als pd.Timestamp.
        """
        row = self.con.execute(
            "SELECT MIN(DATE_TIME)::DATE, MAX(DATE_TIME)::DATE FROM traffic"
        ).fetchone()
        return pd.Timestamp(row[0]), pd.Timestamp(row[1])

    def get_kpis(self, where: str, stau_threshold: int) -> pd.DataFrame:
        """
        Berechnet die vier Kennzahlen für die KPI-Kacheln.

        Args:
            where: SQL-WHERE-Bedingung für den Zeitraumfilter.
            stau_threshold: Geschwindigkeitsschwelle in km/h für Stau-Erkennung.

        Returns:
            DataFrame mit Spalten:
            total_rows, avg_speed, stau_pct, avg_vehicles, total_vehicles.
        """
        return self.con.execute(f"""
            SELECT
                COUNT(*)                                                AS total_rows,
                ROUND(AVG(AVERAGE_SPEED), 1)                           AS avg_speed,
                ROUND(SUM(CASE WHEN AVERAGE_SPEED <= {stau_threshold}
                               THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS stau_pct,
                ROUND(AVG(NUMBER_OF_VEHICLES), 1)                      AS avg_vehicles,
                SUM(NUMBER_OF_VEHICLES)                                AS total_vehicles
            FROM traffic
            WHERE {where}
        """).to_arrow_table().to_pandas()

    def get_monthly(self, where: str, stau_threshold: int) -> pd.DataFrame:
        """
        Aggregiert Ø-Geschwindigkeit, Stau-Anteil und Fahrzeugzahl pro Monat.

        Args:
            where: SQL-WHERE-Bedingung für den Zeitraumfilter.
            stau_threshold: Stau-Schwelle in km/h.

        Returns:
            DataFrame mit Spalten: month_ts, avg_speed, stau_pct, total_vehicles.
            Sortiert nach month_ts aufsteigend.
        """
        return self.con.execute(f"""
            SELECT
                month_ts,
                ROUND(AVG(AVERAGE_SPEED), 1)                           AS avg_speed,
                ROUND(SUM(CASE WHEN AVERAGE_SPEED <= {stau_threshold}
                               THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS stau_pct,
                SUM(NUMBER_OF_VEHICLES)                                AS total_vehicles
            FROM traffic
            WHERE {where}
            GROUP BY month_ts
            ORDER BY month_ts
        """).to_arrow_table().to_pandas()

    def get_heatmap(self, where: str, stau_threshold: int) -> pd.DataFrame:
        """
        Berechnet den Stau-Anteil je Kombination aus Wochentag und Stunde.

        Ergebnis dient als Grundlage für die 7×24-Heatmap.

        Args:
            where: SQL-WHERE-Bedingung für den Zeitraumfilter.
            stau_threshold: Stau-Schwelle in km/h.

        Returns:
            DataFrame mit Spalten: dow (0–6), hour (0–23), stau_pct.
        """
        return self.con.execute(f"""
            SELECT
                dow,
                hour,
                ROUND(SUM(CASE WHEN AVERAGE_SPEED <= {stau_threshold}
                               THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS stau_pct
            FROM traffic
            WHERE {where}
            GROUP BY dow, hour
            ORDER BY dow, hour
        """).to_arrow_table().to_pandas()

    def get_hourly(self, where: str, stau_threshold: int) -> pd.DataFrame:
        """
        Aggregiert Ø-Geschwindigkeit und Stau-Anteil je Tagesstunde.

        Wird für den Tagesverlaufs-Chart verwendet.

        Args:
            where: SQL-WHERE-Bedingung für den Zeitraumfilter.
            stau_threshold: Stau-Schwelle in km/h.

        Returns:
            DataFrame mit Spalten: hour, avg_speed, stau_pct.
            Sortiert nach hour aufsteigend.
        """
        return self.con.execute(f"""
            SELECT
                hour,
                ROUND(AVG(AVERAGE_SPEED), 1)                           AS avg_speed,
                ROUND(SUM(CASE WHEN AVERAGE_SPEED <= {stau_threshold}
                               THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS stau_pct
            FROM traffic
            WHERE {where}
            GROUP BY hour
            ORDER BY hour
        """).to_arrow_table().to_pandas()

    def get_speed_distribution(self, where: str) -> pd.DataFrame:
        """
        Zählt Messungen je 10-km/h-Geschwindigkeitsklasse.

        Args:
            where: SQL-WHERE-Bedingung für den Zeitraumfilter.

        Returns:
            DataFrame mit Spalten: speed_bin (untere Grenze), cnt.
            Sortiert nach speed_bin aufsteigend.
        """
        return self.con.execute(f"""
            SELECT
                FLOOR(AVERAGE_SPEED / 10) * 10  AS speed_bin,
                COUNT(*)                         AS cnt
            FROM traffic
            WHERE {where}
            GROUP BY speed_bin
            ORDER BY speed_bin
        """).to_arrow_table().to_pandas()

    def get_hotspots(self, where: str, stau_threshold: int, limit: int = 50) -> pd.DataFrame:
        """
        Ermittelt die Geohash-Segmente mit dem höchsten Stau-Anteil.

        Segmente mit weniger als 100 Messungen werden ausgeschlossen,
        um statistisch nicht aussagekräftige Einträge zu vermeiden.

        Args:
            where: SQL-WHERE-Bedingung für den Zeitraumfilter.
            stau_threshold: Stau-Schwelle in km/h.
            limit: Maximale Anzahl zurückgegebener Segmente (Standard: 50).

        Returns:
            DataFrame mit Spalten: GEOHASH, lat, lon, messungen, avg_speed, stau_pct.
            Sortiert nach stau_pct absteigend.
        """
        return self.con.execute(f"""
            SELECT
                GEOHASH,
                ROUND(AVG(LATITUDE),  5) AS lat,
                ROUND(AVG(LONGITUDE), 5) AS lon,
                COUNT(*)                                                AS messungen,
                ROUND(AVG(AVERAGE_SPEED), 1)                           AS avg_speed,
                ROUND(SUM(CASE WHEN AVERAGE_SPEED <= {stau_threshold}
                               THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS stau_pct
            FROM traffic
            WHERE {where}
            GROUP BY GEOHASH
            HAVING COUNT(*) > 100
            ORDER BY stau_pct DESC
            LIMIT {limit}
        """).to_arrow_table().to_pandas()

    def get_map_data(self, where: str, stau_threshold: int, limit: int = 500) -> pd.DataFrame:
        """
        Liefert Koordinaten und Stau-Anteil je Geohash für die Kartenansicht.

        Im Gegensatz zu get_hotspots ist die HAVING-Grenze niedriger (>50),
        um auch weniger frequentierte Segmente auf der Karte anzuzeigen.

        Args:
            where: SQL-WHERE-Bedingung für den Zeitraumfilter.
            stau_threshold: Stau-Schwelle in km/h.
            limit: Maximale Anzahl Kartenpunkte (Standard: 500).

        Returns:
            DataFrame mit Spalten: lat, lon, stau_pct, avg_speed, messungen.
            Sortiert nach stau_pct absteigend.
        """
        return self.con.execute(f"""
            SELECT
                ROUND(AVG(LATITUDE),  5) AS lat,
                ROUND(AVG(LONGITUDE), 5) AS lon,
                ROUND(SUM(CASE WHEN AVERAGE_SPEED <= {stau_threshold}
                               THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS stau_pct,
                ROUND(AVG(AVERAGE_SPEED), 1)                             AS avg_speed,
                COUNT(*)                                                 AS messungen
            FROM traffic
            WHERE {where}
            GROUP BY GEOHASH
            HAVING COUNT(*) > 50
            ORDER BY stau_pct DESC
            LIMIT {limit}
        """).to_arrow_table().to_pandas()

    def get_vehicles_monthly(self, where: str) -> pd.DataFrame:
        """
        Aggregiert das Fahrzeugaufkommen pro Monat.

        Args:
            where: SQL-WHERE-Bedingung für den Zeitraumfilter.

        Returns:
            DataFrame mit Spalten: month_ts, avg_vehicles, total_vehicles.
            Sortiert nach month_ts aufsteigend.
        """
        return self.con.execute(f"""
            SELECT
                month_ts,
                ROUND(AVG(NUMBER_OF_VEHICLES), 2)  AS avg_vehicles,
                SUM(NUMBER_OF_VEHICLES)             AS total_vehicles
            FROM traffic
            WHERE {where}
            GROUP BY month_ts
            ORDER BY month_ts
        """).to_arrow_table().to_pandas()
