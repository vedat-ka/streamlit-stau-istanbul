"""
app.py – Streamlit-Dashboard für die Istanbul Stau-Analyse.

Einstiegspunkt der Anwendung. Instanziiert TrafficDashboard und ruft
run() auf. Alle Datenabfragen werden an TrafficDatabase (db.py) delegiert;
diese Datei enthält ausschließlich UI- und Visualisierungslogik.

Starten:
    streamlit run app.py
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path

from db import TrafficDatabase


# ── Streamlit-Seitenkonfiguration (muss vor allen anderen st.*-Aufrufen stehen)
st.set_page_config(
    page_title="Istanbul Stau-Analyse",
    page_icon="🚦",
    layout="wide",
)


@st.cache_resource
def _get_db() -> TrafficDatabase:
    """
    Fabrikfunktion für TrafficDatabase, gewrappt mit @st.cache_resource.

    Stellt sicher, dass die DuckDB-Verbindung nur einmal pro Server-Session
    aufgebaut wird und über alle Nutzer-Interaktionen hinweg erhalten bleibt.
    Der data/-Pfad wird relativ zur Position von app.py aufgelöst.

    Returns:
        TrafficDatabase: Initialisierte Datenbankinstanz mit registrierter View.
    """
    data_dir = Path(__file__).parent / "data"
    return TrafficDatabase(data_dir)


class TrafficDashboard:
    """
    Haupt-Dashboard-Klasse für die Istanbul Stau-Analyse.

    Kapselt die gesamte Streamlit-UI in klar getrennte render_*-Methoden.
    Jede Methode ist für genau einen visuellen Abschnitt verantwortlich
    und erhält ihre Daten über self.db (TrafficDatabase).

    Attributes:
        db (TrafficDatabase): Datenbankinstanz für alle SQL-Abfragen.
        d_min (pd.Timestamp): Frühestes Datum in den Daten.
        d_max (pd.Timestamp): Spätestes Datum in den Daten.
        d_from (pd.Timestamp): Vom Nutzer gewählter Filterzeitraum Start.
        d_to (pd.Timestamp): Vom Nutzer gewählter Filterzeitraum Ende.
        stau_threshold (int): Ø-Geschwindigkeitsschwelle für Stau in km/h.
        where (str): SQL-WHERE-Bedingung, abgeleitet aus d_from/d_to.
    """

    DOW_LABELS = ["Sonntag", "Montag", "Dienstag", "Mittwoch",
                  "Donnerstag", "Freitag", "Samstag"]

    def __init__(self, db: TrafficDatabase) -> None:
        """
        Initialisiert das Dashboard mit einer Datenbankinstanz.

        Liest die Datumsgrenzen aus den Daten aus, die später im Datepicker
        als Minimum und Maximum angezeigt werden.

        Args:
            db: Initialisierte TrafficDatabase-Instanz.
        """
        self.db = db
        self.d_min, self.d_max = db.get_date_bounds()

        # wird in render_sidebar() befüllt
        self.d_from: pd.Timestamp = self.d_min
        self.d_to: pd.Timestamp = self.d_max
        self.stau_threshold: int = 20
        self.where: str = ""

    # ── Sidebar ───────────────────────────────────────────────────────────────

    def render_sidebar(self) -> None:
        """
        Rendert die Sidebar mit Zeitraum-Datepicker und Stau-Schwellen-Slider.

        Schreibt die Nutzerauswahl in self.d_from, self.d_to, self.stau_threshold
        und leitet daraus self.where (SQL-WHERE-Klausel) ab, die von allen
        anderen render_*-Methoden verwendet wird.
        """
        st.sidebar.title("⚙️ Filter")

        date_range = st.sidebar.date_input(
            "Zeitraum",
            value=(pd.Timestamp(self.d_min).date(), pd.Timestamp(self.d_max).date()),
            min_value=pd.Timestamp(self.d_min).date(),
            max_value=pd.Timestamp(self.d_max).date(),
        )
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            self.d_from, self.d_to = date_range
        elif isinstance(date_range, (list, tuple)) and len(date_range) == 1:
            # Nutzer hat nur Startdatum gewählt – bis Ende des Datensatzes
            self.d_from = date_range[0]
            self.d_to = self.d_max
        else:
            self.d_from = self.d_to = date_range

        self.stau_threshold = st.sidebar.slider(
            "Stau-Schwelle (Ø-Geschwindigkeit km/h ≤)",
            min_value=5, max_value=60, value=20, step=5,
        )

        d_from_str = pd.Timestamp(self.d_from).strftime("%Y-%m-%d")
        d_to_str   = pd.Timestamp(self.d_to).strftime("%Y-%m-%d")
        self.where = (
            f"DATE_TIME >= '{d_from_str}' AND "
            f"DATE_TIME <= '{d_to_str} 23:59:59'"
        )

        st.sidebar.markdown("---")
        st.sidebar.caption(
            f"Datenquelle: IBB Open Data Portal  \n"
            f"Zeitraum: {self.d_min.strftime('%b %Y')} – {self.d_max.strftime('%b %Y')}"
        )

    # ── Titel & Beschreibung ──────────────────────────────────────────────────

    def render_header(self) -> None:
        """
        Rendert den Seitentitel und die aktuelle Filterübersicht als Untertitel.
        """
        st.title("🚦 Istanbul Stau-Analyse")
        st.markdown(
            f"Verkehrsdichte-Daten vom **{self.d_from}** bis **{self.d_to}** · "
            f"Stau-Definition: Ø-Geschwindigkeit ≤ **{self.stau_threshold} km/h**"
        )

    # ── KPI-Kacheln ───────────────────────────────────────────────────────────

    def render_kpis(self) -> None:
        """
        Zeigt vier Kennzahl-Kacheln: Gesamtmessungen, Ø-Geschwindigkeit,
        Stau-Anteil in % und Ø-Fahrzeuge pro Segment.

        Daten werden per get_kpis() aus der Datenbank abgefragt.
        """
        kpi = self.db.get_kpis(self.where, self.stau_threshold)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Messwerte gesamt",       f"{int(kpi['total_rows'][0]):,}")
        c2.metric("Ø Geschwindigkeit",      f"{kpi['avg_speed'][0]} km/h")
        c3.metric("Stau-Anteil",            f"{kpi['stau_pct'][0]} %")
        c4.metric("Ø Fahrzeuge / Segment",  f"{kpi['avg_vehicles'][0]}")

    # ── Monatlicher Trend ─────────────────────────────────────────────────────

    def render_monthly_trend(self) -> None:
        """
        Kombiniertes Linien-/Balkendiagramm: Ø-Geschwindigkeit (linke Achse)
        und Stau-Anteil in % (rechte Achse) pro Kalendermonat.

        Ermöglicht es, saisonale Muster und Trendveränderungen auf einen
        Blick zu erkennen.
        """
        st.subheader("📅 Monatlicher Trend")
        monthly = self.db.get_monthly(self.where, self.stau_threshold)

        fig = go.Figure()
        fig.add_bar(
            x=monthly["month_ts"], y=monthly["stau_pct"],
            name="Stau-Anteil %", yaxis="y2",
            marker_color="rgba(255,80,80,0.45)",
        )
        fig.add_scatter(
            x=monthly["month_ts"], y=monthly["avg_speed"],
            name="Ø Geschwindigkeit km/h", mode="lines+markers",
            line=dict(color="#1f77b4", width=2),
        )
        fig.update_layout(
            yaxis=dict(title="km/h"),
            yaxis2=dict(title="Stau %", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", y=-0.18),
            margin=dict(t=20, b=0),
            height=320,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Wochentag-Heatmap ─────────────────────────────────────────────────────

    def render_heatmap(self) -> None:
        """
        Heatmap des Stau-Anteils für jede Kombination aus Wochentag (7 Zeilen)
        und Tagesstunde (24 Spalten).

        Deckt Rush-Hour-Muster und verkehrsreiche Wochentage auf.
        Zeilen werden nach DOW-Index (0=Sonntag) in deutsche Namen übersetzt.
        """
        st.subheader("🗓️ Stau nach Wochentag & Stunde")
        heatmap = self.db.get_heatmap(self.where, self.stau_threshold)

        pivot = heatmap.pivot(index="dow", columns="hour", values="stau_pct").fillna(0)
        pivot.index = [self.DOW_LABELS[i] for i in pivot.index]

        fig = px.imshow(
            pivot,
            labels=dict(x="Stunde", y="Wochentag", color="Stau %"),
            color_continuous_scale="Reds",
            aspect="auto",
            text_auto=".1f",
        )
        fig.update_layout(margin=dict(t=20, b=0), height=320)
        st.plotly_chart(fig, use_container_width=True)

    # ── Tagesverlauf ──────────────────────────────────────────────────────────

    def render_hourly_trend(self) -> None:
        """
        Flächendiagramm der Ø-Geschwindigkeit und gestrichelte Linie des
        Stau-Anteils über alle 24 Stunden des Tages (gemittelt über den
        gesamt gewählten Zeitraum).

        Visualisiert Morgen- und Abend-Rush-Hour sowie ruhige Nachtstunden.
        """
        st.subheader("🕐 Tagesverlauf (Ø Geschwindigkeit)")
        hourly = self.db.get_hourly(self.where, self.stau_threshold)

        fig = go.Figure()
        fig.add_scatter(
            x=hourly["hour"], y=hourly["avg_speed"],
            mode="lines+markers", name="Ø km/h",
            line=dict(color="#2196F3", width=2.5),
            fill="tozeroy", fillcolor="rgba(33,150,243,0.12)",
        )
        fig.add_scatter(
            x=hourly["hour"], y=hourly["stau_pct"],
            mode="lines+markers", name="Stau %", yaxis="y2",
            line=dict(color="#e53935", width=2, dash="dot"),
        )
        fig.update_layout(
            xaxis=dict(title="Stunde", tickmode="linear", dtick=2),
            yaxis=dict(title="km/h"),
            yaxis2=dict(title="Stau %", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", y=-0.22),
            margin=dict(t=20, b=0),
            height=320,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Geschwindigkeits-Verteilung ───────────────────────────────────────────

    def render_speed_distribution(self) -> None:
        """
        Balkendiagramm der Messungen je 10-km/h-Geschwindigkeitsklasse.

        Klassen unterhalb der Stau-Schwelle werden rot eingefärbt,
        Klassen darüber blau – so ist der Stau-Anteil sofort sichtbar.
        """
        st.subheader("📊 Geschwindigkeits-Verteilung")
        df = self.db.get_speed_distribution(self.where)
        df["label"] = (
            df["speed_bin"].astype(int).astype(str)
            + "–"
            + (df["speed_bin"].astype(int) + 9).astype(str)
        )
        df["color"] = df["speed_bin"].apply(
            lambda s: "#e53935" if s <= self.stau_threshold else "#42a5f5"
        )

        fig = px.bar(
            df, x="label", y="cnt",
            color="color", color_discrete_map="identity",
            labels={"label": "km/h", "cnt": "Messungen"},
        )
        fig.update_layout(showlegend=False, margin=dict(t=20, b=0), height=320)
        st.plotly_chart(fig, use_container_width=True)

    # ── Hotspot-Tabelle ───────────────────────────────────────────────────────

    def render_hotspot_table(self) -> None:
        """
        Tabellarische Übersicht der 50 Geohash-Segmente mit dem höchsten
        Stau-Anteil im gewählten Zeitraum.

        Nur Segmente mit mehr als 100 Messungen werden berücksichtigt,
        um statistische Ausreißer bei seltenen Strecken auszuschließen.
        """
        st.subheader("🔥 Top-50 Stau-Hotspots")
        hotspots = self.db.get_hotspots(self.where, self.stau_threshold)
        st.dataframe(
            hotspots[["GEOHASH", "avg_speed", "stau_pct", "messungen"]].rename(
                columns={
                    "GEOHASH":   "Geohash",
                    "avg_speed": "Ø km/h",
                    "stau_pct":  "Stau %",
                    "messungen": "Messungen",
                }
            ),
            use_container_width=True,
            height=380,
        )

    # ── Karte ─────────────────────────────────────────────────────────────────

    def render_map(self) -> None:
        """
        Interaktive Mapbox-Karte der Top-500 Stau-Hotspots.

        Jeder Punkt repräsentiert ein Geohash-Segment. Farbe und Größe
        kodieren den Stau-Anteil (Rot = hoher Stau, Grün = freie Fahrt).
        Im Tooltip werden Ø-Geschwindigkeit, Stau-Anteil und Messungsanzahl
        angezeigt. Basiskarte: carto-positron (kein API-Key nötig).
        """
        st.subheader("🗺️ Karte: Stau-Hotspots (Top 500)")
        map_data = self.db.get_map_data(self.where, self.stau_threshold)

        fig = px.scatter_map(
            map_data,
            lat="lat", lon="lon",
            color="stau_pct",
            size="stau_pct",
            color_continuous_scale="RdYlGn_r",
            range_color=[0, 100],
            size_max=18,
            zoom=10,
            center={"lat": 41.015, "lon": 28.979},
            hover_data={"avg_speed": True, "stau_pct": True, "messungen": True},
            labels={"stau_pct": "Stau %", "avg_speed": "Ø km/h"},
            map_style="carto-positron",
            height=420,
        )
        fig.update_layout(margin=dict(t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

    # ── Fahrzeugaufkommen ─────────────────────────────────────────────────────

    def render_vehicles_monthly(self) -> None:
        """
        Balkendiagramm der absoluten Fahrzeugzahlen pro Kalendermonat.

        Ermöglicht es, Verkehrswachstum, saisonale Einbrüche (z.B. Sommer)
        oder Sondereffekte wie Feiertage über den gesamten Zeitraum zu erkennen.
        """
        st.subheader("🚗 Fahrzeugaufkommen pro Monat")
        df = self.db.get_vehicles_monthly(self.where)

        fig = px.bar(
            df, x="month_ts", y="total_vehicles",
            labels={"month_ts": "Monat", "total_vehicles": "Fahrzeuge (gesamt)"},
            color_discrete_sequence=["#7986CB"],
        )
        fig.update_layout(margin=dict(t=10, b=0), height=260)
        st.plotly_chart(fig, use_container_width=True)

    # ── Haupt-Render-Schleife ─────────────────────────────────────────────────

    def run(self) -> None:
        """
        Orchestriert den vollständigen Seitenaufbau des Dashboards.

        Reihenfolge:
        1. Sidebar (setzt self.where und self.stau_threshold)
        2. Header
        3. KPI-Kacheln
        4. Monatlicher Trend | Wochentag-Heatmap
        5. Tagesverlauf | Geschwindigkeits-Verteilung
        6. Hotspot-Tabelle | Karte
        7. Fahrzeugaufkommen
        """
        self.render_sidebar()
        self.render_header()
        self.render_kpis()

        st.markdown("---")
        col_left, col_right = st.columns(2)
        with col_left:
            self.render_monthly_trend()
        with col_right:
            self.render_heatmap()

        col_l2, col_r2 = st.columns(2)
        with col_l2:
            self.render_hourly_trend()
        with col_r2:
            self.render_speed_distribution()

        st.markdown("---")
        col_l3, col_r3 = st.columns([1, 2])
        with col_l3:
            self.render_hotspot_table()
        with col_r3:
            self.render_map()

        st.markdown("---")
        self.render_vehicles_monthly()


# ── Einstiegspunkt ─────────────────────────────────────────────────────────────
db = _get_db()
dashboard = TrafficDashboard(db)
dashboard.run()
