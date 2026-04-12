"""Million Songs: Genres, Lyrics & Trends — Dashboard."""

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import reverse_geocoder as rg
import streamlit as st
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from PIL.ImageColor import getrgb
from tidepool import COLORWAY, set_plotly_template

COUNTRIES = pd.read_csv(Path(__file__).parent / "iso3_countries.csv")
_iso_lookup = COUNTRIES.set_index("iso2")

load_dotenv()
set_plotly_template()

st.set_page_config(page_title="Million Songs", layout="wide", page_icon="🎵")
st.title("Million Songs: Genres, Lyrics & Trends")


@st.cache_data(ttl=3600)
def query_bq(sql: str):
    """Run a query against BigQuery and return a DataFrame."""
    client = bigquery.Client(project=os.environ.get("GCP_PROJECT_ID"))
    return client.query(sql).to_dataframe()


try:
    features_by_year = query_bq(
        # sql
        "SELECT * FROM million_songs.mrt_audio_features_by_year ORDER BY year",
    )
    features_by_year_genre = query_bq(
        # sql
        "SELECT * FROM million_songs.mrt_audio_features_by_year_genre ORDER BY year, genre",
    )
    genre_fingerprints = query_bq(
        # sql
        "SELECT * FROM million_songs.mrt_genre_audio_fingerprints",
    )
    genre_location = query_bq(
        # sql
        "SELECT * FROM million_songs.mrt_genre_by_location",
    )
    top_words = query_bq(
        # sql
        "SELECT * FROM million_songs.mrt_top_words_by_genre WHERE word_rank <= 20",
    )
    lyrical_diversity = query_bq(
        # sql
        "SELECT * FROM million_songs.mrt_lyrical_diversity_by_genre",
    )
except NotFound:
    st.info(
        "Waiting for pipeline to finish. Reload this page once the pipeline completes.",
    )
    st.stop()


st.header("Audio Features Over Time")

genre_list = sorted(features_by_year_genre["genre"].unique())
selected_genre = st.selectbox("Genre", ["All", *genre_list], key="time_genre")

feature_options = {
    "Tempo": ("norm_tempo", "avg_tempo", "norm_std_tempo", "BPM"),
    "Loudness": ("norm_loudness", "avg_loudness", "norm_std_loudness", "dB"),
    "Duration": ("norm_duration", "avg_duration", "norm_std_duration", "sec"),
    "Hotttnesss": ("norm_hotttnesss", "avg_hotttnesss", "norm_std_hotttnesss", ""),
    "% Major Key": ("pct_major", "pct_major", "std_mode", "pct"),
}


colors = COLORWAY

if selected_genre == "All":
    source = features_by_year
else:
    source = features_by_year_genre[features_by_year_genre["genre"] == selected_genre]

df_time = source[(source["year"] >= 1920) & (source["track_count"] >= 50)].copy()

fig_time = go.Figure()
for i, (name, (norm_col, raw_col, std_col, unit)) in enumerate(feature_options.items()):
    color = colors[i % len(colors)]

    fig_time.add_trace(
        go.Scatter(
            x=df_time["year"],
            y=df_time[norm_col],
            name=name,
            mode="lines",
            line={"color": color},
            legendgroup=name,
            customdata=df_time[[raw_col, "track_count"]].values,
            hovertemplate=(
                f"<b>{name}</b><br>"
                f"Year: %{{x}}<br>"
                + (
                    "Value: %{customdata[0]:.1%}<br>"
                    if unit == "pct"
                    else f"Value: %{{customdata[0]:.1f}} {unit}<br>"
                )
                + "Tracks: %{customdata[1]}<br>"
                "<extra></extra>"
            ),
        ),
    )

    # CI band (mean ± 1.96 * std / sqrt(n))
    if std_col in df_time.columns:
        ci = df_time[[norm_col, std_col, "track_count", "year"]].dropna()
        se = 1.96 * ci[std_col] / (ci["track_count"] ** 0.5)
        upper = ci[norm_col] + se
        lower = (ci[norm_col] - se).clip(0, 1)

        rgb = getrgb(color)
        fill_color = f"rgba({rgb[0]},{rgb[1]},{rgb[2]},0.15)"

        fig_time.add_trace(
            go.Scatter(
                x=ci["year"],
                y=upper,
                mode="lines",
                line={"width": 0},
                legendgroup=name,
                showlegend=False,
                hoverinfo="skip",
            ),
        )
        fig_time.add_trace(
            go.Scatter(
                x=ci["year"],
                y=lower,
                mode="lines",
                line={"width": 0},
                fill="tonexty",
                fillcolor=fill_color,
                legendgroup=name,
                showlegend=False,
                hoverinfo="skip",
            ),
        )

fig_time.update_layout(
    xaxis_title="Year",
    yaxis={"visible": False, "rangemode": "tozero"},
    legend={
        "font": {"size": 14},
        "orientation": "h",
        "yanchor": "top",
        "y": -0.15,
        "xanchor": "center",
        "x": 0.5,
        "entrywidth": 100,
    },
    margin={"l": 30, "r": 30, "t": 5},
    paper_bgcolor="#fafafa",
    plot_bgcolor="#fafafa",
)
st.plotly_chart(fig_time, theme=None)


st.header("Genre Audio Fingerprints")
radar_features = [
    "norm_tempo",
    "norm_loudness",
    "norm_duration",
    "norm_hotttnesss",
    "norm_pct_major",
]
radar_labels = ["Tempo", "Loudness", "Duration", "Hotttnesss", "% Major"]

selected_genres = st.multiselect(
    "Select genres to compare",
    genre_fingerprints["genre"].tolist(),
    default=["Pop", "Electronic", "Jazz", "Metal"],
)

fig_radar = go.Figure()
for i, genre in enumerate(selected_genres):
    row = genre_fingerprints[genre_fingerprints["genre"] == genre].iloc[0]
    values = [row[f] for f in radar_features]
    values.append(values[0])  # close the polygon
    color = colors[i % len(colors)]
    rgb = getrgb(color)
    fill_color = f"rgba({rgb[0]},{rgb[1]},{rgb[2]},0.1)"

    raw_cols = [
        "avg_tempo",
        "avg_loudness",
        "avg_duration",
        "avg_hotttnesss",
        "norm_pct_major",
    ]
    raw_values = [row[c] for c in raw_cols]
    raw_values.append(raw_values[0])
    units = ["BPM", "dB", "sec", "", "%"]
    hover_texts = []
    for j, label in enumerate(radar_labels):
        val = raw_values[j]
        if units[j] == "%":
            hover_texts.append(f"Avg {label}: {val:.1%}")
        else:
            hover_texts.append(f"Avg {label}: {val:.1f} {units[j]}")
    hover_texts.append(hover_texts[0])

    fig_radar.add_trace(
        go.Scatterpolar(
            r=values,
            theta=[*radar_labels, radar_labels[0]],
            name=genre,
            fill="toself",
            fillcolor=fill_color,
            line={"color": color, "width": 2},
            hoveron="points",
            text=hover_texts,
            hovertemplate=f"<b>{genre}</b><br>%{{text}}<extra></extra>",
        ),
    )

fig_radar.update_layout(
    polar={
        "radialaxis": {
            "visible": True,
            "showticklabels": False,
            "showline": False,
            "ticks": "",
            "gridcolor": "#e0e0e0",
            "range": [0, 1],
        },
        "angularaxis": {"tickfont": {"size": 14}},
    },
    margin={"t": 30, "b": 30, "l": 0, "r": 0},
    legend={"font": {"size": 14}},
    font={"size": 14},
    paper_bgcolor="#fafafa",
)
st.plotly_chart(fig_radar, theme=None)


col_words, col_diversity = st.columns(2, gap="large")

with col_words:
    st.header("Top Words by Genre")
    genre_choice = st.selectbox(
        "Select genre",
        ["All", *sorted(top_words["genre"].unique())],
        key="words_genre",
    )
    min_len = st.slider("Min word length", 2, 5, 3, key="word_len")
    words_filtered = top_words[top_words["word"].str.len() >= min_len]
    if genre_choice == "All":
        # Aggregate across all genres
        genre_words = (
            words_filtered.groupby("word")["total_count"]
            .sum()
            .reset_index()
            .sort_values("total_count", ascending=False)
            .head(15)
        )
    else:
        genre_words = (
            words_filtered[words_filtered["genre"] == genre_choice]
            .sort_values("word_rank")
            .head(15)
        )

    fig_words = px.bar(
        genre_words,
        x="total_count",
        y="word",
        orientation="h",
    )
    fig_words.update_layout(
        yaxis={"autorange": "reversed"},
        margin={"r": 20, "t": 20, "b": 50, "l": 60},
        paper_bgcolor="#fafafa",
        plot_bgcolor="#fafafa",
    )
    st.plotly_chart(fig_words, width="stretch", theme=None)

with col_diversity:
    st.header("Lyrical Diversity by Genre")
    metric = st.radio(
        "Metric",
        ["avg_type_token_ratio", "avg_vocab_size", "avg_total_words"],
        format_func=lambda x: {
            "avg_type_token_ratio": "Vocabulary Richness (unique/total words)",
            "avg_vocab_size": "Unique Words per Song",
            "avg_total_words": "Total Words per Song",
        }[x],
    )
    st.markdown("")
    st.markdown("")
    st.markdown("")

    # Add "All" row as weighted average
    all_row = pd.DataFrame(
        {
            "genre": ["All"],
            "track_count": [lyrical_diversity["track_count"].sum()],
            "avg_vocab_size": [
                (lyrical_diversity["avg_vocab_size"] * lyrical_diversity["track_count"]).sum()
                / lyrical_diversity["track_count"].sum(),
            ],
            "avg_total_words": [
                (lyrical_diversity["avg_total_words"] * lyrical_diversity["track_count"]).sum()
                / lyrical_diversity["track_count"].sum(),
            ],
            "avg_type_token_ratio": [
                (lyrical_diversity["avg_type_token_ratio"] * lyrical_diversity["track_count"]).sum()
                / lyrical_diversity["track_count"].sum(),
            ],
        },
    )
    df_div = pd.concat(
        [lyrical_diversity.sort_values(metric, ascending=True), all_row],
        ignore_index=True,
    )

    fig_div = px.bar(
        df_div,
        y="genre",
        x=metric,
        orientation="h",
        category_orders={"genre": df_div["genre"].tolist()},
    )
    fig_div.update_layout(
        margin={"r": 20, "t": 20, "b": 50},
        paper_bgcolor="#fafafa",
        plot_bgcolor="#fafafa",
    )
    st.plotly_chart(fig_div, width="stretch", theme=None)


st.header("Genre Map")


@st.cache_data
def add_countries(df):
    """Reverse geocode lat/lon to ISO3 country codes + names."""
    coords = list(zip(df["lat"], df["lon"]))
    results = rg.search(coords)
    df = df.copy()
    df["country"] = [
        _iso_lookup.loc[r["cc"], "iso3"] if r["cc"] in _iso_lookup.index else r["cc"]
        for r in results
    ]
    df["country_name"] = [
        _iso_lookup.loc[r["cc"], "name"] if r["cc"] in _iso_lookup.index else r["cc"]
        for r in results
    ]
    return df


genre_loc = add_countries(genre_location)

map_genre = st.selectbox("Genre", ["All", *genre_list], key="map_genre")

if map_genre == "All":
    country_genre = genre_loc.groupby(["country", "genre"])["artist_count"].sum().reset_index()
    idx = country_genre.groupby("country")["artist_count"].idxmax()
    dominant = country_genre.loc[idx].copy()
    totals = genre_loc.groupby("country")["artist_count"].sum().reset_index(name="total")
    dominant = dominant.merge(totals, on="country")
    dominant["norm_total"] = dominant["total"] / dominant["total"].max()
    names = genre_loc[["country", "country_name"]].drop_duplicates()
    dominant = dominant.merge(names, on="country")

    fig_map = px.choropleth(
        dominant,
        locations="country",
        color="genre",
        hover_name="country_name",
        hover_data=["artist_count", "total"],
        projection="natural earth",
    )
    for trace in fig_map.data:
        countries = trace.locations
        if countries is not None:
            opacities = []
            for c in countries:
                row = dominant[dominant["country"] == c]
                opacities.append(
                    float(row["norm_total"].iloc[0]) * 0.7 + 0.3 if len(row) else 0.3,
                )
            trace.marker.opacity = opacities
else:
    genre_data = genre_loc[genre_loc["genre"] == map_genre]
    genre_counts = (
        genre_data.groupby("country")["artist_count"].sum().reset_index(name="genre_artists")
    )
    total_counts = (
        genre_loc.groupby("country")["artist_count"].sum().reset_index(name="total_artists")
    )
    country_counts = genre_counts.merge(total_counts, on="country")
    country_counts["pct"] = country_counts["genre_artists"] / country_counts["total_artists"] * 100
    names = genre_loc[["country", "country_name"]].drop_duplicates()
    country_counts = country_counts.merge(names, on="country")

    fig_map = px.choropleth(
        country_counts,
        locations="country",
        color="pct",
        hover_name="country_name",
        hover_data=["genre_artists", "total_artists", "pct"],
        projection="natural earth",
        labels={
            "pct": "% Artists",
            "genre_artists": "Genre artists",
            "total_artists": "Total artists",
        },
    )
    fig_map.update_coloraxes(reversescale=True)

fig_map.update_layout(
    margin={"t": 0, "b": 0, "l": 10, "r": 150},
    paper_bgcolor="#fafafa",
    height=600,
    legend={
        "font": {"size": 14},
        "y": 0.5,
        "yanchor": "middle",
        "xanchor": "left",
    },
)
fig_map.update_geos(
    bgcolor="#fafafa",
)
st.plotly_chart(fig_map, theme=None)
