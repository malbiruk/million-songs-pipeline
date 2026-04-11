-- Audio features per genre, normalized to 0-1 for radar chart
with raw_stats as (
    select
        g.genre,
        count(*) as track_count,
        avg(t.tempo) as avg_tempo,
        avg(t.loudness) as avg_loudness,
        avg(t.duration) as avg_duration,
        avg(t.song_hotttnesss) as avg_hotttnesss
    from {{ ref('stg_tracks') }} t
    inner join {{ ref('stg_genres') }} g using (track_id)
    group by g.genre
),

bounds as (
    select
        min(avg_tempo) as min_tempo, max(avg_tempo) as max_tempo,
        min(avg_loudness) as min_loud, max(avg_loudness) as max_loud,
        min(avg_duration) as min_dur, max(avg_duration) as max_dur,
        min(avg_hotttnesss) as min_hot, max(avg_hotttnesss) as max_hot
    from raw_stats
)

select
    r.genre,
    r.track_count,
    r.avg_tempo,
    r.avg_loudness,
    r.avg_duration,
    r.avg_hotttnesss,
    -- normalized 0-1 columns for radar chart
    (r.avg_tempo - b.min_tempo) / nullif(b.max_tempo - b.min_tempo, 0) as norm_tempo,
    (r.avg_loudness - b.min_loud) / nullif(b.max_loud - b.min_loud, 0) as norm_loudness,
    (r.avg_duration - b.min_dur) / nullif(b.max_dur - b.min_dur, 0) as norm_duration,
    (r.avg_hotttnesss - b.min_hot) / nullif(b.max_hot - b.min_hot, 0) as norm_hotttnesss
from raw_stats r
cross join bounds b
