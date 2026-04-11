-- Average audio features per year per genre with stddev for CI
-- Also includes mode (% major), key distribution, time_signature
with yearly_genre as (
    select
        t.year,
        g.genre,
        count(*) as track_count,
        avg(t.tempo) as avg_tempo,
        stddev(t.tempo) as std_tempo,
        avg(t.loudness) as avg_loudness,
        stddev(t.loudness) as std_loudness,
        avg(t.duration) as avg_duration,
        stddev(t.duration) as std_duration,
        avg(t.song_hotttnesss) as avg_hotttnesss,
        stddev(t.song_hotttnesss) as std_hotttnesss,
        avg(t.mode) as pct_major,
        stddev(t.mode) as std_mode
    from {{ ref('stg_tracks') }} t
    inner join {{ ref('stg_genres') }} g using (track_id)
    where t.year is not null
    group by t.year, g.genre
),

bounds as (
    select
        min(avg_tempo) as min_tempo, max(avg_tempo) as max_tempo,
        min(avg_loudness) as min_loud, max(avg_loudness) as max_loud,
        min(avg_duration) as min_dur, max(avg_duration) as max_dur,
        min(avg_hotttnesss) as min_hot, max(avg_hotttnesss) as max_hot
    from yearly_genre
)

select
    y.*,
    (y.avg_tempo - b.min_tempo) / nullif(b.max_tempo - b.min_tempo, 0) as norm_tempo,
    y.std_tempo / nullif(b.max_tempo - b.min_tempo, 0) as norm_std_tempo,
    (y.avg_loudness - b.min_loud) / nullif(b.max_loud - b.min_loud, 0) as norm_loudness,
    y.std_loudness / nullif(b.max_loud - b.min_loud, 0) as norm_std_loudness,
    (y.avg_duration - b.min_dur) / nullif(b.max_dur - b.min_dur, 0) as norm_duration,
    y.std_duration / nullif(b.max_dur - b.min_dur, 0) as norm_std_duration,
    (y.avg_hotttnesss - b.min_hot) / nullif(b.max_hot - b.min_hot, 0) as norm_hotttnesss,
    y.std_hotttnesss / nullif(b.max_hot - b.min_hot, 0) as norm_std_hotttnesss
from yearly_genre y
cross join bounds b
order by y.year, y.genre
