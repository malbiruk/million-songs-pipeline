-- Average audio features per year with stddev for CI and min-max normalized values
with yearly as (
    select
        year,
        count(*) as track_count,
        avg(tempo) as avg_tempo,
        stddev(tempo) as std_tempo,
        avg(loudness) as avg_loudness,
        stddev(loudness) as std_loudness,
        avg(duration) as avg_duration,
        stddev(duration) as std_duration,
        avg(song_hotttnesss) as avg_hotttnesss,
        stddev(song_hotttnesss) as std_hotttnesss
    from {{ ref('stg_tracks') }}
    where year is not null
    group by year
),

bounds as (
    select
        min(avg_tempo) as min_tempo, max(avg_tempo) as max_tempo,
        min(avg_loudness) as min_loud, max(avg_loudness) as max_loud,
        min(avg_duration) as min_dur, max(avg_duration) as max_dur,
        min(avg_hotttnesss) as min_hot, max(avg_hotttnesss) as max_hot
    from yearly
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
from yearly y
cross join bounds b
order by y.year
