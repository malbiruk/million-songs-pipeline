-- Genre distribution by geographic location (for choropleth map)
-- Counts distinct artists, not tracks
with located_tracks as (
    select
        t.track_id,
        t.artist_name,
        t.artist_latitude,
        t.artist_longitude,
        g.genre
    from {{ ref('stg_tracks') }} t
    inner join {{ ref('stg_genres') }} g using (track_id)
    where t.artist_latitude is not null
      and t.artist_longitude is not null
)

select
    round(artist_latitude, 0) as lat,
    round(artist_longitude, 0) as lon,
    genre,
    count(distinct artist_name) as artist_count
from located_tracks
group by lat, lon, genre
