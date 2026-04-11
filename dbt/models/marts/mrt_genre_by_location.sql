-- Genre distribution by geographic location (for map visualization)
-- Groups by rounded lat/lon to cluster nearby artists
with located_tracks as (
    select
        t.track_id,
        t.artist_latitude,
        t.artist_longitude,
        t.artist_location,
        g.genre
    from {{ ref('stg_tracks') }} t
    inner join {{ ref('stg_genres') }} g using (track_id)
    where t.artist_latitude is not null
      and t.artist_longitude is not null
)

select
    round(artist_latitude, 1) as lat,
    round(artist_longitude, 1) as lon,
    genre,
    count(*) as track_count
from located_tracks
group by lat, lon, genre
