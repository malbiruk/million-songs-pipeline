select
    track_id,
    title,
    artist_name,
    release,
    artist_location,
    case when year = 0 then null else year end as year,
    duration,
    tempo,
    loudness,
    key,
    mode,
    time_signature,
    case when song_hotttnesss is null or is_nan(song_hotttnesss) then null else song_hotttnesss end as song_hotttnesss,
    case when artist_hotttnesss is null or is_nan(artist_hotttnesss) then null else artist_hotttnesss end as artist_hotttnesss,
    case when artist_latitude is null or is_nan(artist_latitude) then null else artist_latitude end as artist_latitude,
    case when artist_longitude is null or is_nan(artist_longitude) then null else artist_longitude end as artist_longitude
from {{ source('million_songs', 'tracks') }}
