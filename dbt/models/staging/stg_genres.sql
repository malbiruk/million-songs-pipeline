select
    track_id,
    genre,
    minority_genre
from {{ source('million_songs', 'genres') }}
