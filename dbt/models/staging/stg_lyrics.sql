select
    track_id,
    word,
    count
from {{ source('million_songs', 'lyrics') }}
