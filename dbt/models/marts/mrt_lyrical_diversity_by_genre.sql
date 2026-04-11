-- Lyrical diversity metrics per genre
with track_stats as (
    select
        l.track_id,
        g.genre,
        count(distinct l.word) as vocab_size,
        sum(l.count) as total_words
    from {{ ref('stg_lyrics') }} l
    inner join {{ ref('stg_genres') }} g using (track_id)
    group by l.track_id, g.genre
)

select
    genre,
    count(*) as track_count,
    avg(vocab_size) as avg_vocab_size,
    avg(total_words) as avg_total_words,
    avg(vocab_size / nullif(total_words, 0)) as avg_type_token_ratio
from track_stats
group by genre
