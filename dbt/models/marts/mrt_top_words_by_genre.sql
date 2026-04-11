-- Top words per genre, ranked by total count, stop words filtered
with stop_words as (
    select word from unnest([
        'i', 'the', 'you', 'to', 'and', 'a', 'it', 'me', 'not', 'in',
        'my', 'is', 'of', 'that', 'do', 'your', 'on', 'am', 'we', 'are',
        'all', 'will', 'for', 'be', 'no', 'have', 'so', 'but', 'was',
        'can', 'if', 'this', 'with', 'they', 'he', 'she', 'her', 'his',
        'what', 'when', 'them', 'at', 'an', 'just', 'or', 'there', 'from',
        'been', 'had', 'has', 'did', 'were', 'would', 'could', 'should',
        'their', 'our', 'about', 'who', 'how', 'which', 'than', 'its',
        'these', 'those', 'then', 'only', 'also', 'more', 'some', 'as'
    ]) as word
)

select
    g.genre,
    l.word,
    sum(l.count) as total_count,
    row_number() over (partition by g.genre order by sum(l.count) desc) as word_rank
from {{ ref('stg_lyrics') }} l
inner join {{ ref('stg_genres') }} g using (track_id)
where l.word not in (select word from stop_words)
group by g.genre, l.word
