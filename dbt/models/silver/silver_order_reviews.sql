with source as (
    select * from {{ ref('bronze_order_reviews') }}
),

cleaned as (
    select
        REVIEW_ID,
        ORDER_ID,
        try_to_number(REVIEW_SCORE)               as review_score,
        try_to_timestamp(REVIEW_CREATION_DATE, 'YYYY-MM-DD HH24:MI:SS')    as review_creation_date,
        try_to_timestamp(REVIEW_ANSWER_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') as review_answer_timestamp
    from source
    where ORDER_ID is not null
      and REVIEW_ID is not null
)

select * from cleaned