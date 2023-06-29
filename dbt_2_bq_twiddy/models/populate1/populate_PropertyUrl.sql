with source as (
    select * from {{ source('airbyte_bq_test_srandle2', 'PropertyUrl') }}
),

renamed as (
    select 
        unitID,
        unitNumber,
        url
    from source

)

select * from renamed