with source as (
    select * from {{ source('airbyte_bq_test_srandle2', 'PropertyUrl') }}
),

renamed as (
    select 
        UnitID unitID,
        UnitNumber unitNumber,
        Url url
    from source

)

select * from renamed