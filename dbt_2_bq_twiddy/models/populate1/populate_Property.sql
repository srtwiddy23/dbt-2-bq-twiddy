with source as (
    select * from {{ source('airbyte_bq_test_srandle2', 'Property') }}
),

renamed as (
    select
        description,
        zipcode,
        address2,
        taxrateid,
        city,
        address1,
        subdivision,
        communitypool,
        name,
        archived,
        neighborhood,
        browselocation,
        location,
        webavailable,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_property_hashid

    from source

)

select * from renamed