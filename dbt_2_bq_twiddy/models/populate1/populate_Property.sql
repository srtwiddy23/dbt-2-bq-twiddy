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
        thumbnailimage,
        area,
        checkinday,
        checkinoffice

    from source

)

select * from renamed