with source as (
    select * from {{ source('airbyte_bq_test_srandle2', 'PersonGuestXREF') }}
),

renamed as (
    select 
	    1 personTypeID, 
        GuestID personID, 
        LOWER(REPLACE(CAST(VisitorID AS CHAR(36)), '-','')) systemID, 
        DATEDIFF(SECOND, '1970-01-01',DateCreated) dateCreated 
        from VisitorGuestXREF 
        where 
            DateDeleted IS NULL 
        UNION ALL 
        select 
            2 personTypeID, 
            OwnerID personID, 
            LOWER(REPLACE(CAST(VisitorID AS CHAR(36)), '-','')) systemID, 
            DATEDIFF(SECOND, '1970-01-01',DateCreated) dateCreated 
        from VisitorOwnerXREF 
        where 
            DateDeleted IS NULL

    from source

)

select * from renamed