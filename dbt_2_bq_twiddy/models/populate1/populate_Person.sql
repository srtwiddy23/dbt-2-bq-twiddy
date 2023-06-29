with source as (
    select * from {{ source('airbyte_bq_test_srandle2', 'Person') }}
),

renamed as (
    select 
        1 personTypeID,
        G.lGuest_id personID,
         title,
         firstName,
         lastName,
         emailAddress,
         phoneNumber1,
         phoneNumber2,
         phoneNumber3,
        CASE WHEN O.EmailAddress IS NULL THEN 0 ELSE 1 END hasOwnerRecord
        from tblGuest G
		  			LEFT OUTER JOIN
		  			(select DISTINCT
		  					RTRIM(LTRIM(sEmail_ad)) EmailAddress
		  				from tblOwner
		  				where boolDeleted_fl = 0
		  					AND LEN(RTRIM(LTRIM(sEmail_ad))) > 0
		  			) O ON O.EmailAddress = ifnull(G.sEmail_ad,'')
		  		where G.boolDeleted_fl = 0
        UNION ALL
			select        
                2 personTypeID,
                lOwner_id PersonID,
                title,
                firstName,
                lastName,
                emailAddress,
                phoneNumber1,
                phoneNumber2,
                phoneNumber3,
                1 hasOwnerRecord         
        from tblOwner           
            where boolDeleted_fl = 0

    
    from source

)

select * from renamed