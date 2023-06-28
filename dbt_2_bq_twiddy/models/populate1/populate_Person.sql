with source as (
    select * from {{ source('airbyte_bq_test_srandle2', 'Person') }}
),

renamed as (
    select 
        1 personTypeID,
        G.lGuest_id personID,
        ifnull(G.sTitle_nm,'') title,
        ifnull(G.sFirst_nm,'') firstName,
        ifnull(G.sLast_nm,'') lastName,
        ifnull(G.sEmail_ad,'') emailAddress,
        ifnull(G.sHPhone_no,'') phoneNumber1,
        ifnull(G.sWPhone_no,'') phoneNumber2,
        ifnull(G.sOPhone_no,'') phoneNumber3,
        CASE WHEN O.EmailAddress IS NULL THEN 0 ELSE 1 END hasOwnerRecord
        from tblGuest G
		  			LEFT OUTER JOIN
		  			(select DISTINCT
		  					RTRIM(LTRIM(sEmail_ad)) EmailAddress
		  				from tblOwner
		  				where
                            boolDeleted_fl = 0
		  					AND LEN(RTRIM(LTRIM(sEmail_ad))) > 0
		  			) O ON O.EmailAddress = ifnull(G.sEmail_ad,'')
		  		where 
                    G.boolDeleted_fl = 0
        UNION ALL
			select        
                2 personTypeID,
                lOwner_id PersonID,
                ifnull(sTitle_nm,'') title,
                ifnull(sFirst_nm,'') firstName,
                ifnull(sLast_nm,'') lastName,
                ifnull(sEmail_ad,'') emailAddress,
                ifnull(sHPhone_no,'') phoneNumber1,
                ifnull(sWPhone_no,'') phoneNumber2,
                ifnull(sOPhone_no,'') phoneNumber3,
        1 hasOwnerRecord         
        from tblOwner           
            where 
                boolDeleted_fl = 0

    
    from source

)

select * from renamed