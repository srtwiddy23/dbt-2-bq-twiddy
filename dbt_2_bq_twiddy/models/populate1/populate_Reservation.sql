with source as (
    select * from {{ source('airbyte_bq_test_srandle2', 'Reservation') }}
),

renamed as (
    select 
        TUR.lUnitReservations_id reservationID,  
        TUR.lResvTypesList_id reservationTypeID,  
        TUR.lPerson_id personID,  
        TUR.lUnit_id unitID, 
        RTRIM(TU.sUnit_no) unitNumber, 
        RTRIM(TUR.sConfirmation_no) confirmationNumber, 
        CONVERT(VARCHAR(24),TUR.dtResv_dt,126) dateReserved,  
        CONVERT(VARCHAR(24),TUR.dtArrive_dt,126) arrivalDate,  
        CONVERT(VARCHAR(24),TUR.dtDepart_dt,126) departureDate,  
        TUR.iNights_no nights, 
        SUM(TC.cChg_amt) rentalFee, 
        CASE WHEN ifnull(THI.boolHeadingInfo_fl,0) = 0 THEN 0 ELSE 1 END signed, 
        CASE 
            WHEN NOT ifnull(THIA.boolHeadingInfo_fl, 0) = 0 THEN 1 
            WHEN NOT ifnull(THIAP.boolHeadingInfo_fl,0) = 0 AND ifnull(THI.boolHeadingInfo_fl,0) = 0 THEN 1 
            ELSE 0 
        END fromAPH 
        from tblUnitReservations TUR 
        INNER JOIN tblUnit TU ON TU.lUnit_id = TUR.lUnit_id 
        LEFT OUTER JOIN tblGuestChgs TC ON 
            TC.lUnitReservations_id = TUR.lUnitReservations_id 
            AND TC.boolPymt_fl = 0 
            AND TC.lHeadingsList_id = 22 
            AND TC.boolDeleted_fl = 0 
        LEFT OUTER JOIN tblUserFieldHeadingsInfo THI --Lease Signed UF 
            ON THI.lPerson_id = TUR.lUnitReservations_id 
            AND THI.lUserFieldHeadingsList_id = 5 
            AND THI.boolDeleted_fl = 0 
        LEFT OUTER JOIN tblUserFieldHeadingsInfo THIA --APH UF 
            ON THIA.lPerson_id = TUR.lUnitReservations_id 
            AND THIA.lUserFieldHeadingsList_id = 195 
            AND THIA.boolDeleted_fl = 0 
        LEFT OUTER JOIN tblUserFieldHeadingsInfo THIAP --APH Signed UF 
            ON THIAP.lPerson_id = TUR.lUnitReservations_id 
            AND THIAP.lUserFieldHeadingsList_id = 60 
            AND THIAP.boolDeleted_fl = 0 
        where 
            TUR.boolDeleted_fl = 0  
            AND TUR.sConfirmation_no IS NOT NULL  
        group by 
            TUR.lUnitReservations_id, 
            TUR.lResvTypesList_id, 
            TUR.lPerson_id, 
            TUR.lUnit_id, 
            RTRIM(TU.sUnit_no), 
            TUR.sConfirmation_no, 
            CONVERT(VARCHAR(24),TUR.dtResv_dt,126), 
            CONVERT(VARCHAR(24),TUR.dtArrive_dt,126), 
            CONVERT(VARCHAR(24),TUR.dtDepart_dt, 126), 
            TUR.iNights_no, 
            CASE WHEN ifnull(THI.boolHeadingInfo_fl,0) = 0 THEN 0 ELSE 1 END, 
            CASE 
                WHEN NOT ifnull(THIA.boolHeadingInfo_fl, 0) = 0 THEN 1 
                WHEN NOT ifnull(THIAP.boolHeadingInfo_fl,0) = 0 AND ifnull(THI.boolHeadingInfo_fl,0) = 0 THEN 1 
                ELSE 0 

    from source

)

select * from renamed