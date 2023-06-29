with source as (
    select * from {{ source('airbyte_bq_test_srandle2', 'PropertyAvailability') }}
),

renamed as (
    select 
        unitID,
        weeklyRate,
        DATEDIFF(SECOND, '1970-01-01',RateArrive) rateArrive,
        DATEDIFF(SECOND, '1970-01-01',RateDepart) rateDepart,
        CASE WHEN ReservationArrive IS NULL THEN NULL ELSE DATEDIFF(SECOND, '1970-01-01', ReservationArrive) END reservationArrive,
        CASE WHEN ReservationDepart IS NULL THEN NULL ELSE DATEDIFF(SECOND, '1970-01-01', ReservationDepart) END reservationDepart,
        CASE WHEN DateReserved IS NULL THEN NULL ELSE DATEDIFF(SECOND, '1970-01-01', DateReserved) END dateReserved,
        reservationTypeID,
        isAvailable,
        ownerApprovedPrice,
        dollarDiscount,
        percentDiscount
    
    from source

)

select * from renamed