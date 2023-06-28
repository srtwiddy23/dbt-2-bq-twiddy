with source as (
    select * from {{ source('airbyte_bq_test_srandle2', 'PropertyAvailability') }}
),

renamed as (
    select 
        unitID UnitID,
        ifnull(WeeklyRate,0) weeklyRate,
        DATEDIFF(SECOND, '1970-01-01',RateArrive) rateArrive,
        DATEDIFF(SECOND, '1970-01-01',RateDepart) rateDepart,
        CASE WHEN ReservationArrive IS NULL THEN NULL ELSE DATEDIFF(SECOND, '1970-01-01', ReservationArrive) END reservationArrive,
        CASE WHEN ReservationDepart IS NULL THEN NULL ELSE DATEDIFF(SECOND, '1970-01-01', ReservationDepart) END reservationDepart,
        CASE WHEN DateReserved IS NULL THEN NULL ELSE DATEDIFF(SECOND, '1970-01-01', DateReserved) END dateReserved,
        ifnull(ReservationTypeID,0) reservationTypeID,
        IsAvailable isAvailable,
        ifnull(OwnerApprovedPrice,0) ownerApprovedPrice,
        ifnull(DollarDiscount,0) dollarDiscount,
        ifnull(PercentDiscount,0) percentDiscount
    
    from source

)

select * from renamed