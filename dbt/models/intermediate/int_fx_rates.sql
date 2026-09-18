-- FX reference. Reporting currency is EUR, with static rates: every invoice converts
-- at the same rate whatever its date. Dated rates are a production concern.

select 'EUR' as currency, cast(1.000 as numeric(10,4)) as rate_to_eur
union all
select 'SEK', cast(0.087 as numeric(10,4))
