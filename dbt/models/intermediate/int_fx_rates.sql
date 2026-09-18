-- FX reference. Reporting currency is EUR: 2,853 of 2,855 invoices are already EUR.
--
-- Static rates, deliberately -- the brief accepts a hardcoded rate with a stated
-- assumption. Every invoice converts at the same rate regardless of its date, which
-- materially affects one paid invoice (EUR 26 of EUR 322,890). Dated rates are a
-- production concern; see the README.
--
-- This model is also a guard: int_paid_invoices_eur joins it with an INNER join, so a
-- currency with no rate here cannot reach revenue at face value.

select 'EUR' as currency, cast(1.000 as numeric(10,4)) as rate_to_eur
union all
select 'SEK', cast(0.087 as numeric(10,4))   -- ~2025 average
