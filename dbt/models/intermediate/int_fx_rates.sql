-- FX reference: conversion to the single reporting currency.
--
-- Reporting currency is EUR: NordStack sells across Europe and 2,853 of 2,855 invoices
-- are already EUR.
--
-- STATIC RATES, DELIBERATELY. The brief asks for a stated FX assumption and accepts a
-- hardcoded rate with a comment. Every invoice converts at the same rate regardless of
-- its date, so a 2024 invoice and a 2026 invoice use identical rates. A production
-- implementation would hold dated rates and convert each invoice at the rate applicable
-- to its accounting date -- see Next Steps.
--
-- Materially this affects one row: I000101, the only PAID non-EUR invoice, worth
-- EUR 26.01 of EUR 325,555. See DISCOVERY.md section 5.2 for why SEK is treated as a
-- genuine currency rather than a mislabelled EUR amount.
--
-- This model is also the guard behind int_paid_invoices_eur's INNER join: a currency
-- with no rate here cannot silently reach revenue at face value -- it disappears, and
-- the reconciliation test in tests/ fails loudly.

select 'EUR' as currency, cast(1.000 as numeric(10,4)) as rate_to_eur
union all
select 'SEK', cast(0.087 as numeric(10,4))   -- ~2025 average, assessment only
