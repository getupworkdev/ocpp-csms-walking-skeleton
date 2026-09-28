-- Enough reference data for the simulator and tests to run against a fresh
-- database. A real deployment would manage these through an admin API.
insert into tariffs (name, currency, price_per_kwh_minor, session_fee_minor, active)
values ('Standard AC', 'EUR', 35, 50, true);

insert into id_tags (id_tag, status) values
    ('DEMO-TAG-1', 'Accepted'),
    ('DEMO-TAG-2', 'Accepted'),
    ('BLOCKED-TAG', 'Blocked');
