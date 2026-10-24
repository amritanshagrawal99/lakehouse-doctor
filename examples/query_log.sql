SELECT * FROM lake.events WHERE user_id = 1842;
SELECT count(*) FROM lake.events e WHERE e.user_id IN (1, 2, 3) AND e.country = 'IN';
SELECT e.event_ts, e.amount FROM lake.events e WHERE e.event_ts BETWEEN '2026-01-01' AND '2026-01-31';
SELECT u.name, sum(e.amount) FROM lake.events e JOIN lake.users u ON u.id = e.user_id WHERE e.event_ts >= '2026-02-01' GROUP BY 1;
SELECT * FROM lake.events WHERE user_id = 77 AND event_ts > '2026-03-01';
SELECT * FROM lake.events WHERE session_id = 'abc';
SELECT country, count(*) FROM lake.events WHERE country = 'US' GROUP BY 1;
this is not sql;
