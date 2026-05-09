CREATE TABLE flights (
  id bigserial PRIMARY KEY,
  fl_date date,
  carrier text,
  flight_num int,
  origin text,
  dest text,
  scheduled_dep_time Time,
  scheduled_arr_time Time,
  scheduled_elapsed_minutes int,
  actual_elapsed_minutes int,
  distance int,
  dep_delay int,
  arr_delay int,
  cancelled boolean,
  price numeric,
  flight_name text
);


SELECT *
FROM flights
LIMIT 10


ANALYZE flights;
SELECT COUNT(*) FROM flights;

--Feature 1: Basic route + date search (normal seq vs index comparison)
DROP INDEX IF EXISTS ix_cancelled_route_date;
--1(A): Before making an index
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, fl_date, origin, dest, scheduled_dep_time, price, scheduled_elapsed_minutes
FROM flights
WHERE origin = 'JFK' AND dest = 'LAX' AND fl_date = '2025-01-10' AND cancelled = false
ORDER BY price
LIMIT 50;

--1(B): Same feature with an index
CREATE INDEX ix_route_date_price
ON flights (origin, dest, fl_date, price);

--1(C): Run query with new index again
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, fl_date, origin, dest, scheduled_dep_time, price, scheduled_elapsed_minutes
FROM flights
WHERE origin = 'JFK' AND dest = 'LAX' AND fl_date = '2025-01-10' AND cancelled = false
ORDER BY price
LIMIT 50;

DROP INDEX IF EXISTS ix_route_date_price;

--Feature 2 - Sort by fastest duration (same but showing composite ordering behavior)
-- 2(A): Run before index:
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, fl_date, origin, dest, scheduled_elapsed_minutes, price
FROM flights
WHERE origin = 'BOS' AND dest = 'ORD' AND fl_date = '2025-02-14' AND cancelled = false
ORDER BY scheduled_elapsed_minutes
LIMIT 20;

--2(B): Create index
CREATE INDEX ix_route_date_duration
ON flights (origin, dest, fl_date, scheduled_elapsed_minutes);

--2(C): Run feature query again after index
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, fl_date, origin, dest, scheduled_elapsed_minutes, price
FROM flights
WHERE origin = 'BOS' AND dest = 'ORD' AND fl_date = '2025-02-14' AND cancelled = false
ORDER BY scheduled_elapsed_minutes
LIMIT 20;

--2(D) ordering of composite indexes
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, fl_date, origin, dest, scheduled_elapsed_minutes, price
FROM flights
WHERE fl_date = '2025-02-14' AND cancelled = false
ORDER BY scheduled_elapsed_minutes
LIMIT 20;

DROP INDEX IF EXISTS ix_route_date_duration;


--Feature 3 - Cheapest flight per route/day:
--3(A) Run query without index (partial index behavior)
EXPLAIN (ANALYZE, BUFFERS)
SELECT DISTINCT ON (fl_date)
    id, fl_date, price, origin, dest
FROM flights
WHERE origin = 'SFO' AND dest = 'MIA'
  AND fl_date BETWEEN '2025-01-01' AND '2025-01-31'
  AND cancelled = false
ORDER BY fl_date, price;

--3(B) Create Index
CREATE INDEX ix_route_daterange_price
ON flights (origin, dest, fl_date, price)
WHERE cancelled = false;

--3(C) After index
EXPLAIN (ANALYZE, BUFFERS)
SELECT DISTINCT ON (fl_date)
    id, fl_date, price, origin, dest
FROM flights
WHERE origin = 'SFO' AND dest = 'MIA'
  AND fl_date BETWEEN '2025-01-01' AND '2025-01-31'
  AND cancelled = false
ORDER BY fl_date, price;

DROP INDEX IF EXISTS ix_route_daterange_price;

--FEATURE 4: Index-only scan / covering index (index scan w and w/o incl)
--4(A1): Create index cover plain w/o include
CREATE INDEX ix_cover_plain
ON flights (origin, dest, fl_date, price);

--4(A2): Run with plain cover
EXPLAIN (ANALYZE, BUFFERS)
SELECT fl_date, MIN(price) AS lowest_price, MIN(scheduled_elapsed_minutes) AS fastest_minutes
FROM flights
WHERE origin = 'JFK' AND dest = 'LAX'
  AND fl_date BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY fl_date
ORDER BY fl_date;

DROP INDEX IF EXISTS ix_cover_plain;

-- 4(B1): Now with proper covering index
CREATE INDEX ix_cover
ON flights (origin, dest, fl_date, price)
INCLUDE (scheduled_elapsed_minutes);

VACUUM ANALYZE flights;

--4(B2): Run with ixcover
EXPLAIN (ANALYZE, BUFFERS)
SELECT fl_date, MIN(price) AS lowest_price, MIN(scheduled_elapsed_minutes) AS fastest_minutes
FROM flights
WHERE origin = 'JFK' AND dest = 'LAX'
  AND fl_date BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY fl_date
ORDER BY fl_date;

DROP INDEX IF EXISTS ix_cover;


--Feature 5 - Departure time filter for flights
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM flights
WHERE origin = 'ATL'
  AND dest = 'DEN'
  AND fl_date = '2025-01-22'
  AND scheduled_dep_time BETWEEN '08:00:00' AND '12:00:00'
ORDER BY scheduled_dep_time;

-- With an index
CREATE INDEX ix_dep_time
ON flights (origin, dest, fl_date, scheduled_dep_time);

EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM flights
WHERE origin = 'ATL'
  AND dest = 'DEN'
  AND fl_date = '2025-01-22'
  AND scheduled_dep_time BETWEEN '08:00:00' AND '12:00:00'
ORDER BY scheduled_dep_time;

DROP INDEX IF EXISTS ix_dep_time;


---- FEATURE 6: Cancellation rate by airline on a route (which airline cancels the least)
-- 6(A): Before index
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    carrier,
    COUNT(*) AS total_flights,
    COUNT(*) FILTER (WHERE cancelled = true) AS total_cancelled,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE cancelled = true) / COUNT(*), 2
    ) AS cancellation_rate
FROM flights
WHERE origin = 'SEA' AND dest = 'DFW'
GROUP BY carrier
ORDER BY cancellation_rate;

-- 6(B): Create index
CREATE INDEX ix_can_rate
ON flights (origin, dest, carrier, cancelled);

-- 6(C): After index
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    carrier,
    COUNT(*) AS total_flights,
    COUNT(*) FILTER (WHERE cancelled = true) AS total_cancelled,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE cancelled = true) / COUNT(*), 2
    ) AS cancellation_rate
FROM flights
WHERE origin = 'SEA' AND dest = 'DFW'
GROUP BY carrier
ORDER BY cancellation_rate;

DROP INDEX IF EXISTS ix_can_rate;

--for experiments:
TRUNCATE TABLE flights;