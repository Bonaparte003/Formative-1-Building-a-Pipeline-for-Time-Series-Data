-- LCA Time-Series – Example queries (≥3 required for Task 2)
-- Run after schema.sql and loading sample data.

-- ---------------------------------------------------------------------------
-- Query 1: Latest record (most recent case by created_at)
-- ---------------------------------------------------------------------------
SELECT c.case_id, c.created_at, c.wage, c.case_status, c.job_title,
       p.fiscal_year, p.quarter,
       e.employer_name, e.employer_location
FROM cases c
JOIN periods p ON c.period_id = p.period_id
JOIN employers e ON c.employer_id = e.employer_id
ORDER BY c.created_at DESC
LIMIT 1;

-- ---------------------------------------------------------------------------
-- Query 2: Records by date range (e.g. FY 2021 Q1 – 2022 Q4)
-- ---------------------------------------------------------------------------
SELECT c.case_id, p.fiscal_year, p.quarter, c.wage, c.case_status,
       e.employer_name
FROM cases c
JOIN periods p ON c.period_id = p.period_id
JOIN employers e ON c.employer_id = e.employer_id
WHERE p.start_date >= '2021-01-01'
  AND p.end_date   <= '2022-12-31'
ORDER BY p.start_date, c.case_id
LIMIT 100;

-- ---------------------------------------------------------------------------
-- Query 3: Aggregate by period (e.g. count and average wage per quarter)
-- ---------------------------------------------------------------------------
SELECT p.fiscal_year, p.quarter,
       COUNT(*) AS case_count,
       AVG(c.wage) AS avg_wage
FROM cases c
JOIN periods p ON c.period_id = p.period_id
WHERE c.unit_of_pay = 'Year'
GROUP BY p.period_id, p.fiscal_year, p.quarter
ORDER BY p.fiscal_year, p.quarter;

-- ---------------------------------------------------------------------------
-- Optional: Get a single case by ID
-- ---------------------------------------------------------------------------
-- SELECT * FROM cases WHERE case_id = 1;
