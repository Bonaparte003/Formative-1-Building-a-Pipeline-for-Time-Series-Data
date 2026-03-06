# MongoDB Collection Design (Cache Layer)

MongoDB is used as a **cache** for faster retrieval of time-series data. The **source of truth** remains the SQL database; this collection mirrors (or is populated from) SQL for read-heavy endpoints.

---

## 1. Role of MongoDB

- **Cache** for: latest record, date-range queries.
- **Faster reads** by avoiding repeated SQL joins and by using indexes on time fields.
- **Optional strategy:** API reads from MongoDB first; on cache miss, read from SQL and optionally write to MongoDB (write-through or lazy population).

---

## 2. Collection Name

- `lca_records` or `time_series_cache`

---

## 3. Document Shape (One Document per Case)

Aligned with the SQL `cases` table plus period and employer info for rich responses without joins:

```json
{
  "_id": ObjectId("..."),
  "case_id": 100001,
  "period": {
    "fiscal_year": 2022,
    "quarter": "Q1",
    "start_date": "2022-01-01",
    "end_date": "2022-03-31"
  },
  "employer": {
    "employer_name": "Example Corp",
    "employer_location": "New York, New York",
    "employer_country": "United States Of America"
  },
  "soc_title": "Software Developers, Applications",
  "visa_class": "H-1B",
  "job_title": "Software Engineer",
  "full_time_position": "Y",
  "worksite": "New York, New York",
  "wage": 120000.0,
  "unit_of_pay": "Year",
  "case_status": "Certified",
  "created_at": { "$date": "2022-02-15T00:00:00.000Z" },
  "cached_at": { "$date": "2025-03-01T12:00:00.000Z" }
}
```

- **case_id:** Matches SQL `cases.case_id` for consistency.
- **period:** Embedded object for date-range and “latest” filters.
- **employer:** Embedded for display without joins.
- **cached_at:** When this document was written to MongoDB (useful for TTL or invalidation).

---

## 4. Indexes

| Index | Purpose |
|-------|---------|
| `{ "created_at": -1 }` | Latest record: `find().sort({ created_at: -1 }).limit(1)` |
| `{ "period.start_date": 1, "period.end_date": 1 }` | Date range: `find({ "period.start_date": { "$gte": start }, "period.end_date": { "$lte": end } })` |
| `{ "case_id": 1 }` (unique) | Deduplication and GET-by-id |

---

## 5. Sample Documents

See `database/mongodb/samples/sample_documents.json` for 2–3 full example documents to include in the report.

---

## 6. Required Queries (≥3)

1. **Latest record:** Sort by `created_at` descending, limit 1.
2. **Records by date range:** `find({ "period.start_date": { "$gte": "2021-01-01" }, "period.end_date": { "$lte": "2022-12-31" } })`.
3. **Get by case_id:** `findOne({ "case_id": 123456 })`.
4. (Optional) **Count by case_status** in a date range for analytics.

Document each query and its result in the report and in `database/mongodb/queries.js` (or equivalent).
