// LCA Time-Series – MongoDB example queries (≥3 required for Task 2)
// Collection: lca_records (see docs/MONGODB_DESIGN.md)
// Ensure indexes: created_at (-1), period.start_date (1), period.end_date (1), case_id (1) unique

// ---------------------------------------------------------------------------
// Query 1: Latest record
// ---------------------------------------------------------------------------
db.lca_records.find().sort({ created_at: -1 }).limit(1);

// ---------------------------------------------------------------------------
// Query 2: Records by date range (e.g. 2021-01-01 to 2022-12-31)
// ---------------------------------------------------------------------------
db.lca_records.find({
  "period.start_date": { $gte: "2021-01-01" },
  "period.end_date":   { $lte: "2022-12-31" }
}).sort({ "period.start_date": 1 }).limit(100);

// ---------------------------------------------------------------------------
// Query 3: Get one document by case_id
// ---------------------------------------------------------------------------
db.lca_records.findOne({ case_id: 100001 });

// ---------------------------------------------------------------------------
// Optional: Count by case_status in a date range
// ---------------------------------------------------------------------------
// db.lca_records.aggregate([
//   { $match: { "period.start_date": { $gte: "2021-01-01" }, "period.end_date": { $lte: "2022-12-31" } } },
//   { $group: { _id: "$case_status", count: { $sum: 1 } } }
// ]);
