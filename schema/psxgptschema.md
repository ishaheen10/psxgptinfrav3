# psxGPT D1 Schema

Six canonical investment tables plus a small set of canonical logging/application tables. Everything else is legacy or infrastructure.

## Design Rules

1. Each table has one clear purpose. If two tables store the same data, one is redundant.
2. If a value changes with stock price, it belongs in `market_data`. If it changes with fundamentals reporting, it belongs in `financial_statements` or `ratios`.
3. The same concept uses the same column name everywhere. The company identifier is `ticker` in every canonical data table.
4. `period_end` and `filing_period` are related but not the same. `period_end` is the date a fact is about. `filing_period` is the filing the source page belongs to.
5. `statement_type + canonical_name` together define the semantic meaning of a fact row. The same `canonical_name` may appear in multiple statement types if the statement type makes the meaning clear.
6. Every row carries provenance. Derived rows use an anchor source and must carry a non-null `method`.
7. Every column earns its place by being queried by a retrieval contract, used by the pipeline for computation, or required for provenance. If none of these, it's dead weight.

---

## Canonical Investment Tables

1. `financial_statements`
2. `financial_documents`
3. `filings_fts`
4. `management_comp`
5. `market_data`
6. `ratios`

---

## `financial_statements`

One row = one line item, one company, one period, one statement type.

**Grain:** `ticker × period_end × section × statement_type × canonical_name × period_duration`

| Column | Type | Values / Range |
|--------|------|---------------|
| ticker | TEXT | PSX ticker, e.g. `ENGRO`, `LUCK`, `HBL` |
| company_name | TEXT | Company name |
| industry | TEXT | e.g. `Banking`, `Insurance`, `Cement`, `Power` |
| unit_type | TEXT | Unit convention used for `value`, e.g. `PKR thousands` |
| period_type | TEXT | `annual`, `quarterly` |
| period_end | TEXT | `YYYY-MM-DD`, e.g. `2024-06-30` |
| period_duration | TEXT | `PIT`, `3M`, `6M`, `9M`, `12M`, `LTM` |
| fiscal_year | INTEGER | fiscal year number |
| section | TEXT | `consolidated`, `unconsolidated` |
| statement_type | TEXT | `balance_sheet`, `profit_loss`, `cash_flow` |
| canonical_name | TEXT | See separate field list. e.g. `revenue_net`, `total_assets`, `net_profit` |
| original_name | TEXT | Source line-item label from filing |
| value | REAL | PKR thousands after normalization |
| method | TEXT | `direct`, `derived`, or a derivation label such as `6M-Q1`, `9M-6M`, `12M-9M`, `LTM_3M_sum` |
| source_file | TEXT | Source PDF filename or anchor filing filename |
| source_pages | TEXT | JSON array of page numbers, e.g. `[12,13]` |
| source_url | TEXT | URL to source PDF folder or anchor source asset |
| qc_flag | TEXT | Optional QC/review flag |
| created_at | TEXT | ISO timestamp |

**Rules:**
- Always filter by `statement_type`. The field name alone is never enough.
- Balance sheet items use `period_duration = 'PIT'`.
- Flow items use `3M`, `6M`, `9M`, `12M`, or `LTM`.
- Ratios mixing a flow item with a balance sheet item (ROE, ROA) must use `12M` or `LTM` for the flow component.
- `period_type` indicates whether the row comes from an annual or quarterly reporting cycle. Use `period_duration` for semantic filtering.
- `statement_type` must always be specified when querying `financial_statements`.
- `canonical_name` may overlap across statement types. For example, `advances` on `balance_sheet` is a point-in-time stock value, while `advances` on `cash_flow` is the change in advances during the period.

Full list of `canonical_name` values: see `canonical_fields.md`

---

## `financial_documents`

One row = one page from one filing. Full markdown text.

**Grain:** `ticker × filing_type × filing_period × filing_year × pg`

| Column | Type | Values / Range |
|--------|------|---------------|
| ticker | TEXT | PSX ticker |
| industry | TEXT | Normalized industry name |
| filing_type | TEXT | `annual`, `quarterly` |
| filing_period | TEXT | `YYYY-MM-DD`. Filing identity, not the same thing as a fact row's `period_end` |
| filing_year | INTEGER | filing year number |
| section_tags | TEXT | JSON object or JSON array of scored tags. Soft hints, not hard filters |
| pg | INTEGER | Page number within filing, starting at 1 |
| jpg_path | TEXT | Page image path |
| search_text | TEXT | BM25-oriented retrieval text generated from each page |
| text | TEXT | Full markdown text of the page |

**Rules:**
- This is the canonical narrative/evidence corpus. All narrative retrieval targets this table.
- `filings_fts` is a search index over this table, not a separate data store.
- `section_tags` are hints. They should improve search and ranking, not act as exclusive truth.

---

## `filings_fts`

One row = one searchable page entry corresponding to one `financial_documents` row.

**Grain:** `rowid -> financial_documents page`

| Column | Type | Values / Range |
|--------|------|---------------|
| rowid | INTEGER | Corresponds to the page row in `financial_documents` |
| search_text | TEXT | Indexed retrieval text |
| text | TEXT | Indexed page text |

**Rules:**
- This is an index, not a primary evidence table.
- It exists only to search `financial_documents`.
- Narrative retrieval should define the target page set against `financial_documents`, then use `filings_fts` as the search operator.

---

## `management_comp`

One row = one compensation aggregate for one company, one year, one scope, one role.

**Grain:** `ticker × period_end × reporting_scope × role`

| Column | Type | Values / Range |
|--------|------|---------------|
| ticker | TEXT | PSX ticker |
| company_name | TEXT | Company name |
| industry | TEXT | Industry |
| period_end | TEXT | `YYYY-12-31` for the compensation year |
| reporting_scope | TEXT | `consolidated`, `company` |
| role | TEXT | `ceo`, `chairman`, `exec_directors`, `non_exec_directors`, `executives`, `other` |
| persons | INTEGER | Number of people in the role bucket |
| base_salary | REAL | PKR |
| bonus | REAL | PKR |
| housing | REAL | PKR |
| retirement | REAL | PKR |
| other_benefits | REAL | PKR |
| total | REAL | PKR |
| source_url | TEXT | Source PDF reference |

---

## `market_data`

One row = one trading day for one company. Append-only. Historical rows are never overwritten away.

**Grain:** `ticker × date`

| Column | Type | Values / Range |
|--------|------|---------------|
| ticker | TEXT | PSX ticker |
| date | TEXT | `YYYY-MM-DD`. Trading date |
| closing_price | REAL | PKR |
| total_shares | INTEGER | Total shares outstanding |
| market_cap | REAL | PKR |
| week_52_high | REAL | PKR |
| week_52_low | REAL | PKR |
| pe_ratio | REAL | Market cap / LTM net profit. Can be NULL or negative |
| pb_ratio | REAL | Price / book value per share |
| dividend_yield | REAL | Percentage based on LTM dividends paid |
| enterprise_value | REAL | PKR |
| fundamentals_period_end | TEXT | `YYYY-MM-DD`. Reporting period used for the derived market ratios on that row |

**Rules:**
- Price-linked derived metrics (PE, PB, dividend yield, EV) belong here because they change with price.
- PE uses LTM net profit and market cap, not EPS.
- PB uses latest `PIT` equity.
- Dividend yield uses LTM dividends paid and market cap, not per-share dividends.
- `fundamentals_period_end` records the accounting period used for the row's derived ratios. It is not a filing date or publication date.

---

## `ratios`

One row = one fundamentals-only ratio for one company for one reporting period. Nothing in this table depends on daily price.

**Grain:** `ticker × period_end × period_duration × section × ratio_name`

| Column | Type | Values / Range |
|--------|------|---------------|
| ticker | TEXT | PSX ticker |
| period_end | TEXT | `YYYY-MM-DD` |
| period_duration | TEXT | `PIT`, `3M`, `6M`, `9M`, `12M`, `LTM` depending on ratio |
| section | TEXT | `consolidated`, `unconsolidated` |
| ratio_name | TEXT | See below |
| value | REAL | Numeric ratio value stored as a raw ratio, not a percentage |
| computed_at | TEXT | ISO timestamp |

**Universal ratio names:**

`roe`, `roa`, `net_margin`, `gross_margin`, `current_ratio`, `debt_to_equity`, `asset_turnover`

**Rules:**
- `ratios` is rebuilt only when `financial_statements` is refreshed. It is not part of the daily market-price sync.
- ROE uses `12M` or `LTM` profit over `PIT` equity.
- ROA uses `12M` or `LTM` profit over `PIT` assets.
- Any ratio mixing a flow item with a balance-sheet item uses a `12M` or `LTM` flow input.
- Flow-over-flow ratios (margins) can use any matching period duration.
- Balance-sheet-only ratios use `PIT`.
- No price-dependent ratios belong here.

---

## Canonical Logging / Application Tables

These are not investment-data tables, but they are still part of the schema contract.

### `query_logs`

One row = one user query / answer event.

| Column | Type | Values / Range |
|--------|------|---------------|
| id | TEXT | Opaque event id |
| user_id | TEXT | Opaque user id |
| user_email | TEXT | Nullable email |
| session_id | TEXT | Opaque session id |
| message_id | TEXT | Opaque message id |
| query | TEXT | User query text |
| answer | TEXT | Assistant answer text |
| error | TEXT | Nullable error text |
| duration_ms | INTEGER | Milliseconds |
| iterations | INTEGER | Non-negative integer |
| status | TEXT | Application-defined status |
| input_tokens | INTEGER | Non-negative integer |
| output_tokens | INTEGER | Non-negative integer |
| scout_input_tokens | INTEGER | Non-negative integer |
| scout_output_tokens | INTEGER | Non-negative integer |
| analyst_input_tokens | INTEGER | Non-negative integer |
| analyst_output_tokens | INTEGER | Non-negative integer |
| was_steered | INTEGER | `0` or `1` |
| created_at | TEXT | ISO timestamp |
| feedback | TEXT | Nullable feedback label |
| feedback_comment | TEXT | Nullable feedback text |
| feedback_at | TEXT | Nullable timestamp |

### `reasoning_logs`

One row = one stored scout/analyst reasoning trace.

| Column | Type | Values / Range |
|--------|------|---------------|
| id | INTEGER | Row id |
| scout_reasoning | TEXT | Scout reasoning text |
| scout_sql | TEXT | Scout SQL text |
| analyst_reasoning | TEXT | Analyst reasoning text |
| analyst_sql | TEXT | Analyst SQL text |
| user_email | TEXT | Nullable email |
| iterations | INTEGER | Non-negative integer |
| scout_duration_ms | INTEGER | Milliseconds |
| analyst_duration_ms | INTEGER | Milliseconds |
| session_id | TEXT | Opaque session id |
| message_id | TEXT | Opaque message id |
| created_at | TEXT | ISO timestamp |

### `contract_trace_logs`

One row = one retrieval or contract trace record.

| Column | Type | Values / Range |
|--------|------|---------------|
| id | TEXT | Opaque trace id |
| session_id | TEXT | Opaque session id |
| message_id | TEXT | Opaque message id |
| stage | TEXT | Application-defined stage name |
| atom_index | INTEGER | Non-negative integer |
| table_name | TEXT | Table touched by the trace |
| question | TEXT | Trace question text |
| tickers_json | TEXT | JSON payload |
| period_natural | TEXT | Nullable natural-language period |
| completeness | TEXT | Nullable completeness label |
| contract_complete | INTEGER | `0` or `1` |
| contract_json | TEXT | JSON payload |
| gaps_json | TEXT | JSON payload |
| source_ids_json | TEXT | JSON payload |
| attachment_count | INTEGER | Non-negative integer |
| sql_queries_json | TEXT | JSON payload |
| search_log_json | TEXT | JSON payload |
| reasoning | TEXT | Nullable reasoning text |
| created_at | TEXT | ISO timestamp |
| contract_id | TEXT | Nullable contract id |
| contract_kind | TEXT | Nullable contract kind |
| requested_table | TEXT | Nullable table name |
| resolved_table | TEXT | Nullable table name |
| failure_stage | TEXT | Nullable failure stage |

### `rlm_events`

One row = one retrieval lifecycle event.

| Column | Type | Values / Range |
|--------|------|---------------|
| id | TEXT | Opaque event id |
| message_id | TEXT | Opaque message id |
| session_id | TEXT | Opaque session id |
| phase | TEXT | Application-defined phase |
| event_type | TEXT | Application-defined event type |
| seq | INTEGER | Non-negative integer |
| payload_json | TEXT | JSON payload |
| created_at | TEXT | ISO timestamp |

### `shared_conversations`

One row = one shared conversation snapshot.

| Column | Type | Values / Range |
|--------|------|---------------|
| id | TEXT | Opaque share id |
| user_id | TEXT | Opaque user id |
| user_email | TEXT | Nullable email |
| session_id | TEXT | Opaque session id |
| title | TEXT | Nullable title |
| message_count | INTEGER | Non-negative integer |
| conversation_snapshot | TEXT | Snapshot payload |
| created_at | TEXT | ISO timestamp |
| view_count | INTEGER | Non-negative integer |

### `user_events`

One row = one user or product analytics event.

| Column | Type | Values / Range |
|--------|------|---------------|
| id | TEXT | Opaque event id |
| user_id | TEXT | Opaque user id |
| user_email | TEXT | Nullable email |
| event_type | TEXT | Application-defined event type |
| event_json | TEXT | JSON payload |
| session_id | TEXT | Nullable session id |
| created_at | TEXT | ISO timestamp |

### `user_memories`

One row = one stored memory item for a user.

| Column | Type | Values / Range |
|--------|------|---------------|
| id | TEXT | Opaque memory id |
| user_id | TEXT | Opaque user id |
| content | TEXT | Memory text |
| first_name | TEXT | Nullable first name |
| source | TEXT | Source label |
| created_at | TEXT | ISO timestamp |

---

## Deprecated Tables

Do not query or write to:

- `market_valuation`
- `market_cap_history`
- `shares_outstanding`

---

## Separate Reference Files

- `canonical_fields.md` — full list of `canonical_name` values in `financial_statements`, grouped by `statement_type`
- `ratio_names.md` — full list of `ratio_name` values in `ratios` with computation definitions
