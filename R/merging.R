combine_esense_mirage <- function(esense, mirage) {
  conn <- duckdb::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(duckdb::dbDisconnect(conn))

  duckdb::duckdb_register(conn, "esense", esense)
  duckdb::duckdb_register(conn, "mirage", mirage)

  res <- DBI::dbGetQuery(conn, "
    WITH
    e AS (
      SELECT *, date_trunc('day', start_time) AS start_date
      FROM esense
    ),
    m AS (
      SELECT *, date_trunc('day', start) AS start_date
      FROM mirage
    ),
    cte AS (
      SELECT
        e.mirage_pid AS mirage_pid, 
        e.start_time AS start_esense,
        e.end_time AS end_esense, 
        m.start AS start_mirage,
        m.end AS end_mirage,
        e.start_date AS date,
        e.id AS row_esense,
        m.id AS row_mirage
      FROM e
      LEFT JOIN m ON (
        e.mirage_pid = m.mirage_pid AND
        e.start_date = m.start_date
      )
    ),
    cte2 AS (
      SELECT DISTINCT
        *, 
        @(datediff('minute', start_mirage, start_esense)) AS diff_start,
        @(datediff('minute', end_mirage, end_esense)) AS diff_end
      FROM cte
    ),
    cte3 AS (
      SELECT
        *,
        diff_start <= 2 AND diff_end <= 2 AS window_2,
        diff_start <= 5 AND diff_end <= 5 AS window_5,
        diff_start <= 10 AND diff_end <= 10 AS window_10,
      FROM cte2
    )
    SELECT * EXCLUDE (window_2, window_5, window_10)
    FROM cte3
    WHERE (
      window_2 OR window_5 OR window_10
    )
    ORDER BY mirage_pid, date;
  ")

  res
}
