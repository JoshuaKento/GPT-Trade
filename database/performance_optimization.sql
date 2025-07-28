-- Database Performance Optimization for GPT Trader
-- Index strategies, query optimization, and performance monitoring
-- Target: Meet 30-minute SLA for daily processing of 50+ tickers

-- =============================================================================
-- PERFORMANCE ANALYSIS QUERIES
-- =============================================================================

-- Query to analyze current table sizes and growth
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables 
WHERE schemaname IN ('edgar', 'trading', 'audit')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Index usage analysis
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    CASE 
        WHEN idx_scan = 0 THEN 'UNUSED INDEX - Consider dropping'
        WHEN idx_scan < 100 THEN 'LOW USAGE - Review necessity'
        ELSE 'ACTIVELY USED'
    END as usage_status
FROM pg_stat_user_indexes 
WHERE schemaname IN ('edgar', 'trading', 'audit')
ORDER BY idx_scan DESC;

-- Slow query identification (requires pg_stat_statements extension)
-- Enable with: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    stddev_time,
    rows,
    100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
FROM pg_stat_statements 
WHERE query LIKE '%edgar%' OR query LIKE '%trading%' OR query LIKE '%audit%'
ORDER BY total_time DESC 
LIMIT 10;

-- =============================================================================
-- SPECIALIZED INDEXES FOR PERFORMANCE
-- =============================================================================

-- Composite index for daily processing queries
CREATE INDEX CONCURRENTLY idx_filings_daily_processing 
ON edgar.filings (filing_date, processing_status, processing_priority)
WHERE processing_status IN ('discovered', 'pending', 'processing');

-- Index for job queue optimization
CREATE INDEX CONCURRENTLY idx_jobs_queue_optimization
ON trading.jobs (status, priority, scheduled_at, next_retry_at)
WHERE status IN ('pending', 'failed') AND scheduled_at <= NOW();

-- Partial index for active error monitoring
CREATE INDEX CONCURRENTLY idx_error_logs_active_monitoring
ON audit.error_logs (created_at, severity, category)
WHERE NOT is_resolved AND severity IN ('ERROR', 'FATAL');

-- Index for SLA performance monitoring
CREATE INDEX CONCURRENTLY idx_filings_sla_monitoring
ON edgar.filings (filing_date, processing_completed_at, processing_duration_ms)
WHERE processing_status = 'completed' AND processing_completed_at IS NOT NULL;

-- Company-specific processing index
CREATE INDEX CONCURRENTLY idx_companies_processing_optimization
ON edgar.companies (processing_enabled, is_active, last_filing_check)
WHERE processing_enabled = true AND is_active = true;

-- Time-series index for metrics (with automatic partitioning consideration)
CREATE INDEX CONCURRENTLY idx_performance_metrics_time_series
ON audit.performance_metrics (timestamp DESC, metric_name, company_id)
WHERE timestamp >= NOW() - INTERVAL '90 days';

-- =============================================================================
-- PARTITIONING STRATEGY FOR LARGE TABLES
-- =============================================================================

-- Partition performance_metrics by month for better query performance
-- This is for future implementation when data volume grows

/*
-- Example partitioning setup for performance_metrics table
CREATE TABLE audit.performance_metrics_template (
    LIKE audit.performance_metrics INCLUDING ALL
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions (automated script would handle this)
CREATE TABLE audit.performance_metrics_y2024m01 
PARTITION OF audit.performance_metrics_template
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE audit.performance_metrics_y2024m02 
PARTITION OF audit.performance_metrics_template
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
-- Continue for each month...

-- Partition pruning will automatically exclude irrelevant partitions
*/

-- =============================================================================
-- QUERY OPTIMIZATION EXAMPLES
-- =============================================================================

-- Optimized query for daily filing processing
-- BEFORE: Simple join without proper indexing
-- AFTER: Leverages composite indexes and proper filtering

EXPLAIN (ANALYZE, BUFFERS) 
SELECT 
    c.ticker,
    c.company_name,
    f.id as filing_id,
    f.form_type,
    f.filing_date,
    f.processing_priority
FROM edgar.companies c
INNER JOIN edgar.filings f ON c.id = f.company_id
WHERE c.processing_enabled = true 
    AND c.is_active = true
    AND f.processing_status = 'pending'
    AND f.filing_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY f.processing_priority ASC, f.filing_date DESC
LIMIT 100;

-- Optimized job queue query
EXPLAIN (ANALYZE, BUFFERS)
SELECT j.id, j.job_type_id, j.parameters, j.priority
FROM trading.jobs j
INNER JOIN trading.job_types jt ON j.job_type_id = jt.id
WHERE j.status = 'pending'
    AND j.scheduled_at <= NOW()
    AND (j.next_retry_at IS NULL OR j.next_retry_at <= NOW())
    AND jt.is_active = true
ORDER BY j.priority ASC, j.scheduled_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- SLA monitoring query with proper indexing
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    DATE(f.filing_date) as filing_date,
    COUNT(*) as total_filings,
    COUNT(CASE WHEN f.processing_status = 'completed' THEN 1 END) as completed_filings,
    AVG(f.processing_duration_ms) / 1000.0 as avg_processing_seconds,
    MAX(f.processing_duration_ms) / 1000.0 as max_processing_seconds,
    COUNT(CASE WHEN f.processing_duration_ms > 1800000 THEN 1 END) as sla_violations
FROM edgar.filings f
WHERE f.filing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(f.filing_date)
ORDER BY filing_date DESC;

-- =============================================================================
-- MATERIALIZED VIEWS FOR EXPENSIVE QUERIES
-- =============================================================================

-- Daily processing summary materialized view
CREATE MATERIALIZED VIEW trading.daily_processing_summary AS
SELECT 
    DATE(f.filing_date) as processing_date,
    f.form_type,
    COUNT(*) as total_filings,
    COUNT(CASE WHEN f.processing_status = 'completed' THEN 1 END) as completed_filings,
    COUNT(CASE WHEN f.processing_status = 'failed' THEN 1 END) as failed_filings,
    AVG(f.processing_duration_ms) as avg_duration_ms,
    MAX(f.processing_duration_ms) as max_duration_ms,
    COUNT(CASE WHEN f.processing_duration_ms > 1800000 THEN 1 END) as sla_violations
FROM edgar.filings f
WHERE f.filing_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE(f.filing_date), f.form_type;

CREATE INDEX idx_daily_processing_summary_date 
ON trading.daily_processing_summary (processing_date DESC);

-- Company performance summary
CREATE MATERIALIZED VIEW trading.company_performance_summary AS
SELECT 
    c.id as company_id,
    c.ticker,
    c.company_name,
    c.sector,
    COUNT(f.id) as total_filings,
    COUNT(CASE WHEN f.processing_status = 'completed' THEN 1 END) as completed_filings,
    AVG(f.processing_duration_ms) as avg_processing_time_ms,
    MAX(f.filing_date) as last_filing_date,
    COUNT(CASE WHEN f.error_count > 0 THEN 1 END) as filings_with_errors
FROM edgar.companies c
LEFT JOIN edgar.filings f ON c.id = f.company_id 
    AND f.filing_date >= CURRENT_DATE - INTERVAL '30 days'
WHERE c.is_active = true
GROUP BY c.id, c.ticker, c.company_name, c.sector;

CREATE INDEX idx_company_performance_summary_ticker 
ON trading.company_performance_summary (ticker);

-- =============================================================================
-- AUTOMATED MAINTENANCE PROCEDURES
-- =============================================================================

-- Procedure to refresh materialized views
CREATE OR REPLACE FUNCTION trading.refresh_performance_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY trading.daily_processing_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY trading.company_performance_summary;
    
    -- Log the refresh
    INSERT INTO audit.performance_metrics (metric_name, metric_type, value_numeric, timestamp)
    VALUES ('materialized_view_refresh', 'counter', 1, NOW());
END;
$$ LANGUAGE plpgsql;

-- Procedure to clean up old data
CREATE OR REPLACE FUNCTION audit.cleanup_old_data()
RETURNS TABLE(table_name TEXT, rows_deleted BIGINT) AS $$
DECLARE
    rec RECORD;
    deleted_count BIGINT;
BEGIN
    -- Clean up old error logs (keep 90 days)
    DELETE FROM audit.error_logs 
    WHERE created_at < NOW() - INTERVAL '90 days'
        AND is_resolved = true;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'audit.error_logs';
    rows_deleted := deleted_count;
    RETURN NEXT;
    
    -- Clean up old performance metrics (keep 6 months)
    DELETE FROM audit.performance_metrics 
    WHERE timestamp < NOW() - INTERVAL '6 months';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'audit.performance_metrics';
    rows_deleted := deleted_count;
    RETURN NEXT;
    
    -- Archive old completed jobs (keep 30 days)
    DELETE FROM trading.jobs 
    WHERE completed_at < NOW() - INTERVAL '30 days'
        AND status = 'completed';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'trading.jobs';
    rows_deleted := deleted_count;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- PERFORMANCE MONITORING QUERIES
-- =============================================================================

-- Real-time processing performance
CREATE OR REPLACE VIEW trading.processing_performance_realtime AS
SELECT 
    'Current Jobs Running' as metric,
    COUNT(*) as value
FROM trading.jobs 
WHERE status = 'running'
UNION ALL
SELECT 
    'Jobs in Queue' as metric,
    COUNT(*) as value
FROM trading.jobs 
WHERE status = 'pending'
UNION ALL
SELECT 
    'Average Job Duration (seconds)' as metric,
    ROUND(AVG(EXTRACT(EPOCH FROM (completed_at - started_at))), 2) as value
FROM trading.jobs 
WHERE completed_at > NOW() - INTERVAL '1 hour'
    AND status = 'completed'
UNION ALL
SELECT 
    'Failed Jobs (last hour)' as metric,
    COUNT(*) as value
FROM trading.jobs 
WHERE status = 'failed'
    AND updated_at > NOW() - INTERVAL '1 hour';

-- SLA compliance monitoring
CREATE OR REPLACE VIEW audit.sla_compliance_realtime AS
SELECT 
    DATE(f.filing_date) as date,
    COUNT(*) as total_filings,
    COUNT(CASE WHEN f.processing_duration_ms <= 1800000 THEN 1 END) as within_sla,
    ROUND(
        (COUNT(CASE WHEN f.processing_duration_ms <= 1800000 THEN 1 END)::decimal / COUNT(*)) * 100, 
        2
    ) as sla_compliance_percent
FROM edgar.filings f
WHERE f.filing_date >= CURRENT_DATE - INTERVAL '7 days'
    AND f.processing_status = 'completed'
GROUP BY DATE(f.filing_date)
ORDER BY date DESC;

-- Database health metrics
CREATE OR REPLACE VIEW audit.database_health AS
SELECT 
    'Total Companies' as metric,
    COUNT(*) as value,
    'count' as unit
FROM edgar.companies 
WHERE is_active = true
UNION ALL
SELECT 
    'Active Filings' as metric,
    COUNT(*) as value,
    'count' as unit
FROM edgar.filings 
WHERE processing_status NOT IN ('completed', 'archived')
UNION ALL
SELECT 
    'Database Size' as metric,
    ROUND(pg_database_size(current_database()) / 1024.0 / 1024.0 / 1024.0, 2) as value,
    'GB' as unit
UNION ALL
SELECT 
    'Cache Hit Ratio' as metric,
    ROUND(
        (sum(blks_hit)::decimal / (sum(blks_hit) + sum(blks_read))) * 100, 
        2
    ) as value,
    'percent' as unit
FROM pg_stat_database 
WHERE datname = current_database();

-- =============================================================================
-- RECOMMENDED POSTGRESQL CONFIGURATION
-- =============================================================================

/*
-- postgresql.conf optimizations for financial data processing:

# Memory configuration (adjust based on available RAM)
shared_buffers = 256MB                    # 25% of RAM
effective_cache_size = 1GB               # 75% of RAM
work_mem = 16MB                          # Per-operation memory
maintenance_work_mem = 128MB             # For maintenance operations

# Checkpoint configuration
checkpoint_completion_target = 0.9       # Spread out checkpoint I/O
wal_buffers = 16MB                       # WAL buffer size
wal_compression = on                     # Compress WAL files

# Query planner
random_page_cost = 1.1                   # SSD optimization
effective_io_concurrency = 200           # SSD concurrent I/O
max_worker_processes = 8                 # Parallel processing
max_parallel_workers_per_gather = 4     # Parallel query workers

# Logging for performance monitoring
log_min_duration_statement = 1000       # Log queries > 1 second
log_checkpoints = on                     # Monitor checkpoint performance
log_connections = on                     # Monitor connections
log_disconnections = on                  # Monitor disconnections
log_lock_waits = on                      # Monitor lock contention

# Statistics collection
track_activities = on
track_counts = on
track_io_timing = on
track_functions = pl
*/

-- =============================================================================
-- PERFORMANCE TESTING QUERIES
-- =============================================================================

-- Test query performance with EXPLAIN ANALYZE
-- Run these queries to validate index effectiveness

-- 1. Test company lookup performance
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT * FROM edgar.companies 
WHERE ticker = 'AAPL' AND is_active = true;

-- 2. Test filing processing queue performance
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT f.*, c.ticker 
FROM edgar.filings f
JOIN edgar.companies c ON f.company_id = c.id
WHERE f.processing_status = 'pending'
    AND c.processing_enabled = true
ORDER BY f.processing_priority ASC, f.filing_date DESC
LIMIT 50;

-- 3. Test job queue performance
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT j.id, j.parameters, jt.name
FROM trading.jobs j
JOIN trading.job_types jt ON j.job_type_id = jt.id
WHERE j.status = 'pending'
    AND j.scheduled_at <= NOW()
ORDER BY j.priority ASC, j.scheduled_at ASC
LIMIT 10;

-- 4. Test error log query performance
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT el.*, c.ticker
FROM audit.error_logs el
LEFT JOIN edgar.companies c ON el.company_id = c.id
WHERE el.severity IN ('ERROR', 'FATAL')
    AND el.created_at >= NOW() - INTERVAL '24 hours'
    AND NOT el.is_resolved
ORDER BY el.created_at DESC;

-- Expected performance targets:
-- - Company lookup by ticker: < 1ms
-- - Filing queue queries: < 10ms for 100 rows
-- - Job queue operations: < 5ms
-- - Error log queries: < 50ms for 24-hour window