-- PostgreSQL Database Schema for GPT Trader Project
-- Designed for financial data storage with performance and scalability in mind
-- Target: Handle 50+ tickers with daily processing within 30-minute SLA

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schema for data organization
CREATE SCHEMA IF NOT EXISTS edgar;
CREATE SCHEMA IF NOT EXISTS trading;
CREATE SCHEMA IF NOT EXISTS audit;

-- =============================================================================
-- CORE COMPANY DATA
-- =============================================================================

-- Companies table - Core entity for all tickers
CREATE TABLE edgar.companies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cik VARCHAR(10) NOT NULL UNIQUE,
    ticker VARCHAR(10),
    company_name TEXT NOT NULL,
    sic VARCHAR(4),  -- Standard Industry Classification
    sector VARCHAR(100),
    industry VARCHAR(200),
    exchange VARCHAR(10),
    state_of_incorporation VARCHAR(2),
    fiscal_year_end VARCHAR(4),
    
    -- Financial metadata
    market_cap BIGINT,
    employees INTEGER,
    
    -- Status and processing flags
    is_active BOOLEAN NOT NULL DEFAULT true,
    processing_enabled BOOLEAN NOT NULL DEFAULT true,
    last_filing_check TIMESTAMP WITH TIME ZONE,
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by VARCHAR(100) DEFAULT 'system',
    updated_by VARCHAR(100) DEFAULT 'system'
);

-- Optimized indexes for companies
CREATE UNIQUE INDEX idx_companies_cik ON edgar.companies(cik);
CREATE INDEX idx_companies_ticker ON edgar.companies(ticker) WHERE ticker IS NOT NULL;
CREATE INDEX idx_companies_active ON edgar.companies(is_active, processing_enabled);
CREATE INDEX idx_companies_sector ON edgar.companies(sector) WHERE sector IS NOT NULL;
CREATE INDEX idx_companies_last_check ON edgar.companies(last_filing_check);

-- =============================================================================
-- FILING METADATA
-- =============================================================================

-- Filing types lookup table
CREATE TABLE edgar.filing_types (
    id SERIAL PRIMARY KEY,
    form_type VARCHAR(20) NOT NULL UNIQUE,
    description TEXT,
    priority INTEGER NOT NULL DEFAULT 10,  -- Lower = higher priority
    retention_days INTEGER DEFAULT 2555,   -- ~7 years default
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Insert common filing types with priorities
INSERT INTO edgar.filing_types (form_type, description, priority) VALUES
('10-K', 'Annual Report', 1),
('10-Q', 'Quarterly Report', 2),
('8-K', 'Current Report', 3),
('DEF 14A', 'Proxy Statement', 4),
('S-1', 'Registration Statement', 5),
('10-K/A', 'Annual Report Amendment', 6),
('10-Q/A', 'Quarterly Report Amendment', 7),
('8-K/A', 'Current Report Amendment', 8);

-- Main filings table
CREATE TABLE edgar.filings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES edgar.companies(id) ON DELETE CASCADE,
    
    -- EDGAR metadata
    accession_number VARCHAR(20) NOT NULL,
    form_type VARCHAR(20) NOT NULL,
    filing_date DATE NOT NULL,
    report_date DATE,
    acceptance_datetime TIMESTAMP WITH TIME ZONE,
    
    -- Document information
    primary_document VARCHAR(255),
    document_count INTEGER DEFAULT 0,
    size_bytes BIGINT,
    
    -- URLs and locations
    edgar_url TEXT,
    s3_bucket VARCHAR(100),
    s3_key_prefix VARCHAR(500),
    
    -- Processing state
    processing_status VARCHAR(20) NOT NULL DEFAULT 'discovered',
    processing_priority INTEGER NOT NULL DEFAULT 10,
    processing_started_at TIMESTAMP WITH TIME ZONE,
    processing_completed_at TIMESTAMP WITH TIME ZONE,
    processing_duration_ms INTEGER,
    
    -- Error handling
    error_count INTEGER NOT NULL DEFAULT 0,
    last_error_message TEXT,
    last_error_at TIMESTAMP WITH TIME ZONE,
    retry_after TIMESTAMP WITH TIME ZONE,
    
    -- Content metadata
    has_parsed_content BOOLEAN NOT NULL DEFAULT false,
    has_embeddings BOOLEAN NOT NULL DEFAULT false,
    text_extraction_method VARCHAR(50),
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    created_by VARCHAR(100) DEFAULT 'system',
    updated_by VARCHAR(100) DEFAULT 'system'
);

-- Comprehensive indexes for filings
CREATE UNIQUE INDEX idx_filings_accession ON edgar.filings(accession_number);
CREATE INDEX idx_filings_company_form_date ON edgar.filings(company_id, form_type, filing_date DESC);
CREATE INDEX idx_filings_status_priority ON edgar.filings(processing_status, processing_priority);
CREATE INDEX idx_filings_date_range ON edgar.filings(filing_date) WHERE processing_status != 'completed';
CREATE INDEX idx_filings_error_retry ON edgar.filings(retry_after) WHERE retry_after IS NOT NULL;
CREATE INDEX idx_filings_processing_active ON edgar.filings(processing_started_at) WHERE processing_completed_at IS NULL;

-- Partial index for unprocessed filings (performance optimization)
CREATE INDEX idx_filings_unprocessed ON edgar.filings(company_id, filing_date DESC) 
WHERE processing_status IN ('discovered', 'pending', 'processing', 'failed');

-- =============================================================================
-- DOCUMENT STORAGE
-- =============================================================================

-- Individual documents within filings
CREATE TABLE edgar.documents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id UUID NOT NULL REFERENCES edgar.filings(id) ON DELETE CASCADE,
    
    -- Document metadata
    document_name VARCHAR(255) NOT NULL,
    document_type VARCHAR(50),
    description TEXT,
    sequence INTEGER,
    size_bytes BIGINT,
    
    -- Storage locations
    s3_bucket VARCHAR(100),
    s3_key VARCHAR(500),
    local_path TEXT,
    
    -- Content processing
    content_type VARCHAR(100),
    extracted_text_length INTEGER,
    has_tables BOOLEAN DEFAULT false,
    has_images BOOLEAN DEFAULT false,
    
    -- Processing status
    processing_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    extraction_method VARCHAR(50),
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE
);

-- Optimized indexes for documents
CREATE INDEX idx_documents_filing ON edgar.documents(filing_id);
CREATE INDEX idx_documents_type_status ON edgar.documents(document_type, processing_status);
CREATE INDEX idx_documents_size ON edgar.documents(size_bytes DESC);

-- =============================================================================
-- JOB TRACKING AND ORCHESTRATION
-- =============================================================================

-- Job types for different processing tasks
CREATE TABLE trading.job_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    default_timeout_minutes INTEGER NOT NULL DEFAULT 30,
    max_retries INTEGER NOT NULL DEFAULT 3,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

INSERT INTO trading.job_types (name, description, default_timeout_minutes, max_retries) VALUES
('filing_discovery', 'Discover new filings for companies', 15, 2),
('filing_download', 'Download filing documents', 10, 3),
('text_extraction', 'Extract text from documents', 20, 2),
('content_parsing', 'Parse structured content from filings', 25, 2),
('embedding_generation', 'Generate vector embeddings', 30, 1),
('data_validation', 'Validate processed data quality', 5, 1);

-- Job execution tracking
CREATE TABLE trading.jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_type_id INTEGER NOT NULL REFERENCES trading.job_types(id),
    
    -- Job parameters
    company_id UUID REFERENCES edgar.companies(id),
    filing_id UUID REFERENCES edgar.filings(id),
    parameters JSONB,
    
    -- Execution state
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 10,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Progress tracking
    progress_current INTEGER DEFAULT 0,
    progress_total INTEGER DEFAULT 1,
    progress_message TEXT,
    
    -- Resource usage
    worker_id VARCHAR(100),
    execution_node VARCHAR(100),
    memory_usage_mb INTEGER,
    cpu_time_ms INTEGER,
    
    -- Error handling
    error_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    
    -- Results
    result_data JSONB,
    output_files TEXT[],
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by VARCHAR(100) DEFAULT 'system'
);

-- Performance indexes for job processing
CREATE INDEX idx_jobs_status_priority ON trading.jobs(status, priority, scheduled_at);
CREATE INDEX idx_jobs_company_type ON trading.jobs(company_id, job_type_id, status);
CREATE INDEX idx_jobs_retry ON trading.jobs(next_retry_at) WHERE next_retry_at IS NOT NULL;
CREATE INDEX idx_jobs_active ON trading.jobs(started_at) WHERE completed_at IS NULL;
CREATE INDEX idx_jobs_performance ON trading.jobs(job_type_id, completed_at) WHERE completed_at IS NOT NULL;

-- =============================================================================
-- ERROR LOGGING AND MONITORING
-- =============================================================================

-- Comprehensive error logging
CREATE TABLE audit.error_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Context
    job_id UUID REFERENCES trading.jobs(id),
    filing_id UUID REFERENCES edgar.filings(id),
    company_id UUID REFERENCES edgar.companies(id),
    
    -- Error details
    error_type VARCHAR(50) NOT NULL,
    error_code VARCHAR(20),
    error_message TEXT NOT NULL,
    stack_trace TEXT,
    
    -- Severity and categorization
    severity VARCHAR(10) NOT NULL DEFAULT 'ERROR',  -- DEBUG, INFO, WARN, ERROR, FATAL
    category VARCHAR(50),  -- network, parsing, validation, etc.
    is_retryable BOOLEAN NOT NULL DEFAULT true,
    
    -- Context data
    request_data JSONB,
    response_data JSONB,
    environment_data JSONB,
    
    -- Resolution tracking
    is_resolved BOOLEAN NOT NULL DEFAULT false,
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolution_notes TEXT,
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    source_module VARCHAR(100),
    source_function VARCHAR(100)
);

-- Error log indexes for monitoring and analysis
CREATE INDEX idx_error_logs_severity_time ON audit.error_logs(severity, created_at DESC);
CREATE INDEX idx_error_logs_company ON audit.error_logs(company_id, created_at DESC);
CREATE INDEX idx_error_logs_unresolved ON audit.error_logs(is_resolved, severity) WHERE NOT is_resolved;
CREATE INDEX idx_error_logs_category ON audit.error_logs(category, created_at DESC);

-- =============================================================================
-- PROCESSING STATE MANAGEMENT
-- =============================================================================

-- Global processing state and configuration
CREATE TABLE trading.processing_state (
    id SERIAL PRIMARY KEY,
    key VARCHAR(100) NOT NULL UNIQUE,
    value TEXT,
    value_type VARCHAR(20) NOT NULL DEFAULT 'string',  -- string, integer, boolean, json
    description TEXT,
    is_system BOOLEAN NOT NULL DEFAULT false,
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_by VARCHAR(100) DEFAULT 'system'
);

-- Initialize key processing states
INSERT INTO trading.processing_state (key, value, value_type, description, is_system) VALUES
('last_company_sync', NULL, 'timestamp', 'Last time company data was synchronized', true),
('daily_processing_start', NULL, 'timestamp', 'Start time of current daily processing cycle', true),
('daily_processing_status', 'idle', 'string', 'Current status of daily processing', true),
('max_concurrent_jobs', '10', 'integer', 'Maximum number of concurrent jobs', false),
('filing_retention_days', '2555', 'integer', 'Default retention period for filings (7 years)', false),
('rate_limit_per_second', '10', 'integer', 'API rate limit per second', false);

-- =============================================================================
-- PERFORMANCE MONITORING
-- =============================================================================

-- Performance metrics tracking
CREATE TABLE audit.performance_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Metric identification
    metric_name VARCHAR(100) NOT NULL,
    metric_type VARCHAR(20) NOT NULL,  -- counter, gauge, histogram, timing
    
    -- Values
    value_numeric DECIMAL(15,6),
    value_text TEXT,
    
    -- Dimensions
    company_id UUID REFERENCES edgar.companies(id),
    job_type VARCHAR(50),
    source_module VARCHAR(100),
    
    -- Tags for grouping
    tags JSONB,
    
    -- Timing
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Metadata
    metadata JSONB
);

-- Time-series partitioning index for performance metrics
CREATE INDEX idx_performance_metrics_time_name ON audit.performance_metrics(timestamp DESC, metric_name);
CREATE INDEX idx_performance_metrics_company_time ON audit.performance_metrics(company_id, timestamp DESC);

-- =============================================================================
-- DATA QUALITY AND VALIDATION
-- =============================================================================

-- Data quality checks and validation rules
CREATE TABLE audit.data_quality_checks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Target
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    check_type VARCHAR(50) NOT NULL,  -- completeness, accuracy, consistency, timeliness
    
    -- Check definition
    check_name VARCHAR(200) NOT NULL,
    check_query TEXT NOT NULL,
    threshold_value DECIMAL(10,4),
    threshold_operator VARCHAR(10),  -- >, <, =, >=, <=
    
    -- Status
    is_active BOOLEAN NOT NULL DEFAULT true,
    severity VARCHAR(10) NOT NULL DEFAULT 'WARN',
    
    -- Execution
    last_run_at TIMESTAMP WITH TIME ZONE,
    last_result DECIMAL(10,4),
    last_status VARCHAR(20),  -- passed, failed, error
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- TRIGGERS FOR AUDIT FIELDS
-- =============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply updated_at triggers to all main tables
CREATE TRIGGER trigger_companies_updated_at 
    BEFORE UPDATE ON edgar.companies 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_filings_updated_at 
    BEFORE UPDATE ON edgar.filings 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_documents_updated_at 
    BEFORE UPDATE ON edgar.documents 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_jobs_updated_at 
    BEFORE UPDATE ON trading.jobs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- VIEWS FOR COMMON QUERIES
-- =============================================================================

-- View for active processing pipeline
CREATE VIEW trading.processing_pipeline AS
SELECT 
    c.ticker,
    c.company_name,
    f.form_type,
    f.filing_date,
    f.processing_status,
    f.processing_priority,
    j.status as job_status,
    j.scheduled_at,
    j.progress_current,
    j.progress_total,
    CASE 
        WHEN j.progress_total > 0 THEN 
            ROUND((j.progress_current::decimal / j.progress_total) * 100, 2)
        ELSE 0 
    END as progress_percentage
FROM edgar.companies c
JOIN edgar.filings f ON c.id = f.company_id
LEFT JOIN trading.jobs j ON f.id = j.filing_id
WHERE c.is_active = true 
    AND c.processing_enabled = true
    AND f.processing_status NOT IN ('completed', 'archived');

-- View for SLA monitoring
CREATE VIEW audit.sla_monitoring AS
SELECT 
    DATE(f.filing_date) as filing_date,
    COUNT(*) as total_filings,
    COUNT(CASE WHEN f.processing_status = 'completed' THEN 1 END) as completed_filings,
    AVG(f.processing_duration_ms) / 1000.0 as avg_processing_seconds,
    MAX(f.processing_duration_ms) / 1000.0 as max_processing_seconds,
    COUNT(CASE WHEN f.processing_duration_ms > 1800000 THEN 1 END) as sla_violations  -- 30 min = 1800000 ms
FROM edgar.filings f
WHERE f.filing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(f.filing_date)
ORDER BY filing_date DESC;

-- =============================================================================
-- STORED PROCEDURES FOR COMMON OPERATIONS
-- =============================================================================

-- Procedure to mark filing as processed
CREATE OR REPLACE FUNCTION edgar.mark_filing_processed(
    p_filing_id UUID,
    p_duration_ms INTEGER DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    UPDATE edgar.filings 
    SET 
        processing_status = CASE 
            WHEN p_error_message IS NULL THEN 'completed'
            ELSE 'failed'
        END,
        processing_completed_at = NOW(),
        processing_duration_ms = COALESCE(p_duration_ms, 
            EXTRACT(EPOCH FROM (NOW() - processing_started_at)) * 1000),
        last_error_message = p_error_message,
        last_error_at = CASE WHEN p_error_message IS NOT NULL THEN NOW() ELSE NULL END,
        error_count = CASE 
            WHEN p_error_message IS NOT NULL THEN error_count + 1 
            ELSE error_count 
        END,
        processed_at = NOW()
    WHERE id = p_filing_id;
END;
$$ LANGUAGE plpgsql;

-- Procedure to get next job for processing
CREATE OR REPLACE FUNCTION trading.get_next_job(p_worker_id VARCHAR(100))
RETURNS UUID AS $$
DECLARE
    v_job_id UUID;
BEGIN
    -- Get highest priority pending job
    SELECT id INTO v_job_id
    FROM trading.jobs
    WHERE status = 'pending'
        AND scheduled_at <= NOW()
        AND (next_retry_at IS NULL OR next_retry_at <= NOW())
    ORDER BY priority ASC, scheduled_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED;
    
    -- Mark job as running
    IF v_job_id IS NOT NULL THEN
        UPDATE trading.jobs
        SET 
            status = 'running',
            started_at = NOW(),
            worker_id = p_worker_id
        WHERE id = v_job_id;
    END IF;
    
    RETURN v_job_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================

COMMENT ON SCHEMA edgar IS 'EDGAR filing data and metadata';
COMMENT ON SCHEMA trading IS 'Trading job orchestration and state management';
COMMENT ON SCHEMA audit IS 'Audit logs, errors, and performance monitoring';

COMMENT ON TABLE edgar.companies IS 'Master company registry with SEC and market data';
COMMENT ON TABLE edgar.filings IS 'SEC filing metadata and processing state';
COMMENT ON TABLE edgar.documents IS 'Individual documents within filings';
COMMENT ON TABLE trading.jobs IS 'Job execution tracking for processing pipeline';
COMMENT ON TABLE audit.error_logs IS 'Comprehensive error logging and tracking';

COMMENT ON COLUMN edgar.filings.processing_status IS 'discovered, pending, processing, completed, failed, archived';
COMMENT ON COLUMN edgar.filings.processing_priority IS 'Lower numbers = higher priority (1-10)';
COMMENT ON COLUMN trading.jobs.status IS 'pending, running, completed, failed, cancelled';

-- =============================================================================
-- GRANTS AND SECURITY
-- =============================================================================

-- Create application roles
CREATE ROLE gpt_trader_app;
CREATE ROLE gpt_trader_readonly;
CREATE ROLE gpt_trader_admin;

-- Grant appropriate permissions
GRANT USAGE ON SCHEMA edgar, trading, audit TO gpt_trader_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA edgar, trading, audit TO gpt_trader_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA edgar, trading, audit TO gpt_trader_app;

GRANT USAGE ON SCHEMA edgar, trading, audit TO gpt_trader_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA edgar, trading, audit TO gpt_trader_readonly;

GRANT ALL PRIVILEGES ON SCHEMA edgar, trading, audit TO gpt_trader_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA edgar, trading, audit TO gpt_trader_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA edgar, trading, audit TO gpt_trader_admin;