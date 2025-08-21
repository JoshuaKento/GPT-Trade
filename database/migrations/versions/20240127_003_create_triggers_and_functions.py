"""Create database triggers, functions, and stored procedures

Revision ID: 20240127_003
Revises: 20240127_002
Create Date: 2024-01-27 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '20240127_003'
down_revision: Union[str, None] = '20240127_002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create database functions, triggers, and stored procedures."""
    
    # =============================================================================
    # TRIGGER FUNCTIONS
    # =============================================================================
    
    # Function to update updated_at timestamp
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    
    # =============================================================================
    # TRIGGERS FOR AUDIT FIELDS
    # =============================================================================
    
    # Apply updated_at triggers to all main tables
    op.execute("""
        CREATE TRIGGER trigger_companies_updated_at 
        BEFORE UPDATE ON edgar.companies 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """)
    
    op.execute("""
        CREATE TRIGGER trigger_filings_updated_at 
        BEFORE UPDATE ON edgar.filings 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """)
    
    op.execute("""
        CREATE TRIGGER trigger_documents_updated_at 
        BEFORE UPDATE ON edgar.documents 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """)
    
    op.execute("""
        CREATE TRIGGER trigger_jobs_updated_at 
        BEFORE UPDATE ON trading.jobs 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """)
    
    op.execute("""
        CREATE TRIGGER trigger_processing_state_updated_at 
        BEFORE UPDATE ON trading.processing_state 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """)
    
    op.execute("""
        CREATE TRIGGER trigger_data_quality_checks_updated_at 
        BEFORE UPDATE ON audit.data_quality_checks 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """)
    
    # =============================================================================
    # STORED PROCEDURES FOR COMMON OPERATIONS
    # =============================================================================
    
    # Procedure to mark filing as processed
    op.execute("""
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
    """)
    
    # Procedure to get next job for processing
    op.execute("""
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
    """)
    
    # Procedure to complete a job
    op.execute("""
        CREATE OR REPLACE FUNCTION trading.complete_job(
            p_job_id UUID,
            p_status VARCHAR(20),
            p_error_message TEXT DEFAULT NULL,
            p_result_data JSONB DEFAULT NULL,
            p_output_files TEXT[] DEFAULT NULL
        )
        RETURNS VOID AS $$
        BEGIN
            UPDATE trading.jobs
            SET 
                status = p_status,
                completed_at = NOW(),
                error_message = p_error_message,
                result_data = p_result_data,
                output_files = p_output_files,
                retry_count = CASE 
                    WHEN p_status = 'failed' THEN retry_count + 1 
                    ELSE retry_count 
                END,
                next_retry_at = CASE 
                    WHEN p_status = 'failed' AND retry_count < max_retries THEN 
                        NOW() + INTERVAL '5 minutes' * POWER(2, retry_count)  -- Exponential backoff
                    ELSE NULL 
                END
            WHERE id = p_job_id;
        END;
        $$ LANGUAGE plpgsql;
    """)
    
    # Procedure to get processing state value
    op.execute("""
        CREATE OR REPLACE FUNCTION trading.get_processing_state(p_key VARCHAR(100))
        RETURNS TEXT AS $$
        DECLARE
            v_value TEXT;
        BEGIN
            SELECT value INTO v_value
            FROM trading.processing_state
            WHERE key = p_key;
            
            RETURN v_value;
        END;
        $$ LANGUAGE plpgsql;
    """)
    
    # Procedure to set processing state value
    op.execute("""
        CREATE OR REPLACE FUNCTION trading.set_processing_state(
            p_key VARCHAR(100),
            p_value TEXT,
            p_value_type VARCHAR(20) DEFAULT 'string',
            p_description TEXT DEFAULT NULL
        )
        RETURNS VOID AS $$
        BEGIN
            INSERT INTO trading.processing_state (key, value, value_type, description)
            VALUES (p_key, p_value, p_value_type, p_description)
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                value_type = EXCLUDED.value_type,
                description = COALESCE(EXCLUDED.description, processing_state.description),
                updated_at = NOW();
        END;
        $$ LANGUAGE plpgsql;
    """)
    
    # Procedure to log errors
    op.execute("""
        CREATE OR REPLACE FUNCTION audit.log_error(
            p_error_type VARCHAR(50),
            p_error_message TEXT,
            p_severity VARCHAR(10) DEFAULT 'ERROR',
            p_category VARCHAR(50) DEFAULT NULL,
            p_company_id UUID DEFAULT NULL,
            p_filing_id UUID DEFAULT NULL,
            p_job_id UUID DEFAULT NULL,
            p_source_module VARCHAR(100) DEFAULT NULL,
            p_source_function VARCHAR(100) DEFAULT NULL,
            p_stack_trace TEXT DEFAULT NULL,
            p_request_data JSONB DEFAULT NULL,
            p_response_data JSONB DEFAULT NULL
        )
        RETURNS UUID AS $$
        DECLARE
            v_error_id UUID;
        BEGIN
            INSERT INTO audit.error_logs (
                error_type, error_message, severity, category,
                company_id, filing_id, job_id,
                source_module, source_function, stack_trace,
                request_data, response_data
            )
            VALUES (
                p_error_type, p_error_message, p_severity, p_category,
                p_company_id, p_filing_id, p_job_id,
                p_source_module, p_source_function, p_stack_trace,
                p_request_data, p_response_data
            )
            RETURNING id INTO v_error_id;
            
            RETURN v_error_id;
        END;
        $$ LANGUAGE plpgsql;
    """)
    
    # Procedure to record performance metrics
    op.execute("""
        CREATE OR REPLACE FUNCTION audit.record_metric(
            p_metric_name VARCHAR(100),
            p_metric_type VARCHAR(20),
            p_value_numeric DECIMAL(15,6) DEFAULT NULL,
            p_value_text TEXT DEFAULT NULL,
            p_company_id UUID DEFAULT NULL,
            p_job_type VARCHAR(50) DEFAULT NULL,
            p_source_module VARCHAR(100) DEFAULT NULL,
            p_tags JSONB DEFAULT NULL,
            p_metadata JSONB DEFAULT NULL
        )
        RETURNS UUID AS $$
        DECLARE
            v_metric_id UUID;
        BEGIN
            INSERT INTO audit.performance_metrics (
                metric_name, metric_type, value_numeric, value_text,
                company_id, job_type, source_module, tags, metadata
            )
            VALUES (
                p_metric_name, p_metric_type, p_value_numeric, p_value_text,
                p_company_id, p_job_type, p_source_module, p_tags, p_metadata
            )
            RETURNING id INTO v_metric_id;
            
            RETURN v_metric_id;
        END;
        $$ LANGUAGE plpgsql;
    """)
    
    # =============================================================================
    # MAINTENANCE PROCEDURES
    # =============================================================================
    
    # Procedure to clean up old data
    op.execute("""
        CREATE OR REPLACE FUNCTION audit.cleanup_old_data()
        RETURNS TABLE(table_name TEXT, rows_deleted BIGINT) AS $$
        DECLARE
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
    """)
    
    # Procedure to analyze table statistics
    op.execute("""
        CREATE OR REPLACE FUNCTION audit.analyze_table_stats()
        RETURNS TABLE(
            schema_name TEXT,
            table_name TEXT,
            total_size TEXT,
            table_size TEXT,
            index_size TEXT,
            live_rows BIGINT,
            dead_rows BIGINT
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                schemaname::TEXT,
                tablename::TEXT,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))::TEXT as total_size,
                pg_size_pretty(pg_relation_size(schemaname||'.'||tablename))::TEXT as table_size,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - 
                              pg_relation_size(schemaname||'.'||tablename))::TEXT as index_size,
                n_live_tup as live_rows,
                n_dead_tup as dead_rows
            FROM pg_stat_user_tables 
            WHERE schemaname IN ('edgar', 'trading', 'audit')
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
        END;
        $$ LANGUAGE plpgsql;
    """)
    
    # =============================================================================
    # INITIAL DATA SETUP
    # =============================================================================
    
    # Insert common filing types
    op.execute("""
        INSERT INTO edgar.filing_types (form_type, description, priority) VALUES
        ('10-K', 'Annual Report', 1),
        ('10-Q', 'Quarterly Report', 2),
        ('8-K', 'Current Report', 3),
        ('DEF 14A', 'Proxy Statement', 4),
        ('S-1', 'Registration Statement', 5),
        ('10-K/A', 'Annual Report Amendment', 6),
        ('10-Q/A', 'Quarterly Report Amendment', 7),
        ('8-K/A', 'Current Report Amendment', 8);
    """)
    
    # Insert job types
    op.execute("""
        INSERT INTO trading.job_types (name, description, default_timeout_minutes, max_retries) VALUES
        ('filing_discovery', 'Discover new filings for companies', 15, 2),
        ('filing_download', 'Download filing documents', 10, 3),
        ('text_extraction', 'Extract text from documents', 20, 2),
        ('content_parsing', 'Parse structured content from filings', 25, 2),
        ('embedding_generation', 'Generate vector embeddings', 30, 1),
        ('data_validation', 'Validate processed data quality', 5, 1);
    """)
    
    # Initialize processing state
    op.execute("""
        INSERT INTO trading.processing_state (key, value, value_type, description, is_system) VALUES
        ('last_company_sync', NULL, 'timestamp', 'Last time company data was synchronized', true),
        ('daily_processing_start', NULL, 'timestamp', 'Start time of current daily processing cycle', true),
        ('daily_processing_status', 'idle', 'string', 'Current status of daily processing', true),
        ('max_concurrent_jobs', '10', 'integer', 'Maximum number of concurrent jobs', false),
        ('filing_retention_days', '2555', 'integer', 'Default retention period for filings (7 years)', false),
        ('rate_limit_per_second', '10', 'integer', 'API rate limit per second', false);
    """)


def downgrade() -> None:
    """Remove database functions, triggers, and stored procedures."""
    
    # Drop triggers
    op.execute("DROP TRIGGER IF EXISTS trigger_data_quality_checks_updated_at ON audit.data_quality_checks")
    op.execute("DROP TRIGGER IF EXISTS trigger_processing_state_updated_at ON trading.processing_state")
    op.execute("DROP TRIGGER IF EXISTS trigger_jobs_updated_at ON trading.jobs")
    op.execute("DROP TRIGGER IF EXISTS trigger_documents_updated_at ON edgar.documents")
    op.execute("DROP TRIGGER IF EXISTS trigger_filings_updated_at ON edgar.filings")
    op.execute("DROP TRIGGER IF EXISTS trigger_companies_updated_at ON edgar.companies")
    
    # Drop stored procedures and functions
    op.execute("DROP FUNCTION IF EXISTS audit.analyze_table_stats()")
    op.execute("DROP FUNCTION IF EXISTS audit.cleanup_old_data()")
    op.execute("DROP FUNCTION IF EXISTS audit.record_metric(VARCHAR, VARCHAR, DECIMAL, TEXT, UUID, VARCHAR, VARCHAR, JSONB, JSONB)")
    op.execute("DROP FUNCTION IF EXISTS audit.log_error(VARCHAR, TEXT, VARCHAR, VARCHAR, UUID, UUID, UUID, VARCHAR, VARCHAR, TEXT, JSONB, JSONB)")
    op.execute("DROP FUNCTION IF EXISTS trading.set_processing_state(VARCHAR, TEXT, VARCHAR, TEXT)")
    op.execute("DROP FUNCTION IF EXISTS trading.get_processing_state(VARCHAR)")
    op.execute("DROP FUNCTION IF EXISTS trading.complete_job(UUID, VARCHAR, TEXT, JSONB, TEXT[])")
    op.execute("DROP FUNCTION IF EXISTS trading.get_next_job(VARCHAR)")
    op.execute("DROP FUNCTION IF EXISTS edgar.mark_filing_processed(UUID, INTEGER, TEXT)")
    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column()")