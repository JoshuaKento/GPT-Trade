#!/usr/bin/env python3
"""
GPT Trader CLI - Command line interface for the GPT Trader platform.

This script provides a comprehensive CLI for managing the GPT Trader ETL pipeline,
database operations, and monitoring.
"""

import argparse
import sys
import os
import json
import logging
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from gpt_trader.config import GPTTraderConfig, ConfigManager, TickerConfig
from gpt_trader.database import get_database_manager, initialize_database, health_check
from gpt_trader.etl import ETLPipeline, ETLScheduler
from gpt_trader.filing_processor_db import BatchFilingProcessor
from gpt_trader.models import Company, Filing, ProcessingJob, ProcessingStatus
from gpt_trader.database import session_scope


def setup_logging(level: str = "INFO"):
    """Set up logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('gpt_trader.log')
        ]
    )


def cmd_init_db(args):
    """Initialize database schema."""
    print("Initializing database...")
    
    try:
        initialize_database(drop_existing=args.reset)
        print("✅ Database initialized successfully")
        
        if args.reset:
            print("⚠️  Existing data was dropped")
            
    except Exception as e:
        print(f"❌ Failed to initialize database: {e}")
        sys.exit(1)


def cmd_db_status(args):
    """Show database status and statistics."""
    print("Database Status:")
    print("=" * 50)
    
    try:
        # Health check
        if health_check():
            print("✅ Database connection: HEALTHY")
        else:
            print("❌ Database connection: FAILED")
            return
        
        # Connection info
        db_manager = get_database_manager()
        info = db_manager.get_connection_info()
        print(f"Database URL: {info['database_url']}")
        print(f"Database Type: {'SQLite' if info['is_sqlite'] else 'PostgreSQL'}")
        print(f"Pool Size: {info['pool_size']}")
        
        # Statistics
        with session_scope() as session:
            company_count = session.query(Company).count()
            active_companies = session.query(Company).filter(Company.is_active == True).count()
            filing_count = session.query(Filing).count()
            processed_filings = session.query(Filing).filter(Filing.is_processed == True).count()
            
            print("\nData Statistics:")
            print(f"Companies: {company_count} total, {active_companies} active")
            print(f"Filings: {filing_count} total, {processed_filings} processed")
            
            # Recent jobs
            recent_jobs = session.query(ProcessingJob).order_by(
                ProcessingJob.created_at.desc()
            ).limit(5).all()
            
            print(f"\nRecent Jobs ({len(recent_jobs)}):")
            for job in recent_jobs:
                status_emoji = "✅" if job.status == ProcessingStatus.COMPLETED else "❌" if job.status == ProcessingStatus.FAILED else "⏳"
                print(f"  {status_emoji} {job.job_name} ({job.status}) - {job.created_at.strftime('%Y-%m-%d %H:%M')}")
                
    except Exception as e:
        print(f"❌ Failed to get database status: {e}")
        sys.exit(1)


def cmd_config_show(args):
    """Show current configuration."""
    try:
        config_manager = ConfigManager(args.config_file)
        config = config_manager.config
        
        print("GPT Trader Configuration:")
        print("=" * 50)
        print(json.dumps(config.to_dict(), indent=2, default=str))
        
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        sys.exit(1)


def cmd_config_create_sample(args):
    """Create sample configuration file."""
    try:
        config = GPTTraderConfig()
        
        # Add sample tickers
        config.add_ticker("AAPL", "0000320193", priority=1, forms=["10-K", "10-Q", "8-K"])
        config.add_ticker("MSFT", "0000789019", priority=1, forms=["10-K", "10-Q"])
        config.add_ticker("GOOGL", "0001652044", priority=2, forms=["10-K", "10-Q"])
        config.add_ticker("AMZN", "0001018724", priority=2, forms=["10-K", "10-Q"])
        config.add_ticker("TSLA", "0001318605", priority=3, forms=["10-K", "10-Q"])
        
        # Save to file
        output_file = args.output or "gpt_trader_config.json"
        config.to_file(output_file)
        
        print(f"✅ Sample configuration created: {output_file}")
        print(f"📝 Added {len(config.tickers)} sample tickers")
        
    except Exception as e:
        print(f"❌ Failed to create sample configuration: {e}")
        sys.exit(1)


def cmd_add_ticker(args):
    """Add ticker to configuration."""
    try:
        config_manager = ConfigManager(args.config_file)
        config = config_manager.config
        
        # Add ticker
        forms = args.forms.split(',') if args.forms else ["10-K", "10-Q", "8-K"]
        config.add_ticker(args.ticker, args.cik, args.priority, forms)
        
        # Save configuration
        config_manager.save_config()
        
        print(f"✅ Added ticker {args.ticker} (CIK: {args.cik}) to configuration")
        
    except Exception as e:
        print(f"❌ Failed to add ticker: {e}")
        sys.exit(1)


def cmd_sync_companies(args):
    """Sync companies from configuration to database."""
    try:
        config_manager = ConfigManager(args.config_file)
        config = config_manager.config
        
        print("Syncing companies to database...")
        
        from gpt_trader.filing_processor_db import FilingProcessorDB
        from edgar.client_new import EdgarClient, ClientConfig
        
        # Create processor
        client_config = ClientConfig(
            rate_limit_per_sec=config.edgar.rate_limit_per_sec,
            user_agent=config.edgar.user_agent
        )
        edgar_client = EdgarClient(client_config)
        processor = FilingProcessorDB(edgar_client, config=config)
        
        # Sync each ticker
        synced_count = 0
        for ticker_config in config.tickers:
            try:
                company = processor.sync_company_from_ticker_config(ticker_config)
                print(f"✅ Synced {ticker_config.ticker} -> Company ID {company.id}")
                synced_count += 1
            except Exception as e:
                print(f"❌ Failed to sync {ticker_config.ticker}: {e}")
        
        print(f"✅ Synced {synced_count} companies to database")
        
    except Exception as e:
        print(f"❌ Failed to sync companies: {e}")
        sys.exit(1)


def cmd_run_etl(args):
    """Run ETL process for specified tickers or all active tickers."""
    try:
        config_manager = ConfigManager(args.config_file)
        config = config_manager.config
        
        print("Starting ETL process...")
        
        # Create batch processor
        batch_processor = BatchFilingProcessor(config)
        
        if args.ticker:
            # Process specific ticker
            ticker_config = config.get_ticker_by_symbol(args.ticker)
            if not ticker_config:
                print(f"❌ Ticker {args.ticker} not found in configuration")
                sys.exit(1)
            
            print(f"Processing ticker: {args.ticker}")
            new_filings, updated_filings = batch_processor.processor.fetch_and_store_filings(
                ticker_config, max_filings=args.max_filings
            )
            print(f"✅ Completed {args.ticker}: {new_filings} new, {updated_filings} updated filings")
            
        else:
            # Process all active tickers
            job = batch_processor.process_all_active_tickers(
                job_name=args.job_name or f"cli_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            
            print(f"✅ Batch ETL completed")
            print(f"Job ID: {job.id}")
            print(f"Status: {job.status}")
            print(f"Duration: {job.duration:.2f} seconds" if job.duration else "N/A")
            
    except Exception as e:
        print(f"❌ ETL process failed: {e}")
        sys.exit(1)


def cmd_start_scheduler(args):
    """Start ETL scheduler daemon."""
    try:
        config_manager = ConfigManager(args.config_file)
        config = config_manager.config
        
        print("Starting ETL scheduler...")
        
        scheduler = ETLScheduler(config)
        scheduler.start()
        
        print("✅ ETL scheduler started")
        print("Press Ctrl+C to stop...")
        
        try:
            import time
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping scheduler...")
            scheduler.stop()
            print("✅ Scheduler stopped")
            
    except Exception as e:
        print(f"❌ Failed to start scheduler: {e}")
        sys.exit(1)


def cmd_list_jobs(args):
    """List recent processing jobs."""
    try:
        print("Recent Processing Jobs:")
        print("=" * 80)
        
        with session_scope() as session:
            query = session.query(ProcessingJob).order_by(ProcessingJob.created_at.desc())
            
            if args.status:
                query = query.filter(ProcessingJob.status == args.status)
            
            if args.limit:
                query = query.limit(args.limit)
            
            jobs = query.all()
            
            if not jobs:
                print("No jobs found")
                return
            
            # Print header
            print(f"{'ID':<6} {'Job Name':<30} {'Status':<12} {'Duration':<10} {'Items':<8} {'Created':<16}")
            print("-" * 80)
            
            for job in jobs:
                duration_str = f"{job.duration:.1f}s" if job.duration else "N/A"
                items_str = f"{job.processed_items}/{job.total_items or 0}"
                created_str = job.created_at.strftime('%m/%d %H:%M')
                
                print(f"{job.id:<6} {job.job_name[:29]:<30} {job.status:<12} {duration_str:<10} {items_str:<8} {created_str:<16}")
                
    except Exception as e:
        print(f"❌ Failed to list jobs: {e}")
        sys.exit(1)


def cmd_job_status(args):
    """Show detailed status of a specific job."""
    try:
        with session_scope() as session:
            job = session.query(ProcessingJob).filter(ProcessingJob.id == args.job_id).first()
            
            if not job:
                print(f"❌ Job {args.job_id} not found")
                sys.exit(1)
            
            print(f"Job Details (ID: {job.id}):")
            print("=" * 50)
            print(f"Name: {job.job_name}")
            print(f"Type: {job.job_type}")
            print(f"Status: {job.status}")
            print(f"UUID: {job.job_uuid}")
            
            if job.ticker_list:
                print(f"Tickers: {', '.join(job.ticker_list)}")
            
            print(f"Created: {job.created_at}")
            print(f"Started: {job.started_at or 'N/A'}")
            print(f"Completed: {job.completed_at or 'N/A'}")
            print(f"Duration: {job.duration:.2f}s" if job.duration else "N/A")
            
            if job.total_items:
                progress = (job.processed_items / job.total_items) * 100
                print(f"Progress: {job.processed_items}/{job.total_items} ({progress:.1f}%)")
            
            if job.error_message:
                print(f"Error: {job.error_message}")
            
            if job.parameters:
                print(f"Parameters: {json.dumps(job.parameters, indent=2)}")
                
    except Exception as e:
        print(f"❌ Failed to get job status: {e}")
        sys.exit(1)


def cmd_monitor(args):
    """Start monitoring dashboard."""
    try:
        config_manager = ConfigManager(args.config_file)
        config = config_manager.config
        
        print("GPT Trader Monitoring Dashboard")
        print("=" * 50)
        print("Press Ctrl+C to exit")
        
        import time
        from gpt_trader.monitoring import setup_monitoring
        
        performance_monitor, sla_monitor, alert_manager = setup_monitoring(config)
        performance_monitor.start()
        sla_monitor.start()
        
        try:
            while True:
                # Clear screen (works on most terminals)
                os.system('cls' if os.name == 'nt' else 'clear')
                
                print(f"GPT Trader Monitoring - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 80)
                
                # Get metrics
                metrics = performance_monitor.get_metrics()
                sla_status = sla_monitor.get_status()
                
                # System metrics
                if 'system' in metrics:
                    sys_metrics = metrics['system']
                    print(f"System - CPU: {sys_metrics.get('cpu_percent', 'N/A'):.1f}% | "
                          f"Memory: {sys_metrics.get('memory_percent', 'N/A'):.1f}% | "
                          f"Disk: {sys_metrics.get('disk_percent', 'N/A'):.1f}%")
                
                # SLA status
                compliance = sla_status['compliance_status']
                emoji = "✅" if compliance == "healthy" else "⚠️"
                print(f"SLA Status: {emoji} {compliance.upper()} | "
                      f"Violations (24h): {sla_status['violations_24h']}")
                
                # Database status
                if health_check():
                    with session_scope() as session:
                        pending_filings = session.query(Filing).filter(
                            Filing.is_processed == False
                        ).count()
                        active_jobs = session.query(ProcessingJob).filter(
                            ProcessingJob.status == ProcessingStatus.IN_PROGRESS
                        ).count()
                        
                        print(f"Database: ✅ Connected | "
                              f"Pending Filings: {pending_filings} | "
                              f"Active Jobs: {active_jobs}")
                else:
                    print("Database: ❌ Disconnected")
                
                print("-" * 80)
                time.sleep(args.refresh or 5)
                
        except KeyboardInterrupt:
            print("\n🛑 Stopping monitoring...")
            performance_monitor.stop()
            sla_monitor.stop()
            print("✅ Monitoring stopped")
            
    except Exception as e:
        print(f"❌ Failed to start monitoring: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="GPT Trader CLI - Financial data ETL platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s init-db --reset              Initialize database with clean slate
  %(prog)s config create-sample         Create sample configuration
  %(prog)s add-ticker NVDA 0000320193   Add NVIDIA ticker
  %(prog)s sync-companies               Sync companies to database
  %(prog)s run-etl --ticker AAPL        Process single ticker
  %(prog)s run-etl                      Process all active tickers
  %(prog)s start-scheduler              Start ETL scheduler daemon
  %(prog)s monitor                      Start monitoring dashboard
        """
    )
    
    parser.add_argument('--config-file', '-c', 
                       help='Configuration file path',
                       default=None)
    parser.add_argument('--log-level', '-l',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO',
                       help='Logging level')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Database commands
    db_parser = subparsers.add_parser('init-db', help='Initialize database')
    db_parser.add_argument('--reset', action='store_true', 
                          help='Drop existing tables first')
    
    subparsers.add_parser('db-status', help='Show database status')
    
    # Configuration commands
    config_parser = subparsers.add_parser('config', help='Configuration management')
    config_subparsers = config_parser.add_subparsers(dest='config_command')
    
    config_subparsers.add_parser('show', help='Show current configuration')
    
    sample_parser = config_subparsers.add_parser('create-sample', help='Create sample configuration')
    sample_parser.add_argument('--output', '-o', help='Output file path')
    
    # Ticker management
    ticker_parser = subparsers.add_parser('add-ticker', help='Add ticker to configuration')
    ticker_parser.add_argument('ticker', help='Ticker symbol (e.g., AAPL)')
    ticker_parser.add_argument('cik', help='SEC CIK number')
    ticker_parser.add_argument('--priority', type=int, default=1, help='Processing priority')
    ticker_parser.add_argument('--forms', help='Comma-separated form types (e.g., 10-K,10-Q)')
    
    subparsers.add_parser('sync-companies', help='Sync companies from config to database')
    
    # ETL commands
    etl_parser = subparsers.add_parser('run-etl', help='Run ETL process')
    etl_parser.add_argument('--ticker', help='Process specific ticker only')
    etl_parser.add_argument('--max-filings', type=int, help='Maximum filings to process')
    etl_parser.add_argument('--job-name', help='Custom job name')
    
    subparsers.add_parser('start-scheduler', help='Start ETL scheduler daemon')
    
    # Job management
    jobs_parser = subparsers.add_parser('list-jobs', help='List processing jobs')
    jobs_parser.add_argument('--status', help='Filter by job status')
    jobs_parser.add_argument('--limit', type=int, default=20, help='Limit number of results')
    
    status_parser = subparsers.add_parser('job-status', help='Show job details')
    status_parser.add_argument('job_id', type=int, help='Job ID')
    
    # Monitoring
    monitor_parser = subparsers.add_parser('monitor', help='Start monitoring dashboard')
    monitor_parser.add_argument('--refresh', type=int, default=5, help='Refresh interval in seconds')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Set up logging
    setup_logging(args.log_level)
    
    # Route to appropriate command handler
    command_handlers = {
        'init-db': cmd_init_db,
        'db-status': cmd_db_status,
        'config': lambda a: cmd_config_show(a) if a.config_command == 'show' else cmd_config_create_sample(a),
        'add-ticker': cmd_add_ticker,
        'sync-companies': cmd_sync_companies,
        'run-etl': cmd_run_etl,
        'start-scheduler': cmd_start_scheduler,
        'list-jobs': cmd_list_jobs,
        'job-status': cmd_job_status,
        'monitor': cmd_monitor
    }
    
    handler = command_handlers.get(args.command)
    if handler:
        handler(args)
    else:
        print(f"❌ Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()