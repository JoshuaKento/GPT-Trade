"""Command-line interface for running ETL jobs and monitoring orchestration."""

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import yaml

from .orchestrator import OrchestrationManager, OrchestrationConfig, TickerConfig
from .scheduler import Priority, ScheduleType
from .progress import ProgressStatus


@dataclass
class CLIConfig:
    """Configuration for CLI interface."""
    
    # Default settings
    default_config_file: str = "orchestration_config.yaml"
    default_tickers_file: str = "tickers.txt"
    default_output_dir: str = "./output"
    
    # Formatting
    progress_update_interval: float = 2.0  # seconds
    show_progress_bar: bool = True
    verbose: bool = False
    
    # Monitoring
    enable_live_monitoring: bool = True
    monitoring_refresh_rate: float = 1.0  # seconds


class CLIRunner:
    """Command-line interface runner for ETL orchestration."""
    
    def __init__(self, config: Optional[CLIConfig] = None):
        """Initialize CLI runner.
        
        Args:
            config: CLI configuration
        """
        self.config = config or CLIConfig()
        self.orchestration_manager: Optional[OrchestrationManager] = None
        
        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
    
    def run(self, args: Optional[List[str]] = None) -> int:
        """Run the CLI with provided arguments.
        
        Args:
            args: Command line arguments (None = use sys.argv)
            
        Returns:
            Exit code (0 = success, non-zero = error)
        """
        try:
            parser = self._create_parser()
            parsed_args = parser.parse_args(args)
            
            # Update CLI config from args
            if hasattr(parsed_args, 'verbose') and parsed_args.verbose:
                self.config.verbose = True
                logging.getLogger().setLevel(logging.DEBUG)
            
            # Execute command
            return self._execute_command(parsed_args)
            
        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
            return 130
        except Exception as e:
            if self.config.verbose:
                self.logger.exception("CLI error")
            else:
                self.logger.error(f"Error: {e}")
            return 1
        finally:
            if self.orchestration_manager:
                self.orchestration_manager.shutdown()
    
    def _create_parser(self) -> argparse.ArgumentParser:
        """Create argument parser."""
        parser = argparse.ArgumentParser(
            description="ETL Pipeline Orchestration CLI",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Process single ticker
  %(prog)s run --ticker AAPL
  
  # Process multiple tickers from file
  %(prog)s run --tickers-file tickers.txt
  
  # Process tickers with custom configuration
  %(prog)s run --tickers AAPL,MSFT,GOOGL --config custom_config.yaml
  
  # Schedule daily processing
  %(prog)s schedule --tickers-file tickers.txt --schedule daily --time "09:00"
  
  # Monitor running jobs
  %(prog)s monitor --job-id abc123
  
  # Check system health
  %(prog)s health
  
  # View recent job history
  %(prog)s history --limit 10
            """
        )
        
        # Global options
        parser.add_argument(
            '--config', '-c',
            type=str,
            default=self.config.default_config_file,
            help='Configuration file path (YAML or JSON)'
        )
        parser.add_argument(
            '--verbose', '-v',
            action='store_true',
            help='Enable verbose logging'
        )
        parser.add_argument(
            '--output-dir', '-o',
            type=str,
            default=self.config.default_output_dir,
            help='Output directory for results'
        )
        
        # Subcommands
        subparsers = parser.add_subparsers(dest='command', help='Available commands')
        
        # Run command
        self._add_run_parser(subparsers)
        
        # Schedule command
        self._add_schedule_parser(subparsers)
        
        # Monitor command
        self._add_monitor_parser(subparsers)
        
        # Health command
        self._add_health_parser(subparsers)
        
        # History command
        self._add_history_parser(subparsers)
        
        # Cancel command
        self._add_cancel_parser(subparsers)
        
        # Status command
        self._add_status_parser(subparsers)
        
        return parser
    
    def _add_run_parser(self, subparsers) -> None:
        """Add run command parser."""
        run_parser = subparsers.add_parser(
            'run',
            help='Run ETL processing for tickers'
        )
        
        # Ticker specification (mutually exclusive)
        ticker_group = run_parser.add_mutually_exclusive_group(required=True)
        ticker_group.add_argument(
            '--ticker',
            type=str,
            help='Single ticker to process'
        )
        ticker_group.add_argument(
            '--tickers',
            type=str,
            help='Comma-separated list of tickers'
        )
        ticker_group.add_argument(
            '--tickers-file',
            type=str,
            help='File containing list of tickers (one per line)'
        )
        
        # Processing options
        run_parser.add_argument(
            '--job-name',
            type=str,
            help='Name for the orchestration job'
        )
        run_parser.add_argument(
            '--priority',
            choices=['low', 'normal', 'high', 'critical'],
            default='normal',
            help='Job priority'
        )
        run_parser.add_argument(
            '--form-types',
            type=str,
            default='10-K,10-Q',
            help='Comma-separated list of form types'
        )
        run_parser.add_argument(
            '--max-filings',
            type=int,
            default=5,
            help='Maximum filings per ticker'
        )
        run_parser.add_argument(
            '--timeout',
            type=int,
            default=1800,
            help='Processing timeout in seconds'
        )
        run_parser.add_argument(
            '--wait',
            action='store_true',
            help='Wait for job completion'
        )
        run_parser.add_argument(
            '--monitor',
            action='store_true',
            help='Monitor job progress in real-time'
        )
    
    def _add_schedule_parser(self, subparsers) -> None:
        """Add schedule command parser."""
        schedule_parser = subparsers.add_parser(
            'schedule',
            help='Schedule periodic ETL processing'
        )
        
        # Ticker specification
        ticker_group = schedule_parser.add_mutually_exclusive_group(required=True)
        ticker_group.add_argument('--ticker', type=str, help='Single ticker')
        ticker_group.add_argument('--tickers', type=str, help='Comma-separated tickers')
        ticker_group.add_argument('--tickers-file', type=str, help='File with tickers')
        
        # Schedule options
        schedule_parser.add_argument(
            '--schedule',
            choices=['hourly', 'daily', 'weekly', 'monthly'],
            default='daily',
            help='Schedule frequency'
        )
        schedule_parser.add_argument(
            '--time',
            type=str,
            default='09:00',
            help='Time to run (HH:MM format)'
        )
        schedule_parser.add_argument(
            '--interval',
            type=str,
            help='Custom interval (e.g., "30m", "2h", "1d")'
        )
    
    def _add_monitor_parser(self, subparsers) -> None:
        """Add monitor command parser."""
        monitor_parser = subparsers.add_parser(
            'monitor',
            help='Monitor job progress'
        )
        
        monitor_parser.add_argument(
            '--job-id',
            type=str,
            help='Specific job ID to monitor'
        )
        monitor_parser.add_argument(
            '--all',
            action='store_true',
            help='Monitor all active jobs'
        )
        monitor_parser.add_argument(
            '--refresh',
            type=float,
            default=2.0,
            help='Refresh interval in seconds'
        )
    
    def _add_health_parser(self, subparsers) -> None:
        """Add health command parser."""
        health_parser = subparsers.add_parser(
            'health',
            help='Check system health'
        )
        
        health_parser.add_argument(
            '--detailed',
            action='store_true',
            help='Show detailed health information'
        )
        health_parser.add_argument(
            '--json',
            action='store_true',
            help='Output in JSON format'
        )
    
    def _add_history_parser(self, subparsers) -> None:
        """Add history command parser."""
        history_parser = subparsers.add_parser(
            'history',
            help='View job history'
        )
        
        history_parser.add_argument(
            '--limit',
            type=int,
            default=20,
            help='Number of recent jobs to show'
        )
        history_parser.add_argument(
            '--status',
            choices=['pending', 'running', 'completed', 'failed', 'cancelled'],
            help='Filter by job status'
        )
        history_parser.add_argument(
            '--since',
            type=str,
            help='Show jobs since date (YYYY-MM-DD)'
        )
        history_parser.add_argument(
            '--json',
            action='store_true',
            help='Output in JSON format'
        )
    
    def _add_cancel_parser(self, subparsers) -> None:
        """Add cancel command parser."""
        cancel_parser = subparsers.add_parser(
            'cancel',
            help='Cancel running job'
        )
        
        cancel_parser.add_argument(
            'job_id',
            type=str,
            help='Job ID to cancel'
        )
    
    def _add_status_parser(self, subparsers) -> None:
        """Add status command parser."""
        status_parser = subparsers.add_parser(
            'status',
            help='Get job status'
        )
        
        status_parser.add_argument(
            'job_id',
            type=str,
            help='Job ID to check'
        )
        status_parser.add_argument(
            '--json',
            action='store_true',
            help='Output in JSON format'
        )
    
    def _execute_command(self, args) -> int:
        """Execute the specified command.
        
        Args:
            args: Parsed arguments
            
        Returns:
            Exit code
        """
        if not args.command:
            print("Error: No command specified. Use --help for usage information.")
            return 1
        
        # Initialize orchestration manager
        orchestration_config = self._load_orchestration_config(args.config)
        self.orchestration_manager = OrchestrationManager(orchestration_config)
        
        # Execute command
        command_handlers = {
            'run': self._handle_run,
            'schedule': self._handle_schedule,
            'monitor': self._handle_monitor,
            'health': self._handle_health,
            'history': self._handle_history,
            'cancel': self._handle_cancel,
            'status': self._handle_status
        }
        
        handler = command_handlers.get(args.command)
        if handler:
            return handler(args)
        else:
            print(f"Error: Unknown command '{args.command}'")
            return 1
    
    def _handle_run(self, args) -> int:
        """Handle run command."""
        try:
            # Parse tickers
            tickers = self._parse_tickers(args)
            if not tickers:
                print("Error: No tickers specified")
                return 1
            
            # Create ticker configurations
            ticker_configs = []
            form_types = [ft.strip() for ft in args.form_types.split(',')]
            priority = Priority[args.priority.upper()]
            
            for ticker in tickers:
                config = TickerConfig(
                    ticker=ticker.upper(),
                    form_types=form_types,
                    priority=priority,
                    max_filings=args.max_filings
                )
                ticker_configs.append(config)
            
            # Submit orchestration job
            job_id = self.orchestration_manager.process_tickers(
                tickers=ticker_configs,
                job_name=args.job_name
            )
            
            print(f"Started orchestration job: {job_id}")
            print(f"Processing {len(ticker_configs)} tickers: {[tc.ticker for tc in ticker_configs]}")
            
            # Monitor or wait if requested
            if args.monitor:
                return self._monitor_job(job_id, args.timeout)
            elif args.wait:
                return self._wait_for_job(job_id, args.timeout)
            else:
                print(f"Job submitted. Use 'gpt-trader monitor --job-id {job_id}' to track progress.")
                return 0
            
        except Exception as e:
            print(f"Error running job: {e}")
            return 1
    
    def _handle_schedule(self, args) -> int:
        """Handle schedule command."""
        try:
            # Parse tickers
            tickers = self._parse_tickers(args)
            if not tickers:
                print("Error: No tickers specified")
                return 1
            
            # Create ticker configurations
            ticker_configs = [TickerConfig(ticker=ticker.upper()) for ticker in tickers]
            
            # Determine schedule type and expression
            if args.interval:
                schedule_type = ScheduleType.INTERVAL
                schedule_expression = args.interval
            else:
                schedule_type = ScheduleType.CRON
                # Convert simple schedule to cron-like expression
                schedule_mapping = {
                    'hourly': '0 * * * *',
                    'daily': f'0 {args.time.split(":")[0]} * * *',
                    'weekly': f'0 {args.time.split(":")[0]} * * 0',
                    'monthly': f'0 {args.time.split(":")[0]} 1 * *'
                }
                schedule_expression = schedule_mapping.get(args.schedule, f'0 {args.time.split(":")[0]} * * *')
            
            # Submit scheduled job
            job_id = self.orchestration_manager.process_tickers(
                tickers=ticker_configs,
                job_name=f"Scheduled_{args.schedule}_{datetime.now().strftime('%Y%m%d')}",
                schedule_type=schedule_type,
                schedule_expression=schedule_expression
            )
            
            print(f"Scheduled job created: {job_id}")
            print(f"Schedule: {args.schedule} at {args.time}")
            print(f"Tickers: {[tc.ticker for tc in ticker_configs]}")
            
            return 0
            
        except Exception as e:
            print(f"Error scheduling job: {e}")
            return 1
    
    def _handle_monitor(self, args) -> int:
        """Handle monitor command."""
        if args.job_id:
            return self._monitor_job(args.job_id, refresh_interval=args.refresh)
        elif args.all:
            return self._monitor_all_jobs(refresh_interval=args.refresh)
        else:
            print("Error: Specify --job-id or --all")
            return 1
    
    def _handle_health(self, args) -> int:
        """Handle health command."""
        try:
            health = self.orchestration_manager.get_system_health()
            
            if args.json:
                print(json.dumps(health, indent=2))
            else:
                self._print_health_status(health, detailed=args.detailed)
            
            # Return appropriate exit code based on health
            if health['status'] == 'healthy':
                return 0
            elif health['status'] == 'degraded':
                return 2
            else:
                return 3
            
        except Exception as e:
            print(f"Error checking health: {e}")
            return 1
    
    def _handle_history(self, args) -> int:
        """Handle history command."""
        try:
            # For now, show scheduler statistics
            # In a real implementation, would query job history from database
            stats = self.orchestration_manager.job_scheduler.get_job_statistics()
            
            if args.json:
                print(json.dumps(stats, indent=2))
            else:
                print("Job Statistics:")
                print(f"  Total jobs: {stats['total_jobs']}")
                print(f"  Running jobs: {stats['running_jobs']}")
                print(f"  Queued jobs: {stats['queue_size']}")
                print(f"  Worker utilization: {stats['worker_utilization']:.1%}")
                
                if 'status_counts' in stats:
                    print("\nStatus breakdown:")
                    for status, count in stats['status_counts'].items():
                        print(f"  {status}: {count}")
            
            return 0
            
        except Exception as e:
            print(f"Error getting history: {e}")
            return 1
    
    def _handle_cancel(self, args) -> int:
        """Handle cancel command."""
        try:
            success = self.orchestration_manager.cancel_orchestration(args.job_id)
            
            if success:
                print(f"Job {args.job_id} cancelled successfully")
                return 0
            else:
                print(f"Failed to cancel job {args.job_id}")
                return 1
                
        except Exception as e:
            print(f"Error cancelling job: {e}")
            return 1
    
    def _handle_status(self, args) -> int:
        """Handle status command."""
        try:
            status = self.orchestration_manager.get_orchestration_status(args.job_id)
            
            if args.json:
                print(json.dumps(status, indent=2))
            else:
                self._print_job_status(status)
            
            return 0
            
        except Exception as e:
            print(f"Error getting status: {e}")
            return 1
    
    def _parse_tickers(self, args) -> List[str]:
        """Parse tickers from command line arguments."""
        if args.ticker:
            return [args.ticker]
        elif args.tickers:
            return [t.strip() for t in args.tickers.split(',') if t.strip()]
        elif args.tickers_file:
            try:
                with open(args.tickers_file, 'r') as f:
                    return [line.strip() for line in f if line.strip() and not line.startswith('#')]
            except FileNotFoundError:
                raise ValueError(f"Tickers file not found: {args.tickers_file}")
        else:
            return []
    
    def _load_orchestration_config(self, config_path: str) -> OrchestrationConfig:
        """Load orchestration configuration from file."""
        config_file = Path(config_path)
        
        if not config_file.exists():
            self.logger.info(f"Config file {config_path} not found, using defaults")
            return OrchestrationConfig()
        
        try:
            with open(config_file, 'r') as f:
                if config_file.suffix.lower() == '.yaml' or config_file.suffix.lower() == '.yml':
                    config_data = yaml.safe_load(f)
                else:
                    config_data = json.load(f)
            
            # Convert config data to OrchestrationConfig
            # This would need proper mapping implementation
            return OrchestrationConfig(**config_data)
            
        except Exception as e:
            self.logger.warning(f"Failed to load config from {config_path}: {e}")
            return OrchestrationConfig()
    
    def _monitor_job(self, job_id: str, timeout: Optional[int] = None, refresh_interval: float = 2.0) -> int:
        """Monitor a specific job."""
        print(f"Monitoring job: {job_id}")
        print("Press Ctrl+C to stop monitoring\n")
        
        start_time = time.time()
        
        try:
            while True:
                status = self.orchestration_manager.get_orchestration_status(job_id)
                
                # Clear screen and show status
                print("\033[2J\033[H")  # Clear screen and move cursor to top
                print(f"Job Monitoring - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 60)
                self._print_job_status(status, detailed=True)
                
                # Check if job is complete
                job_status = status.get('job_status', '').lower()
                if job_status in ['completed', 'failed', 'cancelled']:
                    print(f"\nJob finished with status: {job_status}")
                    return 0 if job_status == 'completed' else 1
                
                # Check timeout
                if timeout and (time.time() - start_time) > timeout:
                    print(f"\nMonitoring timeout after {timeout} seconds")
                    return 1
                
                time.sleep(refresh_interval)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
            return 0
    
    def _monitor_all_jobs(self, refresh_interval: float = 2.0) -> int:
        """Monitor all active jobs."""
        print("Monitoring all jobs")
        print("Press Ctrl+C to stop monitoring\n")
        
        try:
            while True:
                health = self.orchestration_manager.get_system_health()
                
                # Clear screen and show status
                print("\033[2J\033[H")
                print(f"System Monitoring - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 60)
                self._print_health_status(health, detailed=True)
                
                time.sleep(refresh_interval)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
            return 0
    
    def _wait_for_job(self, job_id: str, timeout: Optional[int] = None) -> int:
        """Wait for job completion."""
        print(f"Waiting for job {job_id} to complete...")
        
        start_time = time.time()
        
        while True:
            status = self.orchestration_manager.get_orchestration_status(job_id)
            job_status = status.get('job_status', '').lower()
            
            if job_status in ['completed', 'failed', 'cancelled']:
                print(f"Job finished with status: {job_status}")
                self._print_job_status(status)
                return 0 if job_status == 'completed' else 1
            
            if timeout and (time.time() - start_time) > timeout:
                print(f"Timeout after {timeout} seconds")
                return 1
            
            time.sleep(2.0)
    
    def _print_job_status(self, status: Dict[str, Any], detailed: bool = False) -> None:
        """Print formatted job status."""
        print(f"Job ID: {status.get('orchestration_id', 'Unknown')}")
        print(f"Status: {status.get('job_status', 'Unknown')}")
        
        result = status.get('result', {})
        if result.get('start_time'):
            print(f"Started: {result['start_time']}")
        if result.get('end_time'):
            print(f"Ended: {result['end_time']}")
        if result.get('duration'):
            print(f"Duration: {result['duration']:.2f} seconds")
        
        # Progress information
        progress = status.get('progress', {})
        if progress:
            print(f"\nProgress:")
            print(f"  Overall: {progress.get('overall_percentage', 0):.1f}%")
            print(f"  Completed: {progress.get('completed_tickers', 0)}")
            print(f"  Failed: {progress.get('failed_tickers', 0)}")
            print(f"  Total: {progress.get('total_tickers', 0)}")
            
            if progress.get('throughput'):
                print(f"  Throughput: {progress['throughput']:.2f} tickers/min")
        
        # Performance information
        if detailed:
            performance = status.get('performance', {})
            if performance:
                print(f"\nPerformance:")
                for key, value in performance.items():
                    print(f"  {key}: {value}")
        
        # Error information
        if result.get('error'):
            print(f"\nError: {result['error']}")
    
    def _print_health_status(self, health: Dict[str, Any], detailed: bool = False) -> None:
        """Print formatted health status."""
        print(f"Overall Status: {health.get('status', 'unknown').upper()}")
        print(f"Health Score: {health.get('overall_score', 0):.1f}/100")
        
        # Scheduler status
        scheduler = health.get('scheduler', {})
        print(f"\nScheduler:")
        print(f"  Running jobs: {scheduler.get('running_jobs', 0)}")
        print(f"  Queue size: {scheduler.get('queue_size', 0)}")
        print(f"  Worker utilization: {scheduler.get('worker_utilization', 0):.1%}")
        
        # Resource manager status
        resources = health.get('resource_manager', {})
        print(f"\nResources:")
        print(f"  Active requests: {resources.get('active_requests', 0)}")
        print(f"  Memory usage: {resources.get('memory_usage_mb', 0):.1f} MB")
        print(f"  Rate limited: {'Yes' if resources.get('rate_limit_exceeded', False) else 'No'}")
        
        if detailed:
            # Performance metrics
            performance = health.get('performance', {})
            if performance:
                print(f"\nPerformance:")
                for key, value in performance.items():
                    print(f"  {key}: {value}")
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        log_level = logging.DEBUG if self.config.verbose else logging.INFO
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )


def main():
    """Main entry point for CLI."""
    cli = CLIRunner()
    return cli.run()


if __name__ == '__main__':
    exit(main())