"""
Database-enabled filing processor for GPT Trader platform.

This module extends the EDGAR filing processor with database persistence
and multi-ticker processing capabilities.
"""

import logging
import hashlib
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from edgar.filing_processor import FilingProcessor as BaseFilingProcessor
from edgar.filing_processor import AsyncFilingProcessor as BaseAsyncFilingProcessor
from edgar.client_new import EdgarClient, ClientConfig
from edgar.s3_manager import S3Manager

from .models import Company, Filing, Document, ProcessingJob, ProcessingStatus, FilingType
from .database import get_session, session_scope
from .config import GPTTraderConfig, TickerConfig

logger = logging.getLogger(__name__)


class FilingProcessorDB(BaseFilingProcessor):
    """Enhanced filing processor with database persistence."""
    
    def __init__(self, edgar_client: EdgarClient, s3_manager: S3Manager = None, 
                 config: GPTTraderConfig = None):
        super().__init__(edgar_client, s3_manager)
        self.config = config
        self._performance_metrics = {}
    
    def sync_company_from_ticker_config(self, ticker_config: TickerConfig, 
                                       session: Session = None) -> Company:
        """Sync company information from ticker configuration to database."""
        if session is None:
            with session_scope() as session:
                return self._sync_company_internal(ticker_config, session)
        else:
            return self._sync_company_internal(ticker_config, session)
    
    def _sync_company_internal(self, ticker_config: TickerConfig, session: Session) -> Company:
        """Internal method to sync company information."""
        # Check if company exists
        company = Company.get_by_cik(session, ticker_config.cik)
        
        if company is None:
            # Create new company
            company = Company(
                cik=ticker_config.cik.zfill(10),
                ticker=ticker_config.ticker.upper(),
                name=ticker_config.ticker.upper(),  # Will be updated from SEC data
                is_active=ticker_config.is_active
            )
            session.add(company)
            logger.info(f"Created new company record: {ticker_config.ticker} (CIK: {ticker_config.cik})")
        else:
            # Update existing company
            if company.ticker != ticker_config.ticker.upper():
                company.ticker = ticker_config.ticker.upper()
            company.is_active = ticker_config.is_active
            logger.debug(f"Updated company record: {ticker_config.ticker} (CIK: {ticker_config.cik})")
        
        session.commit()
        return company
    
    def fetch_and_store_filings(self, ticker_config: TickerConfig, 
                               max_filings: int = None) -> Tuple[int, int]:
        """Fetch filings for a ticker and store in database."""
        start_time = datetime.utcnow()
        
        try:
            with session_scope() as session:
                # Sync company information
                company = self.sync_company_from_ticker_config(ticker_config, session)
                
                # Fetch recent filings from SEC
                filings = self.get_recent_filings(company.cik)
                
                new_filings = 0
                updated_filings = 0
                
                for filing_data in filings[:max_filings] if max_filings else filings:
                    # Filter by form types if specified
                    if (ticker_config.forms_to_process and 
                        filing_data.get('form') not in ticker_config.forms_to_process):
                        continue
                    
                    # Check if filing already exists
                    accession_number = filing_data.get('accessionNumber')
                    existing_filing = Filing.get_by_accession(session, accession_number)
                    
                    if existing_filing is None:
                        # Create new filing record
                        filing = self._create_filing_record(company, filing_data, session)
                        new_filings += 1
                    else:
                        # Update existing filing if needed
                        updated = self._update_filing_record(existing_filing, filing_data, session)
                        if updated:
                            updated_filings += 1
                
                # Update company last processed time
                company.last_processed = datetime.utcnow()
                session.commit()
                
                processing_time = (datetime.utcnow() - start_time).total_seconds()
                self._update_performance_metrics('fetch_filings', processing_time)
                
                logger.info(f"Processed {ticker_config.ticker}: {new_filings} new, "
                           f"{updated_filings} updated filings")
                
                return new_filings, updated_filings
                
        except Exception as e:
            logger.error(f"Failed to fetch and store filings for {ticker_config.ticker}: {e}")
            raise
    
    def _create_filing_record(self, company: Company, filing_data: Dict[str, Any], 
                             session: Session) -> Filing:
        """Create a new filing record in database."""
        filing = Filing(
            company_id=company.id,
            accession_number=filing_data.get('accessionNumber'),
            form_type=filing_data.get('form'),
            filing_date=self._parse_date(filing_data.get('filingDate')),
            period_of_report=self._parse_date(filing_data.get('periodOfReport')),
            primary_document=filing_data.get('primaryDocument'),
            primary_doc_url=filing_data.get('primaryDocumentUrl'),
            filing_url=filing_data.get('filingUrl'),
            interactive_data_url=filing_data.get('interactiveDataUrl'),
            file_size=filing_data.get('size'),
            file_count=filing_data.get('fileCount')
        )
        
        session.add(filing)
        session.commit()
        
        # Create document records if available
        if filing_data.get('documents'):
            self._create_document_records(filing, filing_data['documents'], session)
        
        return filing
    
    def _update_filing_record(self, filing: Filing, filing_data: Dict[str, Any], 
                             session: Session) -> bool:
        """Update existing filing record if needed."""
        updated = False
        
        # Update URLs if they've changed
        new_primary_url = filing_data.get('primaryDocumentUrl')
        if new_primary_url and filing.primary_doc_url != new_primary_url:
            filing.primary_doc_url = new_primary_url
            updated = True
        
        new_filing_url = filing_data.get('filingUrl')
        if new_filing_url and filing.filing_url != new_filing_url:
            filing.filing_url = new_filing_url
            updated = True
        
        # Update file metadata
        new_size = filing_data.get('size')
        if new_size and filing.file_size != new_size:
            filing.file_size = new_size
            updated = True
        
        if updated:
            session.commit()
        
        return updated
    
    def _create_document_records(self, filing: Filing, documents_data: List[Dict], 
                                session: Session) -> None:
        """Create document records for a filing."""
        for doc_data in documents_data:
            document = Document(
                filing_id=filing.id,
                sequence=doc_data.get('sequence'),
                description=doc_data.get('description'),
                document_name=doc_data.get('document'),
                document_type=doc_data.get('type'),
                file_size=self._parse_size(doc_data.get('size'))
            )
            session.add(document)
        
        session.commit()
    
    def process_pending_filings(self, limit: int = None) -> Dict[str, int]:
        """Process filings that are pending download/processing."""
        start_time = datetime.utcnow()
        stats = {
            'processed': 0,
            'failed': 0,
            'skipped': 0
        }
        
        try:
            with session_scope() as session:
                # Get pending filings
                pending_filings = Filing.get_pending_filings(session, limit)
                
                for filing in pending_filings:
                    try:
                        # Mark as processing started
                        filing.mark_processing_started(session)
                        
                        # Process the filing
                        success = self._process_single_filing(filing, session)
                        
                        if success:
                            filing.mark_processing_completed(session)
                            stats['processed'] += 1
                        else:
                            filing.mark_processing_failed(session, "Processing failed")
                            stats['failed'] += 1
                            
                    except Exception as e:
                        logger.error(f"Failed to process filing {filing.accession_number}: {e}")
                        filing.mark_processing_failed(session, str(e))
                        stats['failed'] += 1
                
                processing_time = (datetime.utcnow() - start_time).total_seconds()
                self._update_performance_metrics('process_filings', processing_time)
                
                logger.info(f"Processing completed: {stats}")
                return stats
                
        except Exception as e:
            logger.error(f"Failed to process pending filings: {e}")
            raise
    
    def _process_single_filing(self, filing: Filing, session: Session) -> bool:
        """Process a single filing (download documents, upload to S3)."""
        try:
            # Get filing documents
            documents = self.get_filing_documents(
                filing.company.cik, 
                filing.accession_number
            )
            
            success_count = 0
            total_documents = len(documents)
            
            for doc_info in documents:
                try:
                    # Download document
                    content = self.download_document(
                        filing.company.cik,
                        filing.accession_number, 
                        doc_info['document']
                    )
                    
                    # Calculate content hash
                    content_hash = hashlib.sha256(content).hexdigest()
                    
                    # Upload to S3 if configured
                    if self.s3_manager and self.config and self.config.s3.bucket_name:
                        s3_key = f"{self.config.s3.key_prefix}/{filing.company.cik}/{filing.accession_number}/{doc_info['document']}"
                        
                        self.s3_manager.upload_bytes(
                            content, 
                            self.config.s3.bucket_name,
                            s3_key
                        )
                        
                        # Update document record
                        document = session.query(Document).filter(
                            Document.filing_id == filing.id,
                            Document.document_name == doc_info['document']
                        ).first()
                        
                        if document:
                            document.mark_downloaded(
                                session, 
                                self.config.s3.bucket_name,
                                s3_key,
                                content_hash
                            )
                    
                    success_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to process document {doc_info['document']}: {e}")
                    continue
            
            # Consider successful if majority of documents processed
            return success_count > (total_documents * 0.5) if total_documents > 0 else True
            
        except Exception as e:
            logger.error(f"Failed to process filing {filing.accession_number}: {e}")
            return False
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get processing statistics and performance metrics."""
        try:
            with session_scope() as session:
                stats = {}
                
                # Company statistics
                total_companies = session.query(Company).count()
                active_companies = session.query(Company).filter(Company.is_active == True).count()
                stats['companies'] = {
                    'total': total_companies,
                    'active': active_companies
                }
                
                # Filing statistics
                total_filings = session.query(Filing).count()
                processed_filings = session.query(Filing).filter(Filing.is_processed == True).count()
                pending_filings = session.query(Filing).filter(
                    Filing.is_processed == False,
                    Filing.processing_status.in_([ProcessingStatus.PENDING, ProcessingStatus.FAILED])
                ).count()
                
                stats['filings'] = {
                    'total': total_filings,
                    'processed': processed_filings,
                    'pending': pending_filings,
                    'processing_rate': (processed_filings / total_filings * 100) if total_filings > 0 else 0
                }
                
                # Recent activity (last 24 hours)
                recent_cutoff = datetime.utcnow() - timedelta(days=1)
                recent_filings = session.query(Filing).filter(
                    Filing.created_at >= recent_cutoff
                ).count()
                recent_processed = session.query(Filing).filter(
                    Filing.processed_at >= recent_cutoff
                ).count()
                
                stats['recent_activity'] = {
                    'new_filings_24h': recent_filings,
                    'processed_filings_24h': recent_processed
                }
                
                # Performance metrics
                stats['performance'] = self._performance_metrics.copy()
                
                return stats
                
        except Exception as e:
            logger.error(f"Failed to get processing statistics: {e}")
            return {}
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            try:
                return datetime.strptime(date_str, '%m/%d/%Y')
            except ValueError:
                logger.warning(f"Unable to parse date: {date_str}")
                return None
    
    def _parse_size(self, size_str: str) -> Optional[int]:
        """Parse size string to integer bytes."""
        if not size_str:
            return None
        try:
            # Handle different size formats (e.g., "1.2 MB", "500 KB")
            size_str = size_str.strip().replace(',', '')
            if 'MB' in size_str.upper():
                return int(float(size_str.split()[0]) * 1024 * 1024)
            elif 'KB' in size_str.upper():
                return int(float(size_str.split()[0]) * 1024)
            else:
                return int(size_str)
        except (ValueError, IndexError):
            logger.warning(f"Unable to parse size: {size_str}")
            return None
    
    def _update_performance_metrics(self, operation: str, duration: float) -> None:
        """Update performance metrics for monitoring."""
        if operation not in self._performance_metrics:
            self._performance_metrics[operation] = {
                'count': 0,
                'total_time': 0.0,
                'avg_time': 0.0,
                'min_time': float('inf'),
                'max_time': 0.0
            }
        
        metrics = self._performance_metrics[operation]
        metrics['count'] += 1
        metrics['total_time'] += duration
        metrics['avg_time'] = metrics['total_time'] / metrics['count']
        metrics['min_time'] = min(metrics['min_time'], duration)
        metrics['max_time'] = max(metrics['max_time'], duration)


class BatchFilingProcessor:
    """Batch processor for multiple tickers with performance monitoring."""
    
    def __init__(self, config: GPTTraderConfig):
        self.config = config
        self.edgar_client = self._create_edgar_client()
        self.s3_manager = self._create_s3_manager() if config.s3.bucket_name else None
        self.processor = FilingProcessorDB(self.edgar_client, self.s3_manager, config)
        
    def _create_edgar_client(self) -> EdgarClient:
        """Create EDGAR client with configuration."""
        client_config = ClientConfig(
            rate_limit_per_sec=self.config.edgar.rate_limit_per_sec,
            user_agent=self.config.edgar.user_agent,
            num_workers=self.config.edgar.num_workers
        )
        return EdgarClient(client_config)
    
    def _create_s3_manager(self) -> S3Manager:
        """Create S3 manager with configuration."""
        return S3Manager(
            region=self.config.s3.region,
            logger=logger
        )
    
    def process_all_active_tickers(self, job_name: str = None) -> ProcessingJob:
        """Process all active tickers in batch."""
        job_name = job_name or f"batch_process_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        with session_scope() as session:
            # Create processing job
            active_tickers = self.config.get_active_tickers()
            ticker_symbols = [t.ticker for t in active_tickers]
            
            job = ProcessingJob.create_job(
                session=session,
                job_type="batch_etl",
                job_name=job_name,
                ticker_list=ticker_symbols,
                parameters={
                    'batch_size': self.config.etl.batch_size,
                    'max_concurrent': self.config.etl.max_concurrent_jobs
                }
            )
            
            try:
                # Start job
                job.start_job(session, total_items=len(active_tickers))
                
                processed_count = 0
                failed_count = 0
                
                # Process each ticker
                for ticker_config in active_tickers:
                    try:
                        logger.info(f"Processing ticker: {ticker_config.ticker}")
                        
                        new_filings, updated_filings = self.processor.fetch_and_store_filings(
                            ticker_config, 
                            max_filings=self.config.etl.batch_size
                        )
                        
                        processed_count += 1
                        job.update_progress(session, processed_items=processed_count)
                        
                        logger.info(f"Completed {ticker_config.ticker}: "
                                   f"{new_filings} new, {updated_filings} updated")
                        
                    except Exception as e:
                        logger.error(f"Failed to process {ticker_config.ticker}: {e}")
                        failed_count += 1
                        job.update_progress(session, 
                                          processed_items=processed_count,
                                          failed_items=failed_count)
                
                # Complete job
                if failed_count == 0:
                    job.complete_job(session)
                    logger.info(f"Batch processing completed successfully: {job_name}")
                else:
                    job.fail_job(session, f"Failed to process {failed_count} tickers")
                    logger.warning(f"Batch processing completed with {failed_count} failures")
                
                return job
                
            except Exception as e:
                job.fail_job(session, str(e))
                logger.error(f"Batch processing failed: {e}")
                raise
    
    def process_priority_tickers(self, min_priority: int = 1) -> ProcessingJob:
        """Process high-priority tickers only."""
        high_priority_tickers = [
            t for t in self.config.get_active_tickers() 
            if t.priority >= min_priority
        ]
        
        # Temporarily update config with high-priority tickers
        original_tickers = self.config.tickers
        self.config.tickers = high_priority_tickers
        
        try:
            job_name = f"priority_process_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            return self.process_all_active_tickers(job_name)
        finally:
            # Restore original ticker list
            self.config.tickers = original_tickers