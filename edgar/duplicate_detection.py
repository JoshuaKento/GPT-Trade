"""Duplicate detection and incremental processing for EDGAR filing processing."""

import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple

from .database_models import DatabaseManager, ProcessingStatus


class DuplicateDetector:
    """Handles duplicate detection and incremental processing logic."""
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize duplicate detector.
        
        Args:
            db_manager: Database manager for state tracking
            logger: Optional logger instance
        """
        self.db_manager = db_manager
        self.logger = logger or logging.getLogger(__name__)
    
    def get_processed_accessions(
        self,
        cik: str,
        form_types: Optional[List[str]] = None,
        since_date: Optional[str] = None,
    ) -> Set[str]:
        """Get set of processed accession numbers for a CIK with filtering.
        
        Args:
            cik: Company CIK
            form_types: Optional list of form types to filter
            since_date: Optional date to filter filings since (YYYY-MM-DD)
            
        Returns:
            Set of processed accession numbers
        """
        try:
            query = """
                SELECT accession FROM filings 
                WHERE cik = ? AND status IN ('completed', 'skipped')
            """
            params = [cik]
            
            if form_types:
                placeholders = ','.join('?' * len(form_types))
                query += f" AND form_type IN ({placeholders})"
                params.extend(form_types)
            
            if since_date:
                query += " AND filing_date >= ?"
                params.append(since_date)
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute(query, params)
                return {row[0] for row in cursor.fetchall()}
                
        except Exception as e:
            self.logger.error(f"Failed to get processed accessions for {cik}: {e}")
            return set()
    
    def check_filing_exists(self, accession: str) -> bool:
        """Check if a filing already exists in the database.
        
        Args:
            accession: Filing accession number
            
        Returns:
            True if filing exists, False otherwise
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM filings WHERE accession = ? LIMIT 1",
                    (accession,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Failed to check filing existence {accession}: {e}")
            return False
    
    def get_filing_status(self, accession: str) -> Optional[ProcessingStatus]:
        """Get the processing status of a filing.
        
        Args:
            accession: Filing accession number
            
        Returns:
            Processing status or None if not found
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT status FROM filings WHERE accession = ?",
                    (accession,)
                )
                row = cursor.fetchone()
                if row:
                    return ProcessingStatus(row[0])
                return None
        except Exception as e:
            self.logger.error(f"Failed to get filing status {accession}: {e}")
            return None
    
    def mark_filing_skipped(self, accession: str, reason: str) -> bool:
        """Mark a filing as skipped with a reason.
        
        Args:
            accession: Filing accession number
            reason: Reason for skipping
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db_manager.get_connection() as conn:
                conn.execute("""
                    UPDATE filings 
                    SET status = 'skipped', 
                        error_message = ?,
                        updated_at = ?
                    WHERE accession = ?
                """, (reason, datetime.now(timezone.utc).isoformat(), accession))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Failed to mark filing {accession} as skipped: {e}")
            return False
    
    def get_incremental_filings(
        self,
        cik: str,
        all_filings: List[Dict],
        form_types: Optional[List[str]] = None,
        force_reprocess: bool = False,
    ) -> Tuple[List[Dict], List[Dict]]:
        """Get new and skipped filings for incremental processing.
        
        Args:
            cik: Company CIK
            all_filings: All available filings for the CIK
            form_types: Optional list of form types to filter
            force_reprocess: Whether to reprocess failed filings
            
        Returns:
            Tuple of (new_filings, skipped_filings)
        """
        try:
            # Get processed accessions
            processed_accessions = self.get_processed_accessions(cik, form_types)
            
            new_filings = []
            skipped_filings = []
            
            for filing in all_filings:
                accession = filing.get("accession")
                if not accession:
                    continue
                
                # Filter by form type if specified
                if form_types:
                    form_type = filing.get("form", "").upper()
                    if form_type not in [ft.upper() for ft in form_types]:
                        continue
                
                # Check if already processed
                if accession in processed_accessions:
                    if force_reprocess:
                        # Check if it was a failure that should be retried
                        status = self.get_filing_status(accession)
                        if status == ProcessingStatus.FAILED:
                            new_filings.append(filing)
                        else:
                            skipped_filings.append({
                                **filing,
                                "skip_reason": f"Already processed with status: {status.value}"
                            })
                    else:
                        skipped_filings.append({
                            **filing,
                            "skip_reason": "Already processed"
                        })
                else:
                    new_filings.append(filing)
            
            self.logger.info(
                f"CIK {cik}: {len(new_filings)} new, {len(skipped_filings)} skipped filings"
            )
            
            return new_filings, skipped_filings
            
        except Exception as e:
            self.logger.error(f"Failed to get incremental filings for {cik}: {e}")
            return [], []
    
    def detect_content_duplicates(
        self,
        cik: str,
        accession: str,
        document_content: bytes,
    ) -> Optional[str]:
        """Detect if document content is a duplicate of existing content.
        
        Args:
            cik: Company CIK
            accession: Filing accession number
            document_content: Document content bytes
            
        Returns:
            Accession number of duplicate filing or None if unique
        """
        try:
            # Calculate content hash
            content_hash = hashlib.sha256(document_content).hexdigest()
            
            # Check for existing documents with same hash
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT f.accession 
                    FROM filings f
                    JOIN documents d ON f.id = d.filing_id
                    WHERE f.cik = ? 
                    AND f.accession != ?
                    AND d.content_hash = ?
                    LIMIT 1
                """, (cik, accession, content_hash))
                
                row = cursor.fetchone()
                if row:
                    return row[0]
                
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to detect content duplicates for {accession}: {e}")
            return None
    
    def store_document_hash(
        self,
        filing_id: int,
        document_name: str,
        content_hash: str,
    ) -> bool:
        """Store document content hash for future duplicate detection.
        
        Args:
            filing_id: Filing ID
            document_name: Document name
            content_hash: SHA256 hash of document content
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db_manager.get_connection() as conn:
                # Add content_hash column if it doesn't exist
                try:
                    conn.execute("ALTER TABLE documents ADD COLUMN content_hash TEXT")
                    conn.commit()
                except Exception:
                    pass  # Column already exists
                
                # Update document with hash
                conn.execute("""
                    UPDATE documents 
                    SET content_hash = ?
                    WHERE filing_id = ? AND document_name = ?
                """, (content_hash, filing_id, document_name))
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to store document hash: {e}")
            return False
    
    def cleanup_stale_processing(self, hours_old: int = 24) -> int:
        """Clean up filings stuck in processing state.
        
        Args:
            hours_old: Mark filings as failed if in processing for this many hours
            
        Returns:
            Number of filings cleaned up
        """
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_old)
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute("""
                    UPDATE filings 
                    SET status = 'failed',
                        error_message = 'Processing timeout - marked as stale',
                        updated_at = ?
                    WHERE status = 'in_progress' 
                    AND updated_at < ?
                """, (datetime.now(timezone.utc).isoformat(), cutoff_time.isoformat()))
                
                conn.commit()
                cleaned_count = cursor.rowcount
                
                if cleaned_count > 0:
                    self.logger.info(f"Cleaned up {cleaned_count} stale processing records")
                
                return cleaned_count
                
        except Exception as e:
            self.logger.error(f"Failed to cleanup stale processing records: {e}")
            return 0
    
    def get_processing_statistics(self, cik: Optional[str] = None) -> Dict[str, int]:
        """Get processing statistics for a CIK or overall.
        
        Args:
            cik: Optional CIK to filter by
            
        Returns:
            Dictionary with processing statistics
        """
        try:
            query = """
                SELECT status, COUNT(*) as count
                FROM filings
            """
            params = []
            
            if cik:
                query += " WHERE cik = ?"
                params.append(cik)
            
            query += " GROUP BY status"
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute(query, params)
                
                stats = {
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                    "pending": 0,
                    "in_progress": 0,
                    "skipped": 0,
                    "retrying": 0,
                }
                
                for row in cursor.fetchall():
                    status, count = row
                    stats[status] = count
                    stats["total"] += count
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Failed to get processing statistics: {e}")
            return {}
    
    def get_duplicate_summary(self, cik: Optional[str] = None) -> Dict[str, int]:
        """Get summary of duplicate detection results.
        
        Args:
            cik: Optional CIK to filter by
            
        Returns:
            Dictionary with duplicate detection statistics
        """
        try:
            base_query = """
                SELECT 
                    COUNT(*) as total_documents,
                    COUNT(DISTINCT content_hash) as unique_content,
                    COUNT(*) - COUNT(DISTINCT content_hash) as duplicates
                FROM documents d
                JOIN filings f ON d.filing_id = f.id
            """
            
            params = []
            if cik:
                base_query += " WHERE f.cik = ?"
                params.append(cik)
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute(base_query, params)
                row = cursor.fetchone()
                
                if row:
                    total_documents, unique_content, duplicates = row
                    duplicate_rate = (duplicates / total_documents * 100) if total_documents > 0 else 0
                    
                    return {
                        "total_documents": total_documents,
                        "unique_content": unique_content,
                        "duplicates": duplicates,
                        "duplicate_rate_percent": duplicate_rate,
                    }
                
                return {
                    "total_documents": 0,
                    "unique_content": 0,
                    "duplicates": 0,
                    "duplicate_rate_percent": 0.0,
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get duplicate summary: {e}")
            return {}
    
    def find_potential_duplicates(
        self,
        similarity_threshold: float = 0.95,
        limit: int = 100,
    ) -> List[Dict[str, any]]:
        """Find potential duplicate filings based on various criteria.
        
        Args:
            similarity_threshold: Threshold for considering filings similar
            limit: Maximum number of duplicate groups to return
            
        Returns:
            List of potential duplicate groups
        """
        try:
            with self.db_manager.get_connection() as conn:
                # Find filings with identical content hashes
                cursor = conn.execute("""
                    SELECT 
                        d.content_hash,
                        GROUP_CONCAT(f.accession) as accessions,
                        COUNT(*) as duplicate_count
                    FROM documents d
                    JOIN filings f ON d.filing_id = f.id
                    WHERE d.content_hash IS NOT NULL
                    GROUP BY d.content_hash
                    HAVING COUNT(*) > 1
                    ORDER BY duplicate_count DESC
                    LIMIT ?
                """, (limit,))
                
                duplicates = []
                for row in cursor.fetchall():
                    content_hash, accessions_str, count = row
                    accessions = accessions_str.split(',')
                    
                    duplicates.append({
                        "content_hash": content_hash,
                        "accessions": accessions,
                        "duplicate_count": count,
                        "similarity_type": "identical_content",
                    })
                
                # Find filings with similar metadata (same CIK, form, date)
                cursor = conn.execute("""
                    SELECT 
                        cik,
                        form_type,
                        filing_date,
                        GROUP_CONCAT(accession) as accessions,
                        COUNT(*) as duplicate_count
                    FROM filings
                    WHERE filing_date IS NOT NULL
                    GROUP BY cik, form_type, filing_date
                    HAVING COUNT(*) > 1
                    ORDER BY duplicate_count DESC
                    LIMIT ?
                """, (limit,))
                
                for row in cursor.fetchall():
                    cik, form_type, filing_date, accessions_str, count = row
                    accessions = accessions_str.split(',')
                    
                    duplicates.append({
                        "cik": cik,
                        "form_type": form_type,
                        "filing_date": filing_date,
                        "accessions": accessions,
                        "duplicate_count": count,
                        "similarity_type": "same_metadata",
                    })
                
                return duplicates
                
        except Exception as e:
            self.logger.error(f"Failed to find potential duplicates: {e}")
            return []