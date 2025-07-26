"""High level filing helpers with enhanced type safety and error handling."""
from typing import List, Dict, Optional, Any
import os
import logging
from pathlib import Path

from .client_new import EdgarClient, ClientConfig
from .config_manager import ConfigManager
from .urls import URLBuilder, validate_cik, validate_accession_number, CIKValidationError, AccessionValidationError
from .parser import parse_file_list


def fetch_latest_10k(cik: str, download_dir: str = "10k") -> Optional[str]:
    """Download the latest 10-K filing and save it to disk.
    
    Args:
        cik: Central Index Key
        download_dir: Directory to save the filing
        
    Returns:
        Path to downloaded file, or None if no 10-K found
        
    Raises:
        CIKValidationError: If CIK format is invalid
        RuntimeError: If download fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        validate_cik(cik)
        
        # Use new architecture
        config_manager = ConfigManager()
        config = config_manager.get_config()
        
        client_config = ClientConfig(
            rate_limit_per_sec=config.rate_limit_per_sec,
            timeout=config.timeout,
            user_agent=config.user_agent
        )
        
        with EdgarClient(client_config) as client:
            # Get submissions data
            url = URLBuilder.submissions_url(cik)
            data = client.get_json(url)
            
            filings = data.get("filings", {}).get("recent", {})
            forms = filings.get("form", [])
            accession_numbers = filings.get("accessionNumber", [])
            primary_docs = filings.get("primaryDocument", [])

            for form, accession, doc in zip(forms, accession_numbers, primary_docs):
                if form == "10-K":
                    # Create directory
                    download_path = Path(download_dir)
                    download_path.mkdir(parents=True, exist_ok=True)
                    
                    # Build URL and download using new URL builder
                    url = URLBuilder.document_url(cik, accession, doc)
                    local_path = download_path / doc
                    
                    logger.info(f"Downloading {doc}")
                    resp = client.get(url)
                    
                    with open(local_path, 'wb') as f:
                        f.write(resp.content)
                    
                    logger.info(f"Downloaded {doc} ({len(resp.content)} bytes)")
                    return str(local_path)
            
            logger.warning(f"No 10-K filing found for CIK {cik}")
            return None
        
    except CIKValidationError:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch 10-K for CIK {cik}: {e}")
        raise RuntimeError(f"Failed to fetch 10-K for CIK {cik}: {e}")


def list_recent_filings(cik: str) -> List[Dict[str, str]]:
    """Return recent filings for a company.
    
    Args:
        cik: Central Index Key
        
    Returns:
        List of filing information dictionaries
        
    Raises:
        CIKValidationError: If CIK format is invalid
        RuntimeError: If API request fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        validate_cik(cik)
        
        # Use new architecture
        config_manager = ConfigManager()
        config = config_manager.get_config()
        
        client_config = ClientConfig(
            rate_limit_per_sec=config.rate_limit_per_sec,
            timeout=config.timeout,
            user_agent=config.user_agent
        )
        
        with EdgarClient(client_config) as client:
            # Get submissions data
            url = URLBuilder.submissions_url(cik)
            data = client.get_json(url)
            
            recent = data.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            accession_numbers = recent.get("accessionNumber", [])
            primary_docs = recent.get("primaryDocument", [])
            
            filings = []
            for form, accession, doc in zip(forms, accession_numbers, primary_docs):
                filings.append({
                    "form": form,
                    "accession": accession,
                    "doc": doc,
                })
            
            logger.debug(f"Retrieved {len(filings)} recent filings for CIK {cik}")
            return filings
        
    except CIKValidationError:
        raise
    except Exception as e:
        logger.error(f"Failed to get recent filings for CIK {cik}: {e}")
        raise RuntimeError(f"Failed to get recent filings for CIK {cik}: {e}")


def get_filing_files(cik: str, accession_number: str) -> List[Dict[str, str]]:
    """Return file details for a specific filing.
    
    Args:
        cik: Central Index Key
        accession_number: Filing accession number
        
    Returns:
        List of document information dictionaries
        
    Raises:
        CIKValidationError: If CIK format is invalid
        ValueError: If accession number format is invalid
        RuntimeError: If API request fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        validate_cik(cik)
        validate_accession_number(accession_number)
        
        # Use new architecture
        config_manager = ConfigManager()
        config = config_manager.get_config()
        
        client_config = ClientConfig(
            rate_limit_per_sec=config.rate_limit_per_sec,
            timeout=config.timeout,
            user_agent=config.user_agent
        )
        
        with EdgarClient(client_config) as client:
            # Build URL using new URL builder
            url = URLBuilder.filing_index_url(cik, accession_number)
            
            logger.debug(f"Fetching filing files from {url}")
            resp = client.get(url)
            
            files = parse_file_list(resp.text)
            logger.debug(f"Found {len(files)} files in filing {accession_number}")
            
            return files
        
    except (CIKValidationError, AccessionValidationError):
        raise
    except Exception as e:
        logger.error(f"Failed to get filing files for {cik}/{accession_number}: {e}")
        raise RuntimeError(f"Failed to get filing files for {cik}/{accession_number}: {e}")
