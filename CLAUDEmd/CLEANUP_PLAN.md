# Repository Cleanup Plan

## Files to Remove Immediately (Safe)

### Phase 1: Legacy Root Scripts (100% Redundant)
```bash
# Remove legacy standalone scripts
rm edgar_companies.py
rm edgar_fetcher.py  
rm edgar_files.py
rm edgar_monitor.py
```

### Phase 2: User Data & Environment
```bash
# Remove downloaded data (not source code)
rm -rf 10k/

# Remove virtual environment (shouldn't be in repo)
rm -rf venv/
```

### Phase 3: Legacy Package Files (After Migration Complete)
```bash
# These can be removed once all external code is migrated
rm edgar/client.py      # Replaced by client_new.py
rm edgar/config.py      # Replaced by config_manager.py  
rm edgar/s3util.py      # Replaced by s3_manager.py
```

### Phase 4: Legacy Scripts (Optional)
```bash
# Keep these for now with deprecation warnings
# Can remove later when all users migrated
# rm scripts/fetch_10k.py
# rm scripts/list_files.py
# rm scripts/monitor.py
```

## Space Savings Estimate

- **edgar_*.py files**: ~15-20KB
- **10k/ directory**: ~1.5MB (downloaded filing)
- **venv/ directory**: ~50-100MB (virtual environment)
- **Total savings**: ~50-100MB+ (mostly from venv)

## Cleanup Commands

### Conservative Cleanup (Recommended)
```bash
# Remove only obviously redundant files
rm edgar_companies.py edgar_fetcher.py edgar_files.py edgar_monitor.py
rm -rf 10k/ venv/
```

### Aggressive Cleanup (After Testing)
```bash
# Remove legacy package files too
rm edgar/client.py edgar/config.py edgar/s3util.py
rm scripts/monitor.py  # Fully replaced by monitor_new.py
```

## Verification Steps

Before cleanup:
```bash
# Test that new architecture works
python scripts/fetch_10k_new.py --help
python scripts/list_files_new.py --help
python scripts/monitor_new.py --help

# Run tests
python -m pytest tests/ -v
```

After cleanup:
```bash
# Verify package still works
python -c "import edgar; print('Import successful')"
python -m pytest tests/ -v
```

## Rollback Plan

If issues arise:
```bash
# Restore from git
git checkout HEAD -- edgar_companies.py edgar_fetcher.py edgar_files.py edgar_monitor.py

# Or restore from backup if not in git
```