# Sharecleaner

CLI tool to recursively scan directories and store file metadata in DuckDB.

## Usage

```bash
# Scan a directory and store file metadata
uv run sharecleaner scan <root_directory> <output.duckdb>

# Verify stale files still exist, mark missing as deleted
uv run sharecleaner verify <database.duckdb> [--dry-run]
```

## Architecture

- Uses `os.scandir()` for fast directory traversal
- Parallel scanning via `ThreadPoolExecutor` (one thread per top-level directory)
- PyArrow for bulk DuckDB inserts (~1.8M rows/sec vs ~1K rows/sec with `executemany`)
- Text detection by reading first 1KB of each file (checks for null bytes + printable ratio)

## Key Performance Notes

- **Never use `executemany`** for bulk inserts in DuckDB - use PyArrow tables instead
- Batch size of 50K rows balances memory usage with insert efficiency
- Threading lock protects DuckDB connection (not thread-safe)

## Development

Requires nix (with flakes) for the development environment:

```bash
nix develop  # or direnv allow
uv sync
uv run sharecleaner --help
```

## Schema

```sql
CREATE TABLE files (
    dir_path TEXT,
    filename TEXT,
    extension TEXT,
    size_bytes BIGINT,
    mtime TIMESTAMP,
    ctime TIMESTAMP,  -- creation/status change time
    nlink INTEGER,  -- hardlink count (>1 means shared)
    is_text BOOLEAN,  -- NULL if file couldn't be read
    is_hidden BOOLEAN,
    has_shebang BOOLEAN,  -- starts with #! (executable script)
    last_seen TIMESTAMP,  -- timestamp of the scan that last found this file
    deleted_at TIMESTAMP,  -- NULL if file exists, set when verify finds it missing
    PRIMARY KEY (dir_path, filename)
)
```
