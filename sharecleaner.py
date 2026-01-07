import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock

import duckdb
import pyarrow as pa
import typer
from rich.progress import Progress, SpinnerColumn, TextColumn

app = typer.Typer()

TEXT_CHECK_BYTES = 1024
BATCH_SIZE = 50000


def analyze_file_content(path: Path) -> tuple[bool | None, bool | None]:
    """Analyze file content. Returns (is_text, has_shebang). None values if unreadable."""
    try:
        with open(path, "rb") as f:
            chunk = f.read(TEXT_CHECK_BYTES)
        if not chunk:
            return None, None
        has_shebang = chunk.startswith(b"#!")
        if b"\x00" in chunk:
            return False, has_shebang
        text_bytes = sum(
            1 for b in chunk if 32 <= b <= 126 or b in (9, 10, 13) or b >= 128
        )
        is_text = (text_bytes / len(chunk)) > 0.85
        return is_text, has_shebang
    except (OSError, PermissionError):
        return None, None


class BatchCollector:
    """Collects file data in columnar format for efficient PyArrow conversion."""

    def __init__(self, scan_time: datetime):
        self.scan_time = scan_time
        self.dir_paths: list[str] = []
        self.filenames: list[str] = []
        self.extensions: list[str | None] = []
        self.sizes: list[int] = []
        self.mtimes: list[datetime] = []
        self.ctimes: list[datetime] = []
        self.nlinks: list[int] = []
        self.is_texts: list[bool | None] = []
        self.is_hiddens: list[bool] = []
        self.has_shebangs: list[bool | None] = []
        self.last_seens: list[datetime] = []
        self.deleted_ats: list[datetime | None] = []

    def add_file(self, entry):
        """Add a file entry to the batch."""
        stat = entry.stat(follow_symlinks=False)
        path = Path(entry.path)
        is_text, has_shebang = analyze_file_content(path)
        self.dir_paths.append(str(path.parent))
        self.filenames.append(path.name)
        self.extensions.append(path.suffix.lower() if path.suffix else None)
        self.sizes.append(stat.st_size)
        self.mtimes.append(datetime.fromtimestamp(stat.st_mtime))
        self.ctimes.append(datetime.fromtimestamp(stat.st_ctime))
        self.nlinks.append(stat.st_nlink)
        self.is_texts.append(is_text)
        self.is_hiddens.append(path.name.startswith("."))
        self.has_shebangs.append(has_shebang)
        self.last_seens.append(self.scan_time)
        self.deleted_ats.append(None)

    def __len__(self):
        return len(self.filenames)

    def to_arrow(self) -> pa.Table:
        return pa.table(
            {
                "dir_path": self.dir_paths,
                "filename": self.filenames,
                "extension": self.extensions,
                "size_bytes": self.sizes,
                "mtime": self.mtimes,
                "ctime": self.ctimes,
                "nlink": self.nlinks,
                "is_text": self.is_texts,
                "is_hidden": self.is_hiddens,
                "has_shebang": self.has_shebangs,
                "last_seen": self.last_seens,
                "deleted_at": self.deleted_ats,
            }
        )

    def clear(self):
        self.dir_paths.clear()
        self.filenames.clear()
        self.extensions.clear()
        self.sizes.clear()
        self.mtimes.clear()
        self.ctimes.clear()
        self.nlinks.clear()
        self.is_texts.clear()
        self.is_hiddens.clear()
        self.has_shebangs.clear()
        self.last_seens.clear()
        self.deleted_ats.clear()


def insert_batch(con: duckdb.DuckDBPyConnection, batch: BatchCollector, db_lock: Lock):
    """Insert batch using PyArrow for speed."""
    if len(batch) == 0:
        return
    arrow_tbl = batch.to_arrow()
    with db_lock:
        con.execute("INSERT OR REPLACE INTO files SELECT * FROM arrow_tbl")
    batch.clear()


class ExistingFileBatch:
    """Collects existing files for full metadata refresh."""

    def __init__(self, verify_time: datetime):
        self.verify_time = verify_time
        self.dir_paths: list[str] = []
        self.filenames: list[str] = []
        self.extensions: list[str | None] = []
        self.sizes: list[int] = []
        self.mtimes: list[datetime] = []
        self.ctimes: list[datetime] = []
        self.nlinks: list[int] = []
        self.is_texts: list[bool | None] = []
        self.is_hiddens: list[bool] = []
        self.has_shebangs: list[bool | None] = []
        self.last_seens: list[datetime] = []
        self.deleted_ats: list[datetime | None] = []

    def add(
        self,
        dir_path: str,
        filename: str,
        stat_result,
        is_text: bool | None,
        has_shebang: bool | None,
    ):
        """File exists - add with updated metadata."""
        self.dir_paths.append(dir_path)
        self.filenames.append(filename)
        ext = Path(filename).suffix.lower() if Path(filename).suffix else None
        self.extensions.append(ext)
        self.sizes.append(stat_result.st_size)
        self.mtimes.append(datetime.fromtimestamp(stat_result.st_mtime))
        self.ctimes.append(datetime.fromtimestamp(stat_result.st_ctime))
        self.nlinks.append(stat_result.st_nlink)
        self.is_texts.append(is_text)
        self.is_hiddens.append(filename.startswith("."))
        self.has_shebangs.append(has_shebang)
        self.last_seens.append(self.verify_time)
        self.deleted_ats.append(None)

    def __len__(self):
        return len(self.filenames)

    def to_arrow(self) -> pa.Table:
        return pa.table(
            {
                "dir_path": self.dir_paths,
                "filename": self.filenames,
                "extension": self.extensions,
                "size_bytes": self.sizes,
                "mtime": self.mtimes,
                "ctime": self.ctimes,
                "nlink": self.nlinks,
                "is_text": self.is_texts,
                "is_hidden": self.is_hiddens,
                "has_shebang": self.has_shebangs,
                "last_seen": self.last_seens,
                "deleted_at": self.deleted_ats,
            }
        )

    def clear(self):
        self.dir_paths.clear()
        self.filenames.clear()
        self.extensions.clear()
        self.sizes.clear()
        self.mtimes.clear()
        self.ctimes.clear()
        self.nlinks.clear()
        self.is_texts.clear()
        self.is_hiddens.clear()
        self.has_shebangs.clear()
        self.last_seens.clear()
        self.deleted_ats.clear()


class DeletedFileBatch:
    """Collects deleted files - only tracks keys for UPDATE."""

    def __init__(self):
        self.dir_paths: list[str] = []
        self.filenames: list[str] = []

    def add(self, dir_path: str, filename: str):
        self.dir_paths.append(dir_path)
        self.filenames.append(filename)

    def __len__(self):
        return len(self.filenames)

    def to_arrow(self) -> pa.Table:
        return pa.table(
            {
                "dir_path": self.dir_paths,
                "filename": self.filenames,
            }
        )

    def clear(self):
        self.dir_paths.clear()
        self.filenames.clear()


SCANDIR_THRESHOLD = 10
VERIFY_BATCH_SIZE = 50000


def check_directory_batch(
    dir_path: str, filenames: list[str]
) -> tuple[set[str], set[str]]:
    """Check existence of multiple files in a directory using scandir.

    Returns (existing_files, missing_files).
    """
    try:
        existing_in_dir = {
            entry.name
            for entry in os.scandir(dir_path)
            if entry.is_file(follow_symlinks=False)
        }
        filenames_set = set(filenames)
        existing = filenames_set & existing_in_dir
        missing = filenames_set - existing_in_dir
        return existing, missing
    except (OSError, PermissionError):
        return set(), set(filenames)


def check_files_individually(
    dir_path: str, filenames: list[str]
) -> tuple[set[str], set[str]]:
    """Check existence using os.path.exists() for each file."""
    existing = set()
    missing = set()
    for filename in filenames:
        path = os.path.join(dir_path, filename)
        if os.path.exists(path):
            existing.add(filename)
        else:
            missing.add(filename)
    return existing, missing


def flush_existing_batch(
    con: duckdb.DuckDBPyConnection, batch: ExistingFileBatch, db_lock: Lock
):
    """Update existing files using INSERT OR REPLACE."""
    if len(batch) == 0:
        return
    arrow_tbl = batch.to_arrow()
    with db_lock:
        con.execute("INSERT OR REPLACE INTO files SELECT * FROM arrow_tbl")
    batch.clear()


def flush_deleted_batch(
    con: duckdb.DuckDBPyConnection,
    batch: DeletedFileBatch,
    verify_time: datetime,
    db_lock: Lock,
):
    """Mark files as deleted - only updates deleted_at column."""
    if len(batch) == 0:
        return
    arrow_tbl = batch.to_arrow()
    with db_lock:
        con.execute(
            """
            UPDATE files
            SET deleted_at = ?
            FROM arrow_tbl AS d
            WHERE files.dir_path = d.dir_path AND files.filename = d.filename
        """,
            [verify_time],
        )
    batch.clear()


def verify_partition(
    partition: dict[str, list[str]],
    con: duckdb.DuckDBPyConnection,
    db_lock: Lock,
    verify_time: datetime,
    counters: dict[str, list[int]],
) -> None:
    """Verify existence for a partition of directories."""
    existing_batch = ExistingFileBatch(verify_time)
    deleted_batch = DeletedFileBatch()

    for dir_path, filenames in partition.items():
        if len(filenames) >= SCANDIR_THRESHOLD:
            existing, missing = check_directory_batch(dir_path, filenames)
        else:
            existing, missing = check_files_individually(dir_path, filenames)

        for filename in existing:
            full_path = Path(dir_path) / filename
            try:
                stat_result = full_path.stat()
                is_text, has_shebang = analyze_file_content(full_path)
                existing_batch.add(
                    dir_path, filename, stat_result, is_text, has_shebang
                )
                counters["existing"][0] += 1
            except (OSError, PermissionError):
                deleted_batch.add(dir_path, filename)
                counters["missing"][0] += 1

        for filename in missing:
            deleted_batch.add(dir_path, filename)
            counters["missing"][0] += 1

        if len(existing_batch) >= VERIFY_BATCH_SIZE:
            flush_existing_batch(con, existing_batch, db_lock)
        if len(deleted_batch) >= VERIFY_BATCH_SIZE:
            flush_deleted_batch(con, deleted_batch, verify_time, db_lock)

    flush_existing_batch(con, existing_batch, db_lock)
    flush_deleted_batch(con, deleted_batch, verify_time, db_lock)


def collect_parallel_units(
    root: Path, depth: int, scan_time: datetime
) -> tuple[list[Path], BatchCollector]:
    """Walk down to specified depth, collecting files along the way.

    Returns (directories_at_depth, files_collected_above_depth).
    """
    files = BatchCollector(scan_time)
    dirs_at_depth: list[Path] = []

    def walk(path: Path, current_depth: int):
        try:
            with os.scandir(path) as entries:
                for entry in entries:
                    try:
                        if entry.is_file(follow_symlinks=False):
                            files.add_file(entry)
                        elif entry.is_dir(follow_symlinks=False):
                            if current_depth >= depth:
                                dirs_at_depth.append(Path(entry.path))
                            else:
                                walk(Path(entry.path), current_depth + 1)
                    except (PermissionError, OSError):
                        continue
        except (PermissionError, OSError):
            pass

    walk(root, 1)
    return dirs_at_depth, files


def scan_directory_recursive(
    root: Path,
    batch: BatchCollector,
    con: duckdb.DuckDBPyConnection,
    db_lock: Lock,
    counter: list[int],
):
    """Recursively scan directory, inserting batches as we go."""
    try:
        with os.scandir(root) as entries:
            for entry in entries:
                try:
                    if entry.is_file(follow_symlinks=False):
                        batch.add_file(entry)
                        if len(batch) >= BATCH_SIZE:
                            insert_batch(con, batch, db_lock)
                            counter[0] += BATCH_SIZE
                    elif entry.is_dir(follow_symlinks=False):
                        scan_directory_recursive(
                            Path(entry.path), batch, con, db_lock, counter
                        )
                except (PermissionError, OSError):
                    continue
    except (PermissionError, OSError):
        pass


def scan_directory(
    root: Path,
    con: duckdb.DuckDBPyConnection,
    db_lock: Lock,
    counter: list[int],
    scan_time: datetime,
) -> int:
    """Scan a directory tree, inserting in batches."""
    batch = BatchCollector(scan_time)
    scan_directory_recursive(root, batch, con, db_lock, counter)
    remaining = len(batch)
    insert_batch(con, batch, db_lock)
    counter[0] += remaining
    return counter[0]


def init_db(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS files (
            dir_path TEXT,
            filename TEXT,
            extension TEXT,
            size_bytes BIGINT,
            mtime TIMESTAMP,
            ctime TIMESTAMP,
            nlink INTEGER,
            is_text BOOLEAN,
            is_hidden BOOLEAN,
            has_shebang BOOLEAN,
            last_seen TIMESTAMP,
            deleted_at TIMESTAMP,
            PRIMARY KEY (dir_path, filename)
        )
    """)


@app.command()
def scan(
    root: Path = typer.Argument(..., help="Root directory to scan"),
    db: Path = typer.Argument(..., help="DuckDB database file path"),
    parallel_depth: int = typer.Option(
        1,
        "--parallel-depth",
        "-p",
        help="Directory depth at which to parallelize (1=top-level)",
    ),
    workers: int = typer.Option(
        None,
        "--workers",
        "-w",
        help="Number of parallel workers (default: CPU count, use 1-2 for network/HDD storage)",
    ),
):
    """Scan a directory recursively and store file metadata in DuckDB."""
    if not root.exists():
        typer.echo(f"Error: {root} does not exist", err=True)
        raise typer.Exit(1)

    if parallel_depth < 1:
        typer.echo("Error: parallel-depth must be at least 1", err=True)
        raise typer.Exit(1)

    start_time = time.perf_counter()
    scan_time = datetime.now()

    con = duckdb.connect(str(db))
    init_db(con)
    db_lock = Lock()
    file_counter = [0]

    # Collect directories at the parallel depth, and files above that depth
    parallel_dirs, shallow_files = collect_parallel_units(
        root, parallel_depth, scan_time
    )

    # Insert files found above the parallel depth
    shallow_count = len(shallow_files)
    if shallow_count > 0:
        insert_batch(con, shallow_files, db_lock)
        file_counter[0] += shallow_count

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("{task.fields[status]}"),
    ) as progress:
        task = progress.add_task(
            "Scanning...",
            status=f"0/{len(parallel_dirs)} units, {file_counter[0]} files",
        )

        completed = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    scan_directory, d, con, db_lock, file_counter, scan_time
                ): d
                for d in parallel_dirs
            }

            for future in as_completed(futures):
                dir_path = futures[future]
                try:
                    future.result()
                except Exception as e:
                    typer.echo(f"Error scanning {dir_path}: {e}", err=True)
                completed += 1
                progress.update(
                    task,
                    status=f"{completed}/{len(parallel_dirs)} units, {file_counter[0]} files",
                )

    con.close()
    elapsed = time.perf_counter() - start_time
    typer.echo(f"Scanned {file_counter[0]} files into {db} in {elapsed:.2f}s")


READ_BATCH_SIZE = 100000


@app.command()
def verify(
    db: Path = typer.Argument(..., help="DuckDB database file path"),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-n", help="Only report what would be done"
    ),
):
    """Verify files in database still exist on disk, marking missing files as deleted."""
    if not db.exists():
        typer.echo(f"Error: {db} does not exist", err=True)
        raise typer.Exit(1)

    start_time = time.perf_counter()
    verify_time = datetime.now()

    con = duckdb.connect(str(db))

    # Check if files table exists
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if "files" not in tables:
        typer.echo("Error: No 'files' table in database. Run 'scan' first.", err=True)
        con.close()
        raise typer.Exit(1)

    result = con.execute(
        "SELECT MAX(last_seen) FROM files WHERE deleted_at IS NULL"
    ).fetchone()
    latest_ts = result[0]

    if latest_ts is None:
        typer.echo("No files in database to verify")
        con.close()
        return

    stale_count = con.execute(
        "SELECT COUNT(*) FROM files WHERE last_seen < ? AND deleted_at IS NULL",
        [latest_ts],
    ).fetchone()[0]

    if stale_count == 0:
        typer.echo("All files have the latest timestamp - nothing to verify")
        con.close()
        return

    typer.echo(f"Found {stale_count:,} files to verify (not seen since {latest_ts})")

    if dry_run:
        preview = con.execute(
            """
            SELECT dir_path, COUNT(*) as file_count
            FROM files
            WHERE last_seen < ? AND deleted_at IS NULL
            GROUP BY dir_path
            ORDER BY file_count DESC
            LIMIT 10
        """,
            [latest_ts],
        ).fetchall()
        typer.echo("\nTop directories to verify:")
        for dir_path, count in preview:
            typer.echo(f"  {dir_path}: {count} files")
        con.close()
        return

    db_lock = Lock()
    counters = {"existing": [0], "missing": [0]}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("{task.fields[status]}"),
    ) as progress:
        task = progress.add_task(
            "Verifying...",
            status=f"0/{stale_count:,} checked",
        )

        offset = 0
        while offset < stale_count:
            batch_files = con.execute(
                """
                SELECT dir_path, filename
                FROM files
                WHERE last_seen < ? AND deleted_at IS NULL
                ORDER BY dir_path
                LIMIT ? OFFSET ?
            """,
                [latest_ts, READ_BATCH_SIZE, offset],
            ).fetchall()

            if not batch_files:
                break

            by_dir: dict[str, list[str]] = defaultdict(list)
            for dir_path, filename in batch_files:
                by_dir[dir_path].append(filename)

            partitions = partition_directories(by_dir)

            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(
                        verify_partition, p, con, db_lock, verify_time, counters
                    )
                    for p in partitions
                    if p
                ]

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        typer.echo(f"Error during verification: {e}", err=True)

                    progress.update(
                        task,
                        status=f"{counters['existing'][0] + counters['missing'][0]:,}/{stale_count:,} checked, "
                        f"{counters['missing'][0]:,} missing",
                    )

            offset += READ_BATCH_SIZE

    con.close()
    elapsed = time.perf_counter() - start_time
    typer.echo(
        f"\nVerified {counters['existing'][0] + counters['missing'][0]:,} files in {elapsed:.2f}s\n"
        f"  - {counters['existing'][0]:,} still exist (updated last_seen)\n"
        f"  - {counters['missing'][0]:,} marked as deleted"
    )


def partition_directories(by_dir: dict[str, list[str]]) -> list[dict[str, list[str]]]:
    """Partition directories round-robin to workers."""
    num_workers = os.cpu_count() or 4
    partitions: list[dict[str, list[str]]] = [
        defaultdict(list) for _ in range(num_workers)
    ]
    for i, (dir_path, filenames) in enumerate(by_dir.items()):
        partitions[i % num_workers][dir_path] = filenames
    return partitions


def main():
    app()


if __name__ == "__main__":
    main()
