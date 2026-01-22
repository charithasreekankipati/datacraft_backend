"""
Helper to execute masking SQL against Databricks using databricks.sql.
Returns execution log entries and raises on fatal errors.
"""

from databricks import sql
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import time
import re
from typing import List, Dict, Any, Tuple, Optional, Union

STATEMENT_TIMEOUT_DEFAULT = 30


def strip_sql_comments(sql_text: str) -> str:
    no_block = re.sub(r"/\*.*?\*/", "", sql_text, flags=re.S)
    no_line = re.sub(r"--.*?$", "", no_block, flags=re.M)
    return no_line


def split_statements(clean_sql: str) -> List[str]:
    parts = [s.strip() for s in clean_sql.split(";")]
    return [p for p in parts if p]


def _execute_statement(cursor, statement: str) -> Tuple[str, Optional[List[Tuple[Any, ...]]]]:
    cursor.execute(statement)
    try:
        rows = cursor.fetchall()
        return ("rows", rows)
    except Exception:
        return ("norows", None)


def _make_drop_statements_safe(stmt: str) -> str:
    """
    Convert DROP TABLE <name>; -> DROP TABLE IF EXISTS <name>;
    Convert DROP VIEW  <name>; -> DROP VIEW  IF EXISTS <name>;
    Works for simple statements and leaves others unchanged.
    """
    # Add IF EXISTS after "DROP TABLE" or "DROP VIEW" when missing
    stmt = re.sub(r'(?i)\bDROP\s+TABLE\s+(?!IF\s+EXISTS\b)', 'DROP TABLE IF EXISTS ', stmt)
    stmt = re.sub(r'(?i)\bDROP\s+VIEW\s+(?!IF\s+EXISTS\b)', 'DROP VIEW IF EXISTS ', stmt)
    return stmt


def exec_statements_with_timeout(
    conn,
    statements: Union[str, List[str]],
    timeout_sec: int = STATEMENT_TIMEOUT_DEFAULT,
    continue_on_error: bool = False
) -> List[str]:
    """
    Accept either a single SQL string (which will be cleaned/split) or a list of statements.
    Executes sequentially with per-statement timeout and returns logs.
    If continue_on_error is True, non-fatal errors are logged and execution proceeds.
    """
    logs: List[str] = []

    if isinstance(statements, str):
        cleaned = strip_sql_comments(statements)
        stmts = split_statements(cleaned)
    else:
        # list provided: trim & drop empties
        stmts = [s.strip() for s in statements if (s and s.strip())]

    if not stmts:
        logs.append("No executable statements found.")
        return logs

    with conn.cursor() as cursor:
        for i, raw_stmt in enumerate(stmts, start=1):
            # Normalize whitespace
            stmt = raw_stmt.strip()
            # Make DROP statements tolerant if possible
            safe_stmt = _make_drop_statements_safe(stmt)
            preview = safe_stmt.splitlines()[0][:300] if safe_stmt.splitlines() else safe_stmt[:300]
            logs.append(f"\n-----\nStatement {i}/{len(stmts)} preview:\n{preview}\nExecuting (timeout={timeout_sec}s)...")

            with ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(_execute_statement, cursor, safe_stmt)
                start = time.time()
                try:
                    kind, payload = fut.result(timeout=timeout_sec)
                    dur = time.time() - start
                    if kind == "rows":
                        logs.append(f"✅ Finished in {dur:.1f}s — rows returned (first row shown):")
                    else:
                        logs.append(f"✅ Finished in {dur:.1f}s — no result to fetch (likely DDL).")
                except FutureTimeout:
                    logs.append(f"❗ Statement timed out after {timeout_sec}s: {preview}")
                    # Attempt to close connection - caller may recreate connection later
                    try:
                        conn.close()
                    except Exception:
                        pass
                    raise TimeoutError(f"Statement timed out after {timeout_sec}s: {preview}")
                except Exception as e:
                    # Capture error text
                    err_text = f"❌ Execution error: {type(e).__name__}: {e}"
                    logs.append(err_text)
                    if continue_on_error:
                        logs.append("Continuing to next statement due to continue_on_error=True.")
                        # attempt to recover cursor (databricks.sql cursor will be closed in context manager if needed)
                        continue
                    else:
                        # re-raise to allow caller to treat as fatal
                        raise
    return logs


def execute_masking_sql(
    masking_sql: Optional[str],
    host: str,
    http_path: str,
    access_token: str,
    statement_timeout: int = STATEMENT_TIMEOUT_DEFAULT,
    verify_mask_effect: bool = True,
    sample_verify_query: Optional[str] = None,
    masking_sql_lines: Optional[List[str]] = None,
    continue_on_error: bool = False,
) -> Dict[str, Any]:
    """
    Execute masking SQL (DDL/DDL+DDL) against the Databricks SQL endpoint.
    You may provide either:
      - masking_sql (full SQL string) OR
      - masking_sql_lines (list of individual statements)

    Returns a dict: { "status": "ok"/"error", "logs": [...], "verify": [...] }

    continue_on_error: if True, non-fatal statement errors (e.g., DROP on missing table)
                       will be recorded in logs and execution will continue to the next statement.
    """
    logs: List[str] = []
    if not masking_sql and not masking_sql_lines:
        return {"status": "error", "logs": ["No masking SQL provided (neither masking_sql nor masking_sql_lines)."]}

    if not (host and http_path and access_token):
        return {"status": "error", "logs": ["Missing connection parameters (host/http_path/access_token)."]}

    try:
        logs.append("Connecting to Databricks SQL endpoint...")
        with sql.connect(server_hostname=host, http_path=http_path, access_token=access_token) as conn:
            logs.append("Connected. Executing statements...")

            # Prefer explicit list if provided, otherwise pass full SQL string to splitter
            if masking_sql_lines:
                exec_logs = exec_statements_with_timeout(
                    conn,
                    masking_sql_lines,
                    timeout_sec=statement_timeout,
                    continue_on_error=continue_on_error
                )
            else:
                exec_logs = exec_statements_with_timeout(
                    conn,
                    masking_sql,
                    timeout_sec=statement_timeout,
                    continue_on_error=continue_on_error
                )

            logs.extend(exec_logs)

            verify_logs: List[str] = []
            if verify_mask_effect:
                # optional: run a light verification if user provided a query, else try to detect current_user and sample table
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT current_user() as who, current_catalog() as catalog, current_schema() as schema")
                        r = cur.fetchall()
                        verify_logs.append(f"Identity: {r}")
                except Exception as e:
                    verify_logs.append(f"Verification identity query failed: {type(e).__name__}: {e}")

                if sample_verify_query:
                    try:
                        with conn.cursor() as cur:
                            cur.execute(sample_verify_query)
                            sample_rows = cur.fetchall()
                            verify_logs.append("Sample verification rows (first shown):")
                            verify_logs.append(str(sample_rows[0] if sample_rows else sample_rows))
                    except Exception as e:
                        verify_logs.append(f"Sample verification query failed: {type(e).__name__}: {e}")

            return {"status": "ok", "logs": logs, "verify": verify_logs}
    except Exception as e:
        logs.append(f"Fatal execution error: {type(e).__name__}: {e}")
        return {"status": "error", "logs": logs}
