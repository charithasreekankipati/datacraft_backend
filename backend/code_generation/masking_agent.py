# agents/masking_agent.py 
from typing import Dict, List, Tuple, Any, Optional
from backend.agents.agent_state import AgentState
from backend.llm_provider import llm
import re
import json

# ---------------------- Configuration (adjustable) -------------------------
# Layers we'll produce masks for (strict)
TARGET_MASK_LAYERS = {"bronze"} 


FORCE_GENERATE_UNVERIFIED = True


ALLOW_LLM_FALLBACK = True


LLM_CONFIDENCE_THRESHOLD = 0.6

CREATE_FUNCTION_PER_COLUMN = False

# ---------------------- Utility helpers ------------------------------------

def _normalize_table_name(layer: str, table: str) -> str:
    table = (table or "").strip()
    low = table.lower()
    if not low:
        return f"{layer}_unknown"
    if low.startswith(f"{layer}_"):
        return low
    if any(low.startswith(p) for p in ("bronze_", "silver_", "gold_")):
        return low
    return f"{layer}_{low}"


def _safe_fn_name_fragment(name: str) -> str:
    name = (name or "").strip().lower()
    name = re.sub(r'[^0-9a-z_]', '_', name)
    name = re.sub(r'_{2,}', '_', name)
    name = name.strip('_')
    return name or "x"


def _make_mask_fn_name(table: str, col: str) -> str:
    tfrag = _safe_fn_name_fragment(re.sub(r'^(bronze_|silver_|gold_)', '', table))
    cfrag = _safe_fn_name_fragment(col)
    return f"{tfrag}__{cfrag}_mask"


def _normalize_column_candidates(col: str) -> List[str]:
    """
    Produce variants of a column name to try matching (dot->underscore, last segment, lowercase)
    """
    if not col:
        return []
    cand = []
    low = col.lower()
    cand.append(col)
    cand.append(low)
    if "." in low:
        cand.append(low.replace(".", "_"))
        cand.append(low.split(".")[-1])
    if "-" in low:
        cand.append(low.replace("-", "_"))
    cand.append(low.replace(" ", "_"))
    # dedupe preserving order
    seen = set()
    out = []
    for c in cand:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def extract_catalog_schema_from_dbfs_path(dbfs_path: str) -> (str, str):
    parts = (dbfs_path or "").strip("/").split("/")
    if len(parts) >= 3 and parts[0].lower() == "volumes":
        return parts[1], parts[2]
    return "main", "default"


# ---------------------- DLT parser & lineage extraction --------------------

def extract_dlt_table_columns_and_lineage(pyspark_code: str) -> Tuple[Dict[str, List[str]], Dict[str, List[Dict[str, str]]]]:
    """
    Parse DLT python code and return:
      - dlt_table_columns: mapping table_name -> [aliased columns]
      - dlt_lineage_map: mapping table_name -> [{"src":source_col,"alias":alias}, ...]
    This function prioritizes capturing F.col(...).alias("...") so that
    alias names (the final names in the created table) are preserved.
    Always returns dicts (never raises to caller).
    """
    try:
        if not isinstance(pyspark_code, str) or not pyspark_code.strip():
            return {}, {}

        dlt_cols: Dict[str, List[str]] = {}
        lineage: Dict[str, List[Dict[str, str]]] = {}

       
        for m in re.finditer(r'@dlt\.table\s*\(\s*name\s*=\s*([\'"])(?P<tname>.+?)\1', pyspark_code, re.IGNORECASE):
            tname = m.group("tname").strip()
            start_pos = m.end()
            func_re = re.compile(r'def\s+' + re.escape(tname) + r'\s*\(\s*\)\s*:\s*(?P<body>.*?)(?=(?:@dlt\.table\b|def\b|$))', re.DOTALL | re.IGNORECASE)
            func_match = func_re.search(pyspark_code[start_pos:])
            func_body = func_match.group("body") if func_match else ""

            cols: List[str] = []
            table_lineage: List[Dict[str, str]] = []

            
            for select_match in re.finditer(r'\.select\s*\(\s*(?P<args>.*?)\s*\)\s*', func_body, re.DOTALL | re.IGNORECASE):
                args = select_match.group("args") or ""

              
                for colm in re.finditer(r'F\.col\(\s*([\'"])(?P<c>.+?)\1\s*\)\s*(?:\.\s*alias\s*\(\s*([\'"])(?P<a>.+?)\3\s*\))?', args, re.DOTALL | re.IGNORECASE):
                    src = colm.group("c").strip()
                    alias = colm.group("a")
                    cname = alias.strip() if alias else src
                    if cname not in cols:
                        cols.append(cname)
                    table_lineage.append({"src": src, "alias": cname})

            
                for plain in re.finditer(r'([\'"])(?P<p>[\w\.\- ]+?)\1', args):
                    p = plain.group("p").strip()
                    if p in cols:
                        continue
                    if "(" in p or ")" in p or p.lower().startswith("f."):
                        continue
                    cols.append(p)
                    table_lineage.append({"src": p, "alias": p})

       
                if not cols:
                    for bare in re.finditer(r'\b([A-Za-z_][A-Za-z0-9_\.]*)\b', args):
                        token = bare.group(1).strip()
                        if token.lower() in ("f", "col", "select", "df", "spark", "dlt", "count", "alias"):
                            continue
                        if token not in cols:
                            cols.append(token)
                            table_lineage.append({"src": token, "alias": token})

          
            seen = set()
            clean_cols = []
            for c in cols:
                if c not in seen:
                    seen.add(c)
                    clean_cols.append(c)
            seen_pairs = set()
            clean_lineage = []
            for e in table_lineage:
                pair = (e.get("src"), e.get("alias"))
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    clean_lineage.append(e)

            dlt_cols[tname] = clean_cols
            lineage[tname] = clean_lineage

        return dlt_cols, lineage
    except Exception:
        return {}, {}


# ---------------------- Mapping helpers -----------------------------------

def _build_mapping_lookup(state: AgentState) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """
    Build mapping lookup keyed by (source_table_lower, source_column) -> (target_table, target_column).
    Add helpful short/variant keys to improve matching.
    """
    lookup: Dict[Tuple[str, str], Tuple[str, str]] = {}

    mr = getattr(state, "mapping_rows", []) or []
    gm = getattr(state, "gold_mapping_rows", []) or []

    for r in mr:
        try:
            btbl = (r.get("bronze_table") or r.get("df_name") or "").strip()
            bcol = (r.get("bronze_columns") or r.get("column_name") or "").strip()
            stbl = (r.get("silver_table") or r.get("target_table") or "").strip()
            scol = (r.get("silver_column") or r.get("target_column") or "").strip()
            if btbl and bcol and stbl and scol:
                lookup[(btbl.lower(), bcol)] = (stbl, scol)
                lookup[(btbl.lower().replace(".", "_"), bcol)] = (stbl, scol)
                lookup[(btbl.split('.')[-1].lower(), bcol)] = (stbl, scol)
        except Exception:
            continue

    for r in gm:
        try:
            s_tbl = (r.get("silver_table") or r.get("source_table") or "").strip()
            s_col = (r.get("silver_column") or r.get("source_column") or "").strip()
            g_tbl = (r.get("target_table") or r.get("gold_table") or r.get("table") or "").strip()
            g_col = (r.get("target_column") or r.get("gold_column") or r.get("column_name") or "").strip()
            if s_tbl and s_col and g_tbl and g_col:
                lookup[(s_tbl.lower(), s_col)] = (g_tbl, g_col)
                lookup[(s_tbl.lower().replace(".", "_"), s_col)] = (g_tbl, g_col)
                lookup[(s_tbl.split('.')[-1].lower(), s_col)] = (g_tbl, g_col)
        except Exception:
            continue

    return lookup


# ---------------------- Bronze fallback population --------------------------

def _populate_bronze_fallbacks(dlt_table_columns: Dict[str, List[str]], mapping_rows: List[Dict[str, Any]]):
    """
    If a bronze mapping refers to 'patient_test' but the DLT code has 'bronze_patient_test',
    populate guessed bronze table entries so bronze masking is not skipped.
    This is conservative and only adds the column name as-is (no aliasing).
    """
    for r in mapping_rows:
        try:
            btbl = (r.get("bronze_table") or r.get("df_name") or "").strip()
            bcol = (r.get("bronze_columns") or r.get("column_name") or "").strip()
            if not btbl or not bcol:
                continue
            short = btbl.split('.')[-1]
            guessed_names = {
                btbl,
                btbl.replace(".", "_"),
                f"bronze_{btbl}",
                f"bronze_{btbl.replace('.', '_')}",
                f"bronze_{short}",
                f"bronze_{short.replace('.', '_')}"
            }
            for g in guessed_names:
                if g not in dlt_table_columns:
                    dlt_table_columns[g] = []
                if bcol not in dlt_table_columns[g]:
                    dlt_table_columns[g].append(bcol)
        except Exception:
            continue


# ---------------------- Main agent ----------------------------------------

def masking_agent(state: AgentState) -> AgentState:
    """
    Generate Databricks SQL masking functions and ALTER TABLE ... SET MASK statements
    for TARGET_MASK_LAYERS (bronze and silver). Uses mapping rows and DLT code lineage and
    attempts to prefer aliased column names (the final created columns).
    This variant uses a single access principal configured by the UI:
      - state.access_mode  -> "group" or "single"
      - state.access_value -> the group name or user id
    Backwards compatibility: will fall back to pii_* or phi_* state keys if present.
    """
    messages = state.messages or []
    sensitive = getattr(state, "sensitive_metadata", {}) or {}
    pyspark_code = getattr(state, "pyspark_code", "") or ""

    catalog = getattr(state, "catalog", None) or None
    schema = getattr(state, "schema", None) or None
    dbfs_path = getattr(state, "dbfs_path", None)

    if dbfs_path:
        extracted_catalog, extracted_schema = extract_catalog_schema_from_dbfs_path(dbfs_path)
        catalog = extracted_catalog or catalog or "main"
        schema = extracted_schema or schema or "default"
    else:
        catalog = catalog or "main"
        schema = schema or "default"

     
    access_mode = getattr(state, "access_mode", None)
    access_value = getattr(state, "access_value", None)

    
    if not access_mode:
        access_mode = getattr(state, "pii_access_mode", None)
    if not access_value:
        access_value = getattr(state, "pii_access_value", None) or getattr(state, "pii_access_group", None) or getattr(state, "pii_access_user", None)

   
    if not access_mode:
        access_mode = getattr(state, "phi_access_mode", None)
    if not access_value:
        access_value = getattr(state, "phi_access_value", None) or getattr(state, "phi_access_group", None) or getattr(state, "phi_access_user", None)

    
    access_mode = (access_mode or "group").lower()
    access_value = (access_value or "pii_access").strip()

   
    dlt_table_columns, dlt_lineage_map = extract_dlt_table_columns_and_lineage(pyspark_code)
    
    dlt_tables = set(dlt_table_columns.keys())

    _populate_bronze_fallbacks(dlt_table_columns, getattr(state, "mapping_rows", []) or [])
    dlt_tables = set(dlt_table_columns.keys())

    mapping_lookup = _build_mapping_lookup(state)

    def find_matching_dlt_table(pref_name: Optional[str]) -> Optional[str]:
        if not pref_name:
            return None
        pref = pref_name.lower().strip()
        variants = {pref, pref.replace(".", "_"), pref.replace("-", "_"), f"bronze_{pref}", f"silver_{pref}"}
        
        variants.add(pref.split('.')[-1])
        
        
        for t in dlt_tables:
            tl = t.lower()
            if tl in variants:
                return t
        
        for t in dlt_tables:
            tl = t.lower()
            short = tl.split('.')[-1]
            if short in variants:
                return t
            for v in variants:
                if tl.endswith(v):
                    return t
        return None

    
    def pick_column_from_dlt(dlt_table: Optional[str], mapped_col_candidates: List[str]) -> Optional[str]:
        if not dlt_table or dlt_table not in dlt_table_columns:
            return None
        
        lineage_entries = dlt_lineage_map.get(dlt_table, []) or []
        alias_map = {e["alias"].lower(): e["alias"] for e in lineage_entries if e.get("alias")}
        src_map = {e["src"].lower(): e["alias"] for e in lineage_entries if e.get("src") and e.get("alias")}
        actual_cols = dlt_table_columns.get(dlt_table, [])
        actual_lower_map = {c.lower(): c for c in actual_cols}

        
        for cand in mapped_col_candidates:
            for variant in _normalize_column_candidates(cand):
                vl = variant.lower()
                if vl in alias_map:
                    return alias_map[vl]  
                if vl in actual_lower_map:
                    return actual_lower_map[vl]

                # if variant matches a source column in lineage, return the alias if present
                if vl in src_map:
                    ali = src_map[vl]
                    if ali:
                        return ali

        # last-segment fallback
        for cand in mapped_col_candidates:
            last = cand.split(".")[-1].lower()
            if last in actual_lower_map:
                return actual_lower_map[last]
            if last in alias_map:
                return alias_map[last]
        return None

    # trace one-hop lineage: try to find alias in target_table corresponding to start_table.start_col
    def trace_lineage_to_table(start_table: str, start_col: str, target_table: str) -> Optional[str]:
        if not start_table or not start_col or not target_table:
            return None
        # if same table, check directly
        for e in dlt_lineage_map.get(target_table, []):
            if e.get("src") == start_col or e.get("alias") == start_col or e.get("src", "").split(".")[-1] == start_col.split(".")[-1]:
                return e.get("alias")
        # check start_table lineage for alias then see if target_table references it as src
        start_alias = None
        for e in dlt_lineage_map.get(start_table, []):
            if e.get("src") == start_col or e.get("alias") == start_col or e.get("src", "").split(".")[-1] == start_col.split(".")[-1]:
                start_alias = e.get("alias")
                break
        if start_alias:
            for e in dlt_lineage_map.get(target_table, []):
                if e.get("src") == start_alias or e.get("alias") == start_alias:
                    return e.get("alias")
        # nothing found
        return None

    # helper to build access-check expression for function body (unified principal)
    def build_access_condition() -> str:
        """
        Returns a boolean expression (SQL fragment) to check access:
        - if single-user mode selected and user provided -> current_user() = '<user>'
        - else if group mode selected and group provided -> is_account_group_member('<group>')
        - else fallback to always-false check (no access -> mask)
        """
        if access_mode == "single" and access_value:
            return f"current_user() = '{access_value}'"
        if access_mode == "group" and access_value:
            return f"is_account_group_member('{access_value}')"
        return "FALSE"

    # ---------------- deterministic generation ----------------
    def deterministic_generate() -> Tuple[str, List[str], Dict[str, Any]]:
        ordered_fns: Dict[str, str] = {}
        alter_stmts: List[str] = []
        plan_functions: List[Dict[str, Any]] = []
        plan_alters: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []
        llm_suggestions: List[Dict[str, Any]] = []

        # track created functions and alter stmts to avoid duplicates
        created_fn_set = set()
        created_alter_set = set()

        # build table_map from sensitive metadata: (layer, table) -> list[(col, tag)]
        table_map: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        # Only consider layers that are in TARGET_MASK_LAYERS for mask generation
        for layer in sorted(list(sensitive.keys())):
            if layer not in TARGET_MASK_LAYERS:
                # we still keep the metadata available for lineage tracing, but DO NOT create masks for other layers
                continue
            layer_map = sensitive.get(layer, {}) or {}
            for tbl_name, cols in layer_map.items():
                if not isinstance(cols, dict):
                    continue
                for col, tag in cols.items():
                    table_map.setdefault((layer, tbl_name), []).append((col, tag))

        for (layer, src_tbl), col_list in sorted(table_map.items(), key=lambda x: (x[0][0], x[0][1])):
            # mapping/lineage resolution happens below
            dlt_matched_table = None

            # 1) mapping lookup try direct (use lowercase keys)
            resolved_tbl = None
            for key in [(src_tbl.lower(), c) for c, _ in col_list]:
                if key in mapping_lookup:
                    resolved_tbl, _ = mapping_lookup[key]
                    break
            if not resolved_tbl:
                for (k_tbl, k_col), (v_tbl, v_col) in mapping_lookup.items():
                    if k_tbl == src_tbl.lower() or k_tbl == src_tbl.lower().replace(".", "_") or k_tbl.endswith(src_tbl.lower()):
                        resolved_tbl = v_tbl
                        break

            if resolved_tbl:
                dlt_matched_table = find_matching_dlt_table(resolved_tbl)
            if not dlt_matched_table:
                # try to match DLT table by source table name itself
                dlt_matched_table = find_matching_dlt_table(src_tbl)
            if not dlt_matched_table and src_tbl:
                # last resort: suffix match over dlt tables
                last = src_tbl.split(".")[-1].lower()
                for t in dlt_tables:
                    if t.lower().endswith(last):
                        dlt_matched_table = t
                        break

            for col, tag in sorted(col_list, key=lambda x: x[0]):
                tag_up = (tag or "").strip().upper()
                if tag_up not in ("PII", "PHI"):
                    # ignore non-sensitive
                    continue

                mapped_table = None
                mapped_col = None
                for lk in [(src_tbl.lower(), col), (src_tbl.lower().replace(".", "_"), col), (src_tbl.split('.')[-1].lower(), col)]:
                    if lk in mapping_lookup:
                        mapped_table, mapped_col = mapping_lookup[lk]
                        break

                # pick initial candidate table preference
                target_dlt_table = None
                if mapped_table:
                    target_dlt_table = find_matching_dlt_table(mapped_table)
                if not target_dlt_table:
                    target_dlt_table = dlt_matched_table

                # build candidate column names
                candidates = []
                if mapped_col:
                    candidates.append(mapped_col)
                candidates.append(col)
                expanded_candidates = []
                for c in candidates:
                    expanded_candidates.extend(_normalize_column_candidates(c))

                # 1) deterministic pick on target table (prefer alias names)
                final_col = pick_column_from_dlt(target_dlt_table, expanded_candidates)

                # 2) try one-hop lineage trace from mapped_table -> target_dlt_table
                if not final_col and mapped_table and target_dlt_table:
                    traced = trace_lineage_to_table(mapped_table, mapped_col or col, target_dlt_table)
                    if traced:
                        final_col = traced

                # 3) cross-table search with preference: silver tables first (to get aliases), then bronze fallbacks
                if not final_col:
                    # build preference list (prefer any silver DLT tables)
                    pref_tables = []
                    for t in sorted(dlt_tables):
                        if "silver" in t:
                            pref_tables.append(t)
                    # then bronze
                    for t in sorted(dlt_tables):
                        if "bronze" in t and t not in pref_tables:
                            pref_tables.append(t)
                    # then remaining
                    for t in sorted(dlt_tables):
                        if t not in pref_tables:
                            pref_tables.append(t)

                    for cand_table in pref_tables:
                        cand_found = pick_column_from_dlt(cand_table, expanded_candidates)
                        if cand_found:
                            target_dlt_table = cand_table
                            final_col = cand_found
                            plan_functions.append({
                                "fn_note": "cross_table_resolved",
                                "source_table": src_tbl,
                                "source_column": col,
                                "resolved_table": cand_table,
                                "resolved_column": final_col
                            })
                            break

                # 4) optional LLM fallback (controlled)
                if not final_col and ALLOW_LLM_FALLBACK:
                    # prepare compact payload for LLM; we expect the LLM to return small JSON
                    payload = {
                        "task": "align_sensitive_to_dlt",
                        "layer": layer,
                        "source_table": src_tbl,
                        "source_column": col,
                        "source_tag": tag_up,
                        "mapping_lookup_sample": ({"mapped_table": mapped_table, "mapped_col": mapped_col} if mapped_table else None),
                        "dlt_tables_sample": {t: dlt_table_columns.get(t, []) for t in sorted(list(dlt_tables))[:50]}
                    }
                    prompt_text = (
                        "You are an assistant that maps a source sensitive (table,column) to actual DLT tables/columns.\n"
                        "Input JSON:\n" + json.dumps(payload) + "\n\n"
                        "Respond with ONLY a JSON array of suggestion objects like:\n"
                        "[{\"dlt_table\": \"silver_patient\", \"dlt_column\": \"patient_identifier\", \"confidence\": 0.8}, ...]\n"
                        "Return only tables/columns that appear in the provided dlt_tables map. Prefer bronze/silver suggestions.\n"
                        "If no suggestion, return an empty array []."
                    )
                    raw_llm = ""
                    try:
                        resp = llm.invoke(prompt_text)
                        raw_llm = getattr(resp, "content", None) or str(resp)
                    except Exception:
                        raw_llm = ""
                    # parse JSON array if present
                    suggestions = []
                    try:
                        jm = re.search(r'(\[.*\])', raw_llm, re.DOTALL)
                        if jm:
                            suggestions = json.loads(jm.group(1))
                            if not isinstance(suggestions, list):
                                suggestions = []
                    except Exception:
                        suggestions = []

                    llm_suggestions.append({
                        "layer": layer,
                        "source_table": src_tbl,
                        "source_column": col,
                        "raw_llm": raw_llm,
                        "parsed": suggestions
                    })

                    # accept first validated suggestion above threshold
                    chosen = None
                    for s in suggestions:
                        try:
                            stbl = s.get("dlt_table")
                            scol = s.get("dlt_column")
                            conf = float(s.get("confidence", 0))
                            if stbl and scol and stbl in dlt_table_columns and scol in dlt_table_columns.get(stbl, []):
                                if conf >= LLM_CONFIDENCE_THRESHOLD:
                                    chosen = (stbl, scol, conf)
                                    break
                        except Exception:
                            continue
                    if chosen:
                        target_dlt_table = chosen[0]
                        final_col = chosen[1]
                        plan_functions.append({
                            "fn_note": "llm_fallback_used",
                            "source_table": src_tbl,
                            "source_column": col,
                            "dlt_table": target_dlt_table,
                            "dlt_column": final_col,
                            "confidence": chosen[2]
                        })

                # 5) if still not resolved and force generation allowed -> create unverified using mapping-derived names
                if not final_col and FORCE_GENERATE_UNVERIFIED:
                    forced_table = target_dlt_table or dlt_matched_table or _normalize_table_name(layer, src_tbl)
                    forced_col = expanded_candidates[0] if expanded_candidates else col
                    final_col = forced_col
                    target_dlt_table = forced_table
                    plan_functions.append({
                        "fn_note": "forced_unverified",
                        "source_table": src_tbl,
                        "source_column": col,
                        "resolved_table": target_dlt_table,
                        "resolved_column": final_col
                    })

                if not final_col:
                    skipped.append({
                        "reason": "unable_to_determine_column",
                        "layer": layer,
                        "source_table": src_tbl,
                        "source_column": col,
                        "tried_table_candidates": [dlt_matched_table, target_dlt_table],
                        "col_candidates": expanded_candidates
                    })
                    continue

                # preserve exact casing if discovered in DLT discovered columns
                used_table = target_dlt_table or dlt_matched_table or _normalize_table_name(layer, src_tbl)
                if used_table in dlt_table_columns:
                    actual_map = {c.lower(): c for c in dlt_table_columns.get(used_table, [])}
                    if final_col.lower() in actual_map:
                        final_col = actual_map[final_col.lower()]

                # build function + alter statements
                # Decide function name strategy
                if CREATE_FUNCTION_PER_COLUMN:
                    fn_local = _make_mask_fn_name(used_table, final_col)
                else:
                    # shared function naming per configured principal (user or group)
                    principal_frag = _safe_fn_name_fragment(access_value or ('pii_access'))
                    if access_mode == "single":
                        fn_local = f"mask_user__{principal_frag}"
                    else:
                        fn_local = f"mask_group__{principal_frag}"

                full_fn = f"{catalog}.{schema}.{fn_local}"

                # build the function body based on unified access control mode
                access_cond = build_access_condition()
                fn_body = f"CREATE OR REPLACE FUNCTION {full_fn}(val STRING)\nRETURN CASE WHEN {access_cond} THEN val ELSE '***MASKED***' END;"

                # dedupe function creation
                if full_fn not in created_fn_set:
                    ordered_fns[full_fn] = fn_body
                    created_fn_set.add(full_fn)
                    plan_functions.append({
                        "fn": full_fn,
                        "table": f"{catalog}.{schema}.{used_table}",
                        "column": final_col,
                        "tag": tag_up,
                        "access_mode": access_mode,
                        "access_value": access_value,
                        "shared_fn": not CREATE_FUNCTION_PER_COLUMN
                    })

                col_quoted = f"`{final_col}`"
                full_table_id = f"{catalog}.{schema}.{used_table}"

                alter_stmt = (
                    f"ALTER TABLE {full_table_id}\n"
                    f"ALTER COLUMN {col_quoted}\n"
                    f"SET MASK {full_fn};"
                )

                # dedupe alter statements
                if alter_stmt not in created_alter_set:
                    alter_stmts.append(alter_stmt)
                    created_alter_set.add(alter_stmt)
                    plan_alters.append({
                        "stmt": alter_stmt,
                        "table": full_table_id,
                        "column": final_col,
                        "fn": full_fn,
                        "tag": tag_up,
                        "verified": (pick_column_from_dlt(used_table, [final_col]) is not None)
                    })

        # produce deterministic ordering: sorted functions then alters in creation order
        ordered: List[str] = []
        for fn_sql in sorted(ordered_fns.values()):
            ordered.append(fn_sql)
        for a in alter_stmts:
            ordered.append(a)

        combined = "\n\n".join(ordered) if ordered else ""
        plan = {
            "functions": plan_functions,
            "alters": plan_alters,
            "skipped": skipped,
            "llm_suggestions": llm_suggestions,
            "dlt_table_columns": dlt_table_columns,
            "dlt_lineage_map": dlt_lineage_map
        }
        return combined, ordered, plan

    # run generation
    final_sql, final_lines, final_plan = deterministic_generate()

    # append message and persist new attributes on AgentState
    new_messages = list(messages) + [{
        "role": "assistant",
        "content": final_sql,
        "name": "masking_agent"
    }]

    try:
        return state.copy(update={
            "messages": new_messages,
            "masking_sql": final_sql,
            "masking_sql_lines": final_lines,
            "masking_plan": final_plan,
            "dlt_table_columns": final_plan.get("dlt_table_columns"),
            "dlt_lineage_map": final_plan.get("dlt_lineage_map")
        })
    except Exception:
        state.messages = new_messages
        setattr(state, "masking_sql", final_sql)
        setattr(state, "masking_sql_lines", final_lines)
        setattr(state, "masking_plan", final_plan)
        setattr(state, "dlt_table_columns", final_plan.get("dlt_table_columns"))
        setattr(state, "dlt_lineage_map", final_plan.get("dlt_lineage_map"))
        return state


def get_masking_agent() -> Tuple:
    desc = ("Generates Databricks SQL masking functions and ALTER TABLE ... SET MASK statements "
            "for bronze and silver tables only. Uses mapping rows and DLT code lineage and "
            "attempts to prefer aliased column names (the final created columns). Uses a single "
            "configured access principal (group or single user) from state.access_mode/value.")
    return masking_agent, desc


run_masking_agent = masking_agent