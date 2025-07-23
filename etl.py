"""
Manufacturing ETL Pipeline - FINAL FIXED VERSION WITH OPTIMIZED PARALLELIZATION
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────── Config ──────────────────────────────── #

@dataclass(frozen=True)
class Config:
    master_file: Path = Path("Master.xlsx")
    inventory_file: Path = Path("Live-Stock-09-06-2025.xlsx")
    log_level: int = logging.INFO
    SHEET_PROCESS: str = "Process"
    SHEET_ROUTING: str = "Part Lead Time"
    SHEET_OPEN_ORDER: str = "Open Order 9-Jun-25"
    WIP_SHEETS: Tuple[str, ...] = ("WIP MC", "WIP RAW", "RFD", "RFM", "WIP DM")

cfg = Config()

logging.basicConfig(
    level=cfg.log_level,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─────────────────────────── Helper utilities ────────────────────────── #

def safe_float(val) -> float:
    """Convert any value to float safely; NaNs become 0."""
    try:
        if pd.isna(val):
            return 0.0
        return float(str(val).strip())
    except Exception:
        return 0.0

def is_valid_code(code) -> bool:
    return pd.notna(code) and str(code).strip().lower() not in ['nan', ''] and str(code).strip()

# ────────────────────────────── Extractors ───────────────────────────── #

def read_process_master(path: Path) -> pd.DataFrame:
    """Read 'Process' sheet and return a cleaned process-master DataFrame."""
    try:
        df = pd.read_excel(path, sheet_name=cfg.SHEET_PROCESS, header=1)
        logging.info(f"Process sheet columns: {list(df.columns)}")
        
        col_mapping = {}
        for col in df.columns:
            col_str = str(col).strip()
            if col_str in ['Process', 'Unnamed: 2']:
                col_mapping[col] = 'PROCESS_CODE'
            elif col_str in ['Full Form', 'Unnamed: 3']:
                col_mapping[col] = 'PROCESS_FULL_NAME'
            elif col_str in ['Owner Name', 'Unnamed: 4']:
                col_mapping[col] = 'PROCESS_OWNER'
        
        if len(col_mapping) < 2:
            cols = df.columns.tolist()
            if len(cols) >= 3:
                col_mapping = {
                    cols[1]: 'PROCESS_CODE',
                    cols[2]: 'PROCESS_FULL_NAME', 
                    cols[3]: 'PROCESS_OWNER' if len(cols) > 3 else 'Unknown'
                }
        
        df_clean = df[list(col_mapping.keys())].rename(columns=col_mapping)
        
        if 'PROCESS_OWNER' not in df_clean.columns:
            df_clean['PROCESS_OWNER'] = 'Unknown'
        
        df_clean['PROCESS_CODE'] = df_clean['PROCESS_CODE'].astype(str).str.strip()
        df_clean = df_clean[df_clean['PROCESS_CODE'] != 'nan']
        df_clean = df_clean[df_clean['PROCESS_CODE'].str.len() > 0]

        # Enhanced parallelization detection
        parallel_keywords = [
            'MACH', 'CMM', 'JIG', 'PAINT', 'CUT', 'DRILL', 'MILL', 'TURN', 'GRIND',
            'WELD', 'ASSEM', 'PREP', 'FINISH', 'INSPECT', 'TEST', 'CLEAN', 'PACK'
        ]
        
        pattern = '|'.join(parallel_keywords)
        df_clean['CAN_PARALLELIZE'] = (
            df_clean['PROCESS_FULL_NAME'].str.contains(pattern, case=False, na=False) |
            df_clean['PROCESS_CODE'].str.contains(pattern, case=False, na=False)
        )
        
        sequential_only = ['MOULD', 'POUR', 'K_D', 'HEAT', 'CURE', 'DRY']
        sequential_pattern = '|'.join(sequential_only)
        mask = df_clean['PROCESS_FULL_NAME'].str.contains(sequential_pattern, case=False, na=False)
        df_clean.loc[mask, 'CAN_PARALLELIZE'] = False

        logging.info(f"✔ process_master rows: {len(df_clean)}")
        return df_clean[['PROCESS_CODE', 'PROCESS_FULL_NAME', 'PROCESS_OWNER', 'CAN_PARALLELIZE']]
        
    except Exception as exc:
        logging.error(f"read_process_master failed: {exc}")
        return pd.DataFrame()

def read_part_lead_time(path: Path) -> pd.DataFrame:
    """Extract routing data from 'Part Lead Time' sheet."""
    try:
        raw = pd.read_excel(path, sheet_name=cfg.SHEET_ROUTING, header=None)
        logging.info(f"Part Lead Time sheet shape: {raw.shape}")
        
        item_codes = {}
        header_row = 1
        
        if header_row < len(raw):
            item_row = raw.iloc[header_row]
            for col_idx, val in item_row.items():
                if pd.notna(val) and str(val).strip():
                    val_str = str(val).strip()
                    skip_terms = ['unnamed', 'any', 'per day', 'verity']
                    if not any(term in val_str.lower() for term in skip_terms) and len(val_str) > 2:
                        item_codes[col_idx] = val_str
        
        logging.info(f"Found item codes: {list(item_codes.values())[:10]}...")
        
        if not item_codes:
            logging.warning("No valid item codes found")
            return pd.DataFrame()
        
        process_start_row = None
        for idx in range(len(raw)):
            if pd.notna(raw.iloc[idx, 0]) and 'Sr. No.' in str(raw.iloc[idx, 0]):
                process_start_row = idx + 1
                break
        
        if process_start_row is None:
            process_start_row = 10
        
        records = []
        for row_idx in range(process_start_row, min(len(raw), process_start_row + 100)):
            row = raw.iloc[row_idx]
            
            process_name = None
            if len(row) > 1 and pd.notna(row.iloc[1]):
                process_name = str(row.iloc[1]).strip()
                if process_name and process_name.lower() not in ['nan', 'process']:
                    for col_idx, item_code in item_codes.items():
                        if col_idx < len(row):
                            lead_time_val = safe_float(row.iloc[col_idx])
                            if lead_time_val > 0:
                                records.append({
                                    "ITEM_CODE": item_code,
                                    "PROCESS_CODE": process_name,
                                    "LEAD_TIME_DAYS": lead_time_val
                                })
        
        df = pd.DataFrame(records)
        if not df.empty:
            df['SEQUENCE'] = df.groupby('ITEM_CODE').cumcount() + 1
        
        logging.info(f"✔ routing_table rows: {len(df)}")
        return df
        
    except Exception as exc:
        logging.error(f"read_part_lead_time failed: {exc}")
        return pd.DataFrame()

def read_orders(path: Path) -> pd.DataFrame:
    """Read orders from the Open Order sheet with improved error handling."""
    try:
        df = pd.read_excel(path, sheet_name=cfg.SHEET_OPEN_ORDER)
        logging.info(f"Orders sheet columns: {list(df.columns)}")
        
        column_mapping = {
            'Item Code': 'ITEM_CODE',
            'Customer Name': 'CUSTOMER_NAME',
            'PO Date': 'PO_DATE',
            'SO Creation Date': 'SO_CREATION_DATE',
            'Order Qty': 'QTY_ORDERED',
            'Dispatch Qty': 'QTY_DISPATCHED',
            'Open Qty': 'QTY_OPEN',
            'Order Status': 'ORDER_STATUS'
        }
        
        # ✅ Check if columns exist before mapping
        existing_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        
        if not existing_mapping:
            logging.warning("No matching order columns found, returning empty DataFrame")
            return pd.DataFrame()
            
        df = df.rename(columns=existing_mapping)
        
        date_cols = ['PO_DATE', 'SO_CREATION_DATE']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        qty_cols = ['QTY_ORDERED', 'QTY_DISPATCHED', 'QTY_OPEN']
        for col in qty_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if 'ITEM_CODE' in df.columns:
            df = df[df['ITEM_CODE'].apply(is_valid_code)]
        
        logging.info(f"✔ orders rows: {len(df)}")
        return df
        
    except Exception as exc:
        logging.error(f"read_orders failed: {exc}")
        return pd.DataFrame()

def jobs_from_wip_inventory(path: Path) -> pd.DataFrame:
    """Fully fixed job extractor from WIP sheets, ensuring all RFM records are included (status-based)."""
    today = pd.Timestamp.now()
    all_jobs: List[Dict] = []
    sheet_names = ['RFD', 'RFM', 'WIP MC', 'WIP RAW', 'WIP DM']

    for sheet_name in sheet_names:
        try:
            logging.info(f"📄 Processing sheet: {sheet_name}")
            df = pd.read_excel(path, sheet_name=sheet_name)
            jobs = []

            if sheet_name == 'RFD':
                for _, row in df.iloc[1:].iterrows():
                    item_code = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
                    if is_valid_code(item_code):
                        qty = safe_float(row.iloc[2]) if len(row) > 2 else 1
                        if qty == 0:
                            qty = 1
                        ton = safe_float(row.iloc[4]) if len(row) > 4 else 0
                        jobs.append({
                            'ITEM_CODE': item_code,
                            'STAGE': 'RFD',
                            'PROCESS': 'Ready for Dispatch',
                            'QTY': qty,
                            'TONNAGE': ton,
                            'DATE': today,
                            'AGING': None
                        })

            elif sheet_name == 'RFM':
                logging.info("🔎 Correcting RFM sheet header parsing...")

                first_row_idx = None
                for idx, row in df.iterrows():
                    if any(str(cell).strip().upper() == 'ITEM CODE' for cell in row):
                        first_row_idx = idx
                        break

                if first_row_idx is None:
                    logging.error("RFM sheet header row not found")
                    continue

                df = pd.read_excel(path, sheet_name=sheet_name, skiprows=first_row_idx + 1)
                df.columns = [str(c).strip().upper() for c in df.columns]

                item_col = next((col for col in df.columns if col in ['ITEM CODE', 'ITEM', 'PART CODE']), None)
                qty_col = next((col for col in df.columns if col.startswith('TOTAL')), None)
                ton_col = next((col for col in df.columns if 'TON' in col), None)
                machine_col = next((col for col in df.columns if 'MACHINE' in col), None)

                extracted = 0
                for _, row in df.iterrows():
                    item_code = str(row.get(item_col, "")).strip()
                    if not is_valid_code(item_code):
                        continue

                    qty = safe_float(row.get(qty_col)) or 1.0
                    ton = safe_float(row.get(ton_col)) or 0.0
                    machine = str(row.get(machine_col, "UNASSIGNED")).strip()

                    jobs.append({
                        "ITEM_CODE": item_code,
                        "STAGE": "RFM",
                        "PROCESS": f"Ready for Machining ({machine})",
                        "QTY": qty if qty else 1.0,
                        "TONNAGE": ton,
                        "DATE": today,
                        "AGING": None
                    })
                    extracted += 1

                logging.info(f"✅ Extracted {extracted} RFM jobs")

            elif sheet_name in ['WIP MC', 'WIP RAW']:
                if df.shape[0] < 3:
                    continue
                header = df.iloc[2]
                process_cols = [
                    (i, str(val).strip())
                    for i, val in enumerate(header)
                    if pd.notna(val) and str(val).strip() not in ['Item', 'Weight', 'Ton.', 'Grand Total']
                ]
                for i in range(3, len(df)):
                    row = df.iloc[i]
                    item_code = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
                    if is_valid_code(item_code):
                        for idx, proc in process_cols:
                            qty = safe_float(row.iloc[idx]) if idx < len(row) else 0
                            if qty > 0:
                                jobs.append({
                                    'ITEM_CODE': item_code,
                                    'STAGE': sheet_name.replace(" ", "_"),
                                    'PROCESS': proc,
                                    'QTY': qty,
                                    'TONNAGE': safe_float(row.iloc[-1]) if len(row) > 2 else 0,
                                    'DATE': today,
                                    'AGING': None
                                })

            elif sheet_name == 'WIP DM':
                for _, row in df.iterrows():
                    item_code = next((str(row.get(col)).strip() for col in ['Item Code', 'Item', 'MatchCode'] if col in df.columns and pd.notna(row.get(col))), None)
                    if is_valid_code(item_code):
                        qty = safe_float(row.get('Qty')) or 1
                        if qty == 0:
                            qty = 1
                        jobs.append({
                            'ITEM_CODE': item_code,
                            'STAGE': 'WIP_DM',
                            'PROCESS': str(row.get('Current Step', 'In Progress')),
                            'QTY': qty,
                            'TONNAGE': safe_float(row.get('Weight in Tonn', 0)),
                            'DATE': today,
                            'AGING': safe_float(row.get('Aging from Initiation Days', 0))
                        })

            all_jobs.extend(jobs)
            logging.info(f"✅ Extracted {len(jobs)} jobs from {sheet_name}")

        except Exception as e:
            logging.error(f"❌ Error processing {sheet_name}: {e}")

    df_jobs = pd.DataFrame(all_jobs)
    logging.info(f"\n🎯 Total inventory jobs extracted: {len(df_jobs)}")
    return df_jobs

# ✅ NEW: Optimized Parallelization Analysis Functions

def get_machine_usage(all_jobs: pd.DataFrame) -> Dict[str, List[str]]:
    """Get machine usage mapping for better parallelization analysis."""
    machine_usage = {}
    
    for _, job in all_jobs.iterrows():
        process = job['PROCESS']
        item_code = job['ITEM_CODE']
        
        # Extract machine from process name
        if 'Ready for Machining' in process:
            machine = process.split('(')[1].split(')')[0] if '(' in process else 'UNASSIGNED'
        else:
            machine = 'GENERAL'
        
        if machine not in machine_usage:
            machine_usage[machine] = []
        machine_usage[machine].append(item_code)
    
    return machine_usage

def calculate_realistic_lead_time(item_code: str, processes: List[str], routing_data: pd.DataFrame = None) -> float:
    """Calculate realistic lead time for an item based on its processes."""
    
    # Try to get from routing data first
    if routing_data is not None and not routing_data.empty:
        item_routing = routing_data[routing_data["ITEM_CODE"] == item_code]
        if not item_routing.empty:
            return item_routing["LEAD_TIME_DAYS"].sum()
    
    # Fallback: Calculate based on process complexity
    base_time = 2.0  # 2 days minimum
    process_time = len(processes) * 1.5  # 1.5 days per process
    
    # Add complexity based on process types
    complexity_multiplier = 1.0
    for process in processes:
        process_upper = process.upper()
        if any(keyword in process_upper for keyword in ['MACH', 'MILL', 'TURN', 'GRIND']):
            complexity_multiplier += 0.3
        elif any(keyword in process_upper for keyword in ['ASSEM', 'WELD', 'PAINT']):
            complexity_multiplier += 0.2
        elif any(keyword in process_upper for keyword in ['INSPECT', 'TEST', 'CMM']):
            complexity_multiplier += 0.1
    
    return round(base_time + (process_time * complexity_multiplier), 1)

def analyze_optimized_parallelization(all_jobs: pd.DataFrame, process_master: pd.DataFrame, routing_data: pd.DataFrame = None) -> pd.DataFrame:
    """Advanced parallelization analysis with machine constraints and optimization."""
    
    logging.info("Starting optimized parallelization analysis...")
    
    # Get parallelizable items by priority
    critical_items = all_jobs[
        (all_jobs["CAN_PARALLELIZE"] == True) & 
        (all_jobs["URGENCY"] == "CRITICAL")
    ]["ITEM_CODE"].unique()
    
    high_items = all_jobs[
        (all_jobs["CAN_PARALLELIZE"] == True) & 
        (all_jobs["URGENCY"] == "HIGH")
    ]["ITEM_CODE"].unique()
    
    # Limit analysis for performance - but ensure we have enough items
    prioritized_items = list(critical_items)[:20] + list(high_items)[:20]
    
    # If we don't have enough high priority items, add some medium ones
    if len(prioritized_items) < 10:
        medium_items = all_jobs[
            (all_jobs["CAN_PARALLELIZE"] == True) & 
            (all_jobs["URGENCY"] == "MEDIUM")
        ]["ITEM_CODE"].unique()
        prioritized_items.extend(list(medium_items)[:15])
    
    logging.info(f"Analyzing {len(prioritized_items)} high-priority parallelizable items")
    
    if len(prioritized_items) < 2:
        logging.warning("Not enough items for parallelization analysis")
        return pd.DataFrame()
    
    optimal_pairs = []
    
    for i, item1 in enumerate(prioritized_items):
        for item2 in prioritized_items[i+1:]:
            
            # Get job details for both items
            item1_jobs = all_jobs[all_jobs["ITEM_CODE"] == item1]
            item2_jobs = all_jobs[all_jobs["ITEM_CODE"] == item2]
            
            if item1_jobs.empty or item2_jobs.empty:
                continue
            
            # Get processes and machines
            item1_processes = list(set(item1_jobs["PROCESS"].tolist()))
            item2_processes = list(set(item2_jobs["PROCESS"].tolist()))
            
            # Check for machine conflicts
            item1_machines = set()
            item2_machines = set()
            
            for process in item1_processes:
                if '(' in process and ')' in process:
                    machine = process.split('(')[1].split(')')[0]
                    item1_machines.add(machine)
                else:
                    item1_machines.add("GENERAL")
            
            for process in item2_processes:
                if '(' in process and ')' in process:
                    machine = process.split('(')[1].split(')')[0]
                    item2_machines.add(machine)
                else:
                    item2_machines.add("GENERAL")
            
            # Check conflicts
            process_conflicts = set(item1_processes) & set(item2_processes)
            machine_conflicts = item1_machines & item2_machines
            
            # Remove "GENERAL" from machine conflicts if both have it
            if "GENERAL" in machine_conflicts and len(machine_conflicts) == 1:
                machine_conflicts = set()
            
            can_be_parallel = len(process_conflicts) == 0 and len(machine_conflicts) == 0
            
            # ✅ FIX: Calculate time savings using realistic lead times
            item1_lead_time = calculate_realistic_lead_time(item1, item1_processes, routing_data)
            item2_lead_time = calculate_realistic_lead_time(item2, item2_processes, routing_data)
            
            # Ensure minimum realistic times
            if item1_lead_time <= 0:
                item1_lead_time = 5.0  # 5 days minimum
            if item2_lead_time <= 0:
                item2_lead_time = 5.0  # 5 days minimum
            
            sequential_time = item1_lead_time + item2_lead_time
            parallel_time = max(item1_lead_time, item2_lead_time)
            time_saved = sequential_time - parallel_time
            
            # Get urgency scores
            item1_urgency = item1_jobs["URGENCY"].iloc[0] if not item1_jobs.empty else "LOW"
            item2_urgency = item2_jobs["URGENCY"].iloc[0] if not item2_jobs.empty else "LOW"
            
            optimal_pairs.append({
                "ITEM_1": item1,
                "ITEM_2": item2,
                "ITEM_1_URGENCY": item1_urgency,
                "ITEM_2_URGENCY": item2_urgency,
                "ITEM_1_PROCESSES": ", ".join(item1_processes),
                "ITEM_2_PROCESSES": ", ".join(item2_processes),
                "ITEM_1_MACHINES": ", ".join(item1_machines) if item1_machines else "GENERAL",
                "ITEM_2_MACHINES": ", ".join(item2_machines) if item2_machines else "GENERAL",
                "PROCESS_CONFLICTS": ", ".join(process_conflicts) if process_conflicts else "",
                "MACHINE_CONFLICTS": ", ".join(machine_conflicts) if machine_conflicts else "",
                "CAN_RUN_PARALLEL": can_be_parallel,
                "SEQUENTIAL_TIME_DAYS": round(sequential_time, 1),
                "PARALLEL_TIME_DAYS": round(parallel_time, 1),
                "TIME_SAVED_DAYS": round(time_saved, 1),
                "EFFICIENCY_GAIN_PCT": round((time_saved / sequential_time * 100) if sequential_time > 0 else 0, 2)
            })
    
    df_result = pd.DataFrame(optimal_pairs)
    
    # Sort by efficiency gain and parallelizability
    if not df_result.empty:
        df_result = df_result.sort_values([
            "CAN_RUN_PARALLEL",
            "EFFICIENCY_GAIN_PCT",
            "TIME_SAVED_DAYS"
        ], ascending=[False, False, False])
        
        # Log insights
        parallelizable_pairs = df_result[df_result["CAN_RUN_PARALLEL"] == True]
        logging.info(f"Generated {len(df_result)} total pairs, {len(parallelizable_pairs)} can run in parallel")
        
        if not parallelizable_pairs.empty:
            avg_savings = parallelizable_pairs["TIME_SAVED_DAYS"].mean()
            total_savings = parallelizable_pairs["TIME_SAVED_DAYS"].sum()
            avg_efficiency = parallelizable_pairs["EFFICIENCY_GAIN_PCT"].mean()
            logging.info(f"Average time savings: {avg_savings:.1f} days")
            logging.info(f"Total potential savings: {total_savings:.1f} days")
            logging.info(f"Average efficiency gain: {avg_efficiency:.1f}%")
    
    return df_result

def create_all_jobs_master(routing: pd.DataFrame, jobs: pd.DataFrame, orders: pd.DataFrame, process_master: pd.DataFrame = None) -> Tuple[pd.DataFrame, Dict]:
    """Build the all-jobs master table with urgency scoring and parallelization info."""
    logging.info("Creating all_jobs_master …")
    today = pd.Timestamp.now()
    
    def calculate_urgency(stage: str, aging: float, qty_open: float, lead_time: float) -> str:
        if stage in ("RFD", "RFM"):
            return "CRITICAL"
        if stage in ("WIP_MC", "WIP_RAW") and aging and aging > 30:
            return "HIGH"
        if qty_open > 0 and lead_time > 60:
            return "HIGH"
        if stage == "WIP_DM":
            return "MEDIUM"
        if qty_open > 0:
            return "MEDIUM"
        return "LOW"
    
    rows: List[Dict] = []
    
    if jobs.empty:
        logging.warning("Jobs DataFrame is empty")
        return pd.DataFrame(), {}
    
    for _, job in jobs.iterrows():
        item = job["ITEM_CODE"]
        stage = job["STAGE"]
        process_name = job.get("PROCESS", "")
        
        route = routing[routing["ITEM_CODE"] == item]
        lead = route["LEAD_TIME_DAYS"].sum() if not route.empty else 0
        
        parallel_cnt = 0
        if not route.empty:
            parallel_cnt = route["PROCESS_CODE"].str.contains(r"MACH|CMM|JIG|PAINT", case=False, na=False).sum()
        
        # ✅ Fixed: Determine if this specific job's process can be parallelized
        can_parallelize = False
        if process_master is not None and not process_master.empty:
            try:
                # Safe process matching with regex=False to avoid warnings
                process_first_word = process_name.split()[0] if process_name.split() else ""
                process_matches = process_master[
                    (process_master["PROCESS_CODE"].str.contains(process_first_word, case=False, na=False, regex=False)) |
                    (process_master["PROCESS_FULL_NAME"].str.contains(process_name, case=False, na=False, regex=False))
                ]
                if not process_matches.empty:
                    can_parallelize = process_matches.iloc[0]["CAN_PARALLELIZE"]
                else:
                    # Fallback: Check if process name contains parallelizable keywords
                    parallel_keywords = ['MACH', 'CMM', 'JIG', 'PAINT', 'CUT', 'DRILL', 'MILL', 'TURN', 'GRIND',
                                       'WELD', 'ASSEM', 'PREP', 'FINISH', 'INSPECT', 'TEST', 'CLEAN', 'PACK']
                    can_parallelize = any(keyword in process_name.upper() for keyword in parallel_keywords)
            except Exception as e:
                logging.warning(f"Process matching failed for {process_name}: {e}")
                can_parallelize = False
        
        open_qty = 0
        if not orders.empty and "ITEM_CODE" in orders.columns:
            matching_orders = orders[orders["ITEM_CODE"] == item]
            if not matching_orders.empty:
                open_qty = matching_orders["QTY_OPEN"].sum()
        
        # Use calculated lead time or fallback to realistic estimate
        if lead <= 0:
            lead = calculate_realistic_lead_time(item, [process_name], routing)
        
        rows.append({
            "ITEM_CODE": item,
            "STAGE": stage,
            "PROCESS": job["PROCESS"],
            "QUANTITY": job["QTY"],
            "LEAD_TIME_ESTIMATE": lead,
            "PARALLEL_STEPS": parallel_cnt,
            "CAN_PARALLELIZE": can_parallelize,
            "OPEN_QTY": open_qty,
            "URGENCY": calculate_urgency(stage, job.get("AGING", 0), open_qty, lead),
            "START_DATE": job["DATE"].strftime('%Y-%m-%d'),
            "EST_COMPLETION": (job["DATE"] + pd.Timedelta(days=lead)).strftime('%Y-%m-%d') if lead else '',
        })
    
    all_jobs = pd.DataFrame(rows)
    
    if all_jobs.empty:
        logging.warning("No jobs created for all_jobs_master")
        return all_jobs, {}
    
    urgency_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    all_jobs["urgency_sort"] = all_jobs["URGENCY"].map(urgency_order)
    all_jobs = all_jobs.sort_values(["urgency_sort", "LEAD_TIME_ESTIMATE"], ascending=[True, False])
    all_jobs.drop(columns="urgency_sort", inplace=True)
    
    all_jobs.to_csv("all_jobs_master.csv", index=False)
    logging.info(f"all_jobs_master.csv written ({len(all_jobs)} rows)")
    
    # ✅ MOVED: Generate optimized parallelization analysis AFTER all_jobs is complete
    if process_master is not None and not all_jobs.empty:
        try:
            logging.info("Generating optimized parallelization analysis...")
            optimized_parallel_analysis = analyze_optimized_parallelization(all_jobs, process_master, routing)
            optimized_parallel_analysis.to_csv("optimized_parallelization.csv", index=False)
            logging.info(f"optimized_parallelization.csv written ({len(optimized_parallel_analysis)} pairs)")
            
            # Log some insights
            if not optimized_parallel_analysis.empty:
                parallelizable_pairs = optimized_parallel_analysis[optimized_parallel_analysis["CAN_RUN_PARALLEL"] == True]
                logging.info(f"Found {len(parallelizable_pairs)} parallelizable pairs out of {len(optimized_parallel_analysis)} total combinations")
                
                if not parallelizable_pairs.empty:
                    total_savings = parallelizable_pairs["TIME_SAVED_DAYS"].sum()
                    avg_efficiency = parallelizable_pairs["EFFICIENCY_GAIN_PCT"].mean()
                    logging.info(f"🎯 Total potential time savings: {total_savings:.1f} days")
                    logging.info(f"🎯 Average efficiency gain: {avg_efficiency:.1f}%")
        except Exception as e:
            logging.error(f"Parallelization analysis failed: {e}")
    
    # ✅ Log parallelizable jobs count
    parallelizable_count = all_jobs[all_jobs["CAN_PARALLELIZE"] == True].shape[0]
    logging.info(f"✅ Parallelizable jobs: {parallelizable_count}")
    
    stage_counts = all_jobs['STAGE'].value_counts()
    logging.info(f"\n✅ FINAL STAGE DISTRIBUTION:")
    for stage, count in stage_counts.items():
        logging.info(f"  {stage}: {count} jobs")
    
    summary = {
        "total": len(all_jobs),
        "by_urgency": all_jobs["URGENCY"].value_counts().to_dict(),
        "by_stage": all_jobs["STAGE"].value_counts().to_dict(),
        "parallelizable_count": parallelizable_count,
    }
    return all_jobs, summary

# ────────────────────────────── Orchestrator ─────────────────────────── #

def run_etl(master_file: str | Path = cfg.master_file, inventory_file: str | Path = cfg.inventory_file) -> Dict:
    """Main ETL function - CSV generation with optimized parallelization analysis."""
    logging.info("🏁 ETL started (CSV only)")
    master_file = Path(master_file)
    inventory_file = Path(inventory_file)
    
    if not master_file.exists() or not inventory_file.exists():
        raise FileNotFoundError(f"Files not found: {master_file}, {inventory_file}")
    
    # Extract all data
    process_master = read_process_master(master_file)
    routing_table = read_part_lead_time(master_file)
    orders = read_orders(master_file)
    jobs = jobs_from_wip_inventory(inventory_file)
    
    # ✅ Pass process_master to include parallelization info
    all_jobs, summary = create_all_jobs_master(routing_table, jobs, orders, process_master)
    
    # Save all CSV files
    process_master.to_csv("process_master.csv", index=False)
    routing_table.to_csv("routing_table.csv", index=False)
    orders.to_csv("order_fact.csv", index=False)
    jobs.to_csv("jobs_fact.csv", index=False)
    
    logging.info("✅ ETL completed - All CSV files generated")
    
    # Check for optimized parallelization file
    optimized_file_exists = os.path.exists("optimized_parallelization.csv")
    
    files_created = [
        "process_master.csv",
        "routing_table.csv", 
        "order_fact.csv",
        "jobs_fact.csv",
        "all_jobs_master.csv"
    ]
    
    if optimized_file_exists:
        files_created.append("optimized_parallelization.csv")
    
    logging.info(f"Files created:")
    logging.info(f"  - process_master.csv ({len(process_master)} rows)")
    logging.info(f"  - routing_table.csv ({len(routing_table)} rows)")
    logging.info(f"  - order_fact.csv ({len(orders)} rows)")
    logging.info(f"  - jobs_fact.csv ({len(jobs)} rows)")
    logging.info(f"  - all_jobs_master.csv ({len(all_jobs)} rows)")
    
    if optimized_file_exists:
        try:
            parallel_df = pd.read_csv("optimized_parallelization.csv")
            logging.info(f"  - optimized_parallelization.csv ({len(parallel_df)} pairs)")
            
            # Log optimization insights
            if not parallel_df.empty:
                parallelizable_pairs = parallel_df[parallel_df["CAN_RUN_PARALLEL"] == True]
                if not parallelizable_pairs.empty:
                    total_time_saved = parallelizable_pairs["TIME_SAVED_DAYS"].sum()
                    avg_efficiency = parallelizable_pairs["EFFICIENCY_GAIN_PCT"].mean()
                    max_efficiency = parallelizable_pairs["EFFICIENCY_GAIN_PCT"].max()
                    
                    logging.info(f"🎯 Parallelization Insights:")
                    logging.info(f"  - Parallelizable pairs: {len(parallelizable_pairs)}")
                    logging.info(f"  - Total time savings potential: {total_time_saved:.1f} days")
                    logging.info(f"  - Average efficiency gain: {avg_efficiency:.1f}%")
                    logging.info(f"  - Maximum efficiency gain: {max_efficiency:.1f}%")
        except Exception as e:
            logging.error(f"Error reading parallelization results: {e}")
    
    return {
        "processes": len(process_master),
        "routing": len(routing_table),
        "orders": len(orders), 
        "jobs": len(jobs),
        "all_jobs": len(all_jobs),
        "files_created": files_created,
        "optimization_enabled": optimized_file_exists,
        "summary": summary,
    }

if __name__ == "__main__":
    import os
    
    current_dir = Path(".")
    excel_files = list(current_dir.glob("*.xlsx"))
    print(f"Excel files found: {[f.name for f in excel_files]}")
    
    master_file = None
    inventory_file = None
    
    for file in excel_files:
        filename = file.name.lower()
        if 'master' in filename:
            master_file = file
        elif 'stock' in filename or 'live' in filename:
            inventory_file = file
    
    if master_file and inventory_file:
        print(f"✅ Using Master file: {master_file.name}")
        print(f"✅ Using Inventory file: {inventory_file.name}")
        
        try:
            stats = run_etl(master_file, inventory_file)
            logging.info(f"ETL summary: {stats}")
            print("\n🎉 ETL completed successfully!")
            print(f"Files generated: {stats['files_created']}")
        except Exception as e:
            logging.error(f"ETL failed: {e}")
            print(f"❌ ETL failed: {e}")
    else:
        print("❌ Could not auto-detect required files!")
        print(f"Found files: {[f.name for f in excel_files]}")
        print("\nPlease ensure you have:")
        print("  - A file with 'master' in the name (Master.xlsx)")
        print("  - A file with 'stock' or 'live' in the name (Live-Stock-*.xlsx)")
