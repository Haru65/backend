from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import shutil
import tempfile
import pandas as pd
import numpy as np
from datetime import datetime

from etl import run_etl

app = FastAPI(
    title="Manufacturing Analytics Platform", 
    description="📦 Complete ETL + Business Intelligence", 
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def clean_dataframe_for_json(df):
    """Clean DataFrame for JSON serialization"""
    df = df.replace([np.nan, np.inf, -np.inf], None)
    for col in df.columns:
        if df[col].dtype == 'float64':
            df[col] = df[col].fillna(0)
        elif df[col].dtype == 'object':
            df[col] = df[col].fillna('')
    return df

@app.post("/run_etl/")
async def run_etl_api(
    master_file: UploadFile = File(...),
    inventory_file: UploadFile = File(...)
):
    """ETL endpoint that always works"""
    temp_dir = tempfile.mkdtemp()

    try:
        # Save uploaded files
        master_path = os.path.join(temp_dir, "Master.xlsx")
        inventory_path = os.path.join(temp_dir, "live_stock.xlsx")

        with open(master_path, "wb") as f:
            shutil.copyfileobj(master_file.file, f)
        with open(inventory_path, "wb") as f:
            shutil.copyfileobj(inventory_file.file, f)

        # Run ETL
        result = run_etl(master_path, inventory_path)

        return {
            "status": "✅ success",
            "message": "ETL completed successfully",
            "files": result["files_created"],
            "inserted_rows": {
                "processes": result["processes"],
                "routing": result["routing"],
                "orders": result["orders"],
                "jobs": result["jobs"],
                "all_jobs": result["all_jobs"]
            }
        }

    except Exception as e:
        return JSONResponse(
            content={"status": "❌ error", "message": f"ETL failed: {str(e)}"},
            status_code=500
        )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

@app.get("/dashboard-summary")
def get_dashboard_summary():
    """Dashboard summary that always works"""
    try:
        summary = {}
        
        # Always try to provide data from existing CSV files
        if os.path.exists("all_jobs_master.csv"):
            jobs_df = pd.read_csv("all_jobs_master.csv")
            jobs_df = clean_dataframe_for_json(jobs_df)
            
            summary["jobs"] = {
                "total": len(jobs_df),
                "by_stage": jobs_df["STAGE"].value_counts().to_dict(),
                "by_urgency": jobs_df["URGENCY"].value_counts().to_dict(),
                "all_jobs": jobs_df.to_dict(orient="records")
            }
        else:
            summary["jobs"] = {
                "total": 0,
                "by_stage": {},
                "by_urgency": {},
                "all_jobs": []
            }

        # Lead time summary
        if os.path.exists("lead_time_analysis.csv"):
            lead_df = pd.read_csv("lead_time_analysis.csv")
            lead_df = clean_dataframe_for_json(lead_df)
            summary["lead_time"] = {
                "total_items": len(lead_df),
                "avg_serial_time": float(lead_df["LEAD_TIME_SERIAL"].mean()),
                "avg_parallel_time": float(lead_df["LEAD_TIME_PARALLELIZED"].mean()),
                "total_time_savings": float(lead_df["TIME_SAVED_DAYS"].sum()),
                "avg_efficiency_gain": float(lead_df["EFFICIENCY_GAIN_PCT"].mean()),
            }
        else:
            summary["lead_time"] = {
                "total_items": 0,
                "avg_serial_time": 0,
                "avg_parallel_time": 0,
                "total_time_savings": 0,
                "avg_efficiency_gain": 0,
            }

        # Stock summary
        if os.path.exists("stock_vs_demand.csv"):
            stock_df = pd.read_csv("stock_vs_demand.csv")
            stock_df = clean_dataframe_for_json(stock_df)
            summary["stock"] = {
                "out_of_stock": len(stock_df[stock_df["STOCK_ADEQUACY"] == "Out of Stock"]),
                "shortage": len(stock_df[stock_df["STOCK_ADEQUACY"] == "Shortage"]),
                "adequate": len(stock_df[stock_df["STOCK_ADEQUACY"] == "Adequate"]),
                "excess": len(stock_df[stock_df["STOCK_ADEQUACY"] == "Excess"]),
            }
        else:
            summary["stock"] = {
                "out_of_stock": 0,
                "shortage": 0,
                "adequate": 0,
                "excess": 0,
            }

        return {
            "status": "success",
            "summary": summary,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": f"Dashboard summary failed: {str(e)}"},
            status_code=500
        )

@app.get("/lead-time")
def get_lead_time():
    """Lead time data that always works"""
    try:
        if os.path.exists("lead_time_analysis.csv"):
            df = pd.read_csv("lead_time_analysis.csv")
            df = clean_dataframe_for_json(df)
            return {
                "status": "success",
                "data": df.to_dict(orient="records"),
                "total_items": len(df)
            }
        else:
            return {
                "status": "success",
                "data": [],
                "total_items": 0
            }
    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )

@app.get("/stock-vs-demand")
def get_stock_vs_demand():
    """Stock vs demand data that always works"""
    try:
        if os.path.exists("stock_vs_demand.csv"):
            df = pd.read_csv("stock_vs_demand.csv")
            df = clean_dataframe_for_json(df)
            return {
                "status": "success",
                "data": df.to_dict(orient="records"),
                "total_items": len(df)
            }
        else:
            return {
                "status": "success",
                "data": [],
                "total_items": 0
            }
    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )

@app.get("/optimized-parallelization")
def get_optimized_parallelization():
    """Parallelization data that always works"""
    try:
        if os.path.exists("optimized_parallelization.csv"):
            df = pd.read_csv("optimized_parallelization.csv")
            df = clean_dataframe_for_json(df)
            
            parallelizable_pairs = df[df["CAN_RUN_PARALLEL"] == True].sort_values(
                ["EFFICIENCY_GAIN_PCT", "TIME_SAVED_DAYS"], 
                ascending=[False, False]
            )
            
            return {
                "status": "success",
                "data": parallelizable_pairs.to_dict(orient="records"),
                "summary": {
                    "total_pairs_analyzed": len(df),
                    "parallelizable_pairs": len(parallelizable_pairs),
                    "total_time_savings": float(parallelizable_pairs["TIME_SAVED_DAYS"].sum()) if len(parallelizable_pairs) > 0 else 0.0,
                    "avg_efficiency_gain": float(parallelizable_pairs["EFFICIENCY_GAIN_PCT"].mean()) if len(parallelizable_pairs) > 0 else 0.0,
                    "top_efficiency_gain": float(parallelizable_pairs["EFFICIENCY_GAIN_PCT"].max()) if len(parallelizable_pairs) > 0 else 0.0
                }
            }
        else:
            return {
                "status": "success",
                "data": [],
                "summary": {
                    "total_pairs_analyzed": 0,
                    "parallelizable_pairs": 0,
                    "total_time_savings": 0.0,
                    "avg_efficiency_gain": 0.0,
                    "top_efficiency_gain": 0.0
                }
            }
    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )

@app.get("/analysis_status/")
def get_analysis_status():
    """Check status of files"""
    files = [
        "process_master.csv", "routing_table.csv", "order_fact.csv", 
        "jobs_fact.csv", "all_jobs_master.csv", "lead_time_analysis.csv",
        "stock_vs_demand.csv", "optimized_parallelization.csv"
    ]
    
    file_status = {}
    existing_files = 0
    
    for file in files:
        if os.path.exists(file):
            stat = os.stat(file)
            file_status[file] = {
                "exists": True,
                "size_bytes": stat.st_size,
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            }
            existing_files += 1
        else:
            file_status[file] = {"exists": False}
    
    return {
        "status": "✅ success",
        "file_status": file_status,
        "completion_stats": {
            "etl": {
                "completed_files": existing_files,
                "total_files": len(files),
                "completion_percentage": round((existing_files / len(files)) * 100, 1)
            }
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/")
def root():
    """API Root"""
    return {
        "message": "🏭 Manufacturing Analytics Platform API",
        "version": "2.0.0",
        "status": "operational"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
