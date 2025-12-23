import os
import json
import shutil
import base64
import time
import csv
import uuid
import threading
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from io import BytesIO
from typing import Dict, Optional, List

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from groq import Groq
from pdf2image import convert_from_path

# Optional: faster/accurate page count if available
try:
    from pdf2image import pdfinfo_from_path
except Exception:
    pdfinfo_from_path = None


# -----------------------------
# Config
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent
JOBS_DIR = BASE_DIR / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)

GROQ_API_KEY = ""
if not GROQ_API_KEY:
    print("⚠️  GROQ_API_KEY not set. Set it as an environment variable before running.")
client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


# -----------------------------
# Job state
# -----------------------------
@dataclass
class JobState:
    job_id: str
    status: str = "queued"  # queued|running|done|error|cancelled
    progress_pct: int = 0
    message: str = "Queued"
    error: Optional[str] = None
    cancel_requested: bool = False
    log: List[str] = field(default_factory=list)

    total_pages: int = 0
    processed_pages: int = 0

    # paths
    job_dir: Path = None
    input_dir: Path = None
    output_dir: Path = None
    csv_path: Optional[Path] = None

    # zips
    zip_all: Optional[Path] = None
    zip_receipts_unstamped: Optional[Path] = None
    zip_receipts_stamped: Optional[Path] = None
    zip_credit_notes: Optional[Path] = None


JOBS: Dict[str, JobState] = {}
LOCK = threading.Lock()


def job_log(job: JobState, line: str):
    job.log.append(line)
    # keep log from growing infinitely
    if len(job.log) > 4000:
        job.log = job.log[-2000:]


def safe_pct(x: float) -> int:
    if x < 0:
        return 0
    if x > 100:
        return 100
    return int(x)


# -----------------------------
# Groq analysis (your logic)
# -----------------------------
def get_groq_analysis(image_bytes: bytes, model_name: str) -> Optional[dict]:
    """Universal stamp detection and strict classification logic."""
    if client is None:
        raise RuntimeError("GROQ_API_KEY not set or Groq client not initialized.")

    base64_image = base64.b64encode(image_bytes).decode("utf-8")

    prompt = """
CRITICAL OCR TASK: Extract all text.
You must follow these strict categorization rules to separate Receipts from Credit Notes.

1. SEARCH FOR KEYWORDS (Literal Text):
- Inclusion Phrases (RECEIPT markers): ["Barcode", "TAX INVOICE", "RECEIPT", "Total Amount in Words", "PURCHASE RETURN VOUCHER", "TAX CREDIT NOTE", "PURCHASE RETURN"]
- Exclusion Phrases (REPORT/NON-RECEIPT markers): ["DAILY FIELD ACTIVITY REPORT", "Number of Invoices", "Transfer", "Month Target"]

2. STAMP DETECTION (Visual):
- Scan for manual rubber stamps/seals in BLUE, RED, or BLACK.
- These marks are often circular or rectangular and look "stamped on" (wet ink).
- Identify words inside stamps like "RECEIVED", "PAID", "POSTED", or "GOODS RECEIVED".

3. FILING LOGIC (Follow strictly):
- RULE 1: IF "TAX CREDIT NOTE" or "PURCHASE RETURN" is found, set "is_receipt": true. (These MUST be treated as receipts).
- RULE 2: IF the word "Barcode" (text) is found, set "is_receipt": true.
- RULE 3: IF "TAX INVOICE" or "RECEIPT" is the header, set "is_receipt": true.
- RULE 4: ONLY set "is_receipt": false IF none of the above rules match AND an 'Exclusion Phrase' is found.

Return ONLY a JSON object:
{
  "is_receipt": true/false,
  "has_stamp": true/false,
  "found_inclusion_keywords": [],
  "found_exclusion_keywords": [],
  "detected_stamp_details": "color and text of stamp",
  "document_data": { "ALL_FIELDS": "..." }
}
"""

    try:
        completion = client.chat.completions.create(
            model=model_name,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url",
                     "image_url": {"url": f"data:image/png;base64,{base64_image}"}}
                ],
            }],
            response_format={"type": "json_object"},
        )
        return json.loads(completion.choices[0].message.content)
    except Exception as e:
        return {"__error__": str(e)}


# -----------------------------
# Processing pipeline (based on your code)
# -----------------------------
def zip_folder(src_dir: Path, zip_path: Path):
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if not src_dir.exists():
            return
        for p in src_dir.rglob("*"):
            if p.is_file():
                zf.write(p, arcname=p.relative_to(src_dir))


def zip_filtered_output(output_root: Path, zip_path: Path, filter_fn):
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if not output_root.exists():
            return
        for p in output_root.rglob("*"):
            if p.is_file() and filter_fn(p):
                zf.write(p, arcname=p.relative_to(output_root))


def process_job(job: JobState, dpi: int, sleep_sec: float, model_name: str):
    try:
        job.status = "running"
        job.message = "Starting…"
        job.progress_pct = 1
        job_log(job, f"Job {job.job_id} started. DPI={dpi}, sleep={sleep_sec}, model={model_name}")

        all_rows = []
        input_pdfs = sorted(job.input_dir.glob("*.pdf"))
        if not input_pdfs:
            raise RuntimeError("No PDFs found in upload.")

        # attempt pre-count pages for progress
        total_pages_est = 0
        if pdfinfo_from_path is not None:
            for pdf in input_pdfs:
                try:
                    info = pdfinfo_from_path(str(pdf))
                    total_pages_est += int(info.get("Pages", 0))
                except Exception:
                    pass

        if total_pages_est > 0:
            job.total_pages = total_pages_est
        else:
            job.total_pages = 0  # will increase as we process

        for pdf_file in input_pdfs:
            if job.cancel_requested:
                job.status = "cancelled"
                job.message = "Cancelled"
                job_log(job, "Cancel requested. Stopping.")
                return

            job_log(job, f"Processing: {pdf_file.name}")

            pdf_output_dir = job.output_dir / pdf_file.stem

            # Folders (strict)
            unstamped_receipt_dir = pdf_output_dir / "receipts" / "unstamped"
            stamped_receipt_dir = pdf_output_dir / "receipts" / "stamped"
            credit_dir = pdf_output_dir / "credit_notes"

            unstamped_receipt_dir.mkdir(parents=True, exist_ok=True)
            stamped_receipt_dir.mkdir(parents=True, exist_ok=True)
            credit_dir.mkdir(parents=True, exist_ok=True)

            # Convert PDF → PIL images
            pages = convert_from_path(str(pdf_file), dpi=dpi)

            # if we couldn't pre-count, expand total now
            if job.total_pages == 0:
                job.total_pages += len(pages)

            for i, page in enumerate(pages):
                if job.cancel_requested:
                    job.status = "cancelled"
                    job.message = "Cancelled"
                    job_log(job, "Cancel requested mid-file. Stopping.")
                    return

                page_num = i + 1
                job.message = f"{pdf_file.name}: page {page_num}/{len(pages)}"
                # progress
                if job.total_pages > 0:
                    job.progress_pct = safe_pct((job.processed_pages / job.total_pages) * 100)

                img_buf = BytesIO()
                page.save(img_buf, format="PNG")

                analysis = get_groq_analysis(img_buf.getvalue(), model_name=model_name)
                if analysis is None:
                    job_log(job, f"❌ Page {page_num}: analysis returned None, skipped.")
                    continue

                if "__error__" in analysis:
                    job_log(job, f"⚠️ Page {page_num}: Groq error: {analysis['__error__']}")
                    # keep going, but count it as processed
                    job.processed_pages += 1
                    continue

                is_receipt = bool(analysis.get("is_receipt", False))
                has_stamp = bool(analysis.get("has_stamp", False))
                img_filename = f"page_{page_num}.png"

                # ✅ FIXED: your stamped/unstamped status was swapped
                if is_receipt:
                    if has_stamp:
                        target_path = stamped_receipt_dir / img_filename
                        status = "STAMPED_RECEIPT"
                    else:
                        target_path = unstamped_receipt_dir / img_filename
                        status = "UNSTAMPED_RECEIPT"
                else:
                    target_path = credit_dir / img_filename
                    status = "CREDIT_NOTE"

                page.save(str(target_path))
                job_log(job, f"  -> Filed page {page_num} as {status}")

                all_rows.append({
                    "source_pdf": pdf_file.name,
                    "page": page_num,
                    "status": status,
                    "is_receipt": is_receipt,
                    "has_stamp": has_stamp,
                    "stamp_details": analysis.get("detected_stamp_details", ""),
                    "document_data": json.dumps(analysis.get("document_data", {}), ensure_ascii=False),
                })

                job.processed_pages += 1
                if job.total_pages > 0:
                    job.progress_pct = safe_pct((job.processed_pages / job.total_pages) * 100)

                # throttle
                if sleep_sec > 0:
                    time.sleep(sleep_sec)

            # Move original PDF into its output folder (like your script)
            shutil.move(str(pdf_file), str(pdf_output_dir / pdf_file.name))
            job_log(job, f"✅ Completed: {pdf_file.name}")

        # Write CSV
        if all_rows:
            job.csv_path = job.job_dir / "classification_log.csv"
            headers = ["source_pdf", "page", "status", "is_receipt", "has_stamp", "stamp_details", "document_data"]
            with open(job.csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(all_rows)
            job_log(job, f"📊 CSV saved: {job.csv_path.name}")

        # Create ZIPs for download
        zips_dir = job.job_dir / "zips"
        zips_dir.mkdir(parents=True, exist_ok=True)

        job.zip_all = zips_dir / "all_output.zip"
        zip_folder(job.output_dir, job.zip_all)

        # Filter zips by folder path
        job.zip_receipts_unstamped = zips_dir / "receipts_unstamped.zip"
        job.zip_receipts_stamped = zips_dir / "receipts_stamped.zip"
        job.zip_credit_notes = zips_dir / "credit_notes.zip"

        def is_unstamped(p: Path) -> bool:
            return "receipts" in p.parts and "unstamped" in p.parts

        def is_stamped(p: Path) -> bool:
            return "receipts" in p.parts and "stamped" in p.parts

        def is_credit(p: Path) -> bool:
            return "credit_notes" in p.parts

        zip_filtered_output(job.output_dir, job.zip_receipts_unstamped, is_unstamped)
        zip_filtered_output(job.output_dir, job.zip_receipts_stamped, is_stamped)
        zip_filtered_output(job.output_dir, job.zip_credit_notes, is_credit)

        job.progress_pct = 100
        job.status = "done"
        job.message = "Complete"
        job_log(job, "✅ Job finished successfully.")

    except Exception as e:
        job.status = "error"
        job.error = str(e)
        job.message = "Error"
        job_log(job, f"❌ Job error: {job.error}")
        job.progress_pct = safe_pct(job.progress_pct)


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="Receipt/Credit Note Classifier")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/process")
async def start_process(
    pdfs: List[UploadFile] = File(...),
    dpi: int = Form(150),
    sleep_sec: float = Form(3),
    model: str = Form("meta-llama/llama-4-scout-17b-16e-instruct"),
):
    if client is None:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY not set on server.")

    job_id = uuid.uuid4().hex[:12]
    job_dir = JOBS_DIR / job_id
    input_dir = job_dir / "input"
    output_dir = job_dir / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # save uploads
    for uf in pdfs:
        if not uf.filename.lower().endswith(".pdf"):
            continue
        dest = input_dir / Path(uf.filename).name
        content = await uf.read()
        with open(dest, "wb") as f:
            f.write(content)

    job = JobState(
        job_id=job_id,
        job_dir=job_dir,
        input_dir=input_dir,
        output_dir=output_dir,
    )

    with LOCK:
        JOBS[job_id] = job

    t = threading.Thread(target=process_job, args=(job, int(dpi), float(sleep_sec), str(model)), daemon=True)
    t.start()

    return {"job_id": job_id}


@app.get("/api/job/{job_id}")
def get_job(job_id: str):
    with LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    downloads = None
    if job.status == "done":
        downloads = {
            "receipts_unstamped_zip": f"/api/job/{job_id}/download/receipts_unstamped",
            "receipts_stamped_zip": f"/api/job/{job_id}/download/receipts_stamped",
            "credit_notes_zip": f"/api/job/{job_id}/download/credit_notes",
            "all_zip": f"/api/job/{job_id}/download/all",
            "csv_log": f"/api/job/{job_id}/download/csv",
        }

    log_tail = job.log[-40:] if job.log else []

    return {
        "job_id": job.job_id,
        "status": job.status,
        "progress_pct": job.progress_pct,
        "message": job.message,
        "error": job.error,
        "processed_pages": job.processed_pages,
        "total_pages": job.total_pages,
        "downloads": downloads,
        "log_tail": log_tail,
    }


@app.post("/api/job/{job_id}/cancel")
def cancel_job(job_id: str):
    with LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    job.cancel_requested = True
    job_log(job, "Cancel requested by user.")
    return {"ok": True}


@app.get("/api/job/{job_id}/download/{kind}")
def download(job_id: str, kind: str):
    with LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "done":
        raise HTTPException(status_code=400, detail="Job not completed yet")

    if kind == "all": 
        path = job.zip_all
        name = "all_output.zip"
    elif kind == "receipts_unstamped":
        path = job.zip_receipts_unstamped
        name = "receipts_unstamped.zip"
    elif kind == "receipts_stamped":
        path = job.zip_receipts_stamped
        name = "receipts_stamped.zip"
    elif kind == "credit_notes":
        path = job.zip_credit_notes
        name = "credit_notes.zip"
    elif kind == "csv":
        path = job.csv_path
        name = "classification_log.csv"
    else:
        raise HTTPException(status_code=404, detail="Unknown download type")

    if not path or not path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(path=str(path), filename=name)

# -----------------------------
# Serve frontend (MUST be last, after /api routes)
# -----------------------------
FRONTEND_DIR = (BASE_DIR.parent / "frontend").resolve()
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")

