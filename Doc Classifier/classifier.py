import os
import json
import shutil
import base64
import time
import csv
import re
from pathlib import Path
from groq import Groq
from pdf2image import convert_from_path
from io import BytesIO

# 1. Setup Groq Client
# WARNING: Ensure your TPD (Tokens Per Day) is reset or switch to llama-3.3-70b-versatile if Llama 4 is limited.
client = Groq(api_key="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")

def get_groq_analysis(image_bytes):
    """Universal stamp detection and strict classification logic."""
    base64_image = base64.b64encode(image_bytes).decode('utf-8')
    model_name = "meta-llama/llama-4-scout-17b-16e-instruct"
    
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
            messages=[{"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{base64_image}"}}
            ]}],
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)
    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg:
            print(f"  🛑 Rate limit hit. Check Groq Dashboard. Error: {err_msg[:100]}")
        else:
            print(f"  ⚠️ API Error: {e}")
        return None

def process_docs():
    input_folder = Path("Nellara")
    output_base = Path("Nellara output")
    output_base.mkdir(exist_ok=True)
    
    all_rows = [] 

    for pdf_file in input_folder.glob("*.pdf"):
        print(f"--- Processing: {pdf_file.name} ---")
        
        pdf_output_dir = output_base / pdf_file.stem
        
        # DEFINED DIRECTORIES - KEEPING THEM STRICTLY SEPARATE
        unstamped_receipt_dir = pdf_output_dir / "receipts" / "unstamped"
        stamped_receipt_dir = pdf_output_dir / "receipts" / "stamped"
        credit_dir = pdf_output_dir / "credit_notes"
        
        unstamped_receipt_dir.mkdir(parents=True, exist_ok=True)
        stamped_receipt_dir.mkdir(parents=True, exist_ok=True)
        credit_dir.mkdir(parents=True, exist_ok=True)

        try:
            # DPI 150 is the balance between clarity for stamps and token size
            pages = convert_from_path(pdf_file, dpi=150)
            
            for i, page in enumerate(pages):
                print(f"  Page {i+1}/{len(pages)}...")
                img_buf = BytesIO()
                page.save(img_buf, format='PNG')
                
                analysis = get_groq_analysis(img_buf.getvalue())
                if not analysis:
                    print(f"    ❌ Skipping Page {i+1} due to error.")
                    continue

                is_receipt = analysis.get("is_receipt", False)
                has_stamp = analysis.get("has_stamp", False)
                img_filename = f"page_{i+1}.png"
                
                # --- FINAL FILING LOGIC ---
                if is_receipt:
                    if not has_stamp:
                        target_path = unstamped_receipt_dir / img_filename
                        status = "STAMPED_RECEIPT"
                    else:
                        target_path = stamped_receipt_dir / img_filename
                        status = "UNSTAMPED_RECEIPT"
                else:
                    # Anything flagged as false for is_receipt goes to Credit Notes
                    target_path = credit_dir / img_filename
                    status = "CREDIT_NOTE"

                page.save(target_path)
                print(f"    -> Filed as: {status}")

                all_rows.append({
                    "source_pdf": pdf_file.name,
                    "page": i + 1,
                    "status": status,
                    "is_receipt": is_receipt,
                    "has_stamp": has_stamp,
                    "stamp_details": analysis.get("detected_stamp_details", ""),
                    "document_data": json.dumps(analysis.get("document_data", {}))
                })
                
                # 3-second sleep to help maintain the Free Tier 'Tokens Per Minute' limit
                time.sleep(3) 

            # Move the original PDF after processing all pages
            shutil.move(str(pdf_file), pdf_output_dir / pdf_file.name)
            print(f"✅ Completed: {pdf_file.name}\n")

        except Exception as e:
            print(f"❌ Fatal Error on {pdf_file.name}: {e}")

    # Generate CSV Summary
    if all_rows:
        headers = ["source_pdf", "page", "status", "is_receipt", "has_stamp", "stamp_details", "document_data"]
        with open("classification_log.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"📊 Process complete. Log saved to: classification_log.csv")

if __name__ == "__main__":
    process_docs()
