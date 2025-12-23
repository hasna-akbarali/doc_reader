// If backend served from same host/port, keep empty:
const API_BASE = "";

const els = {
  pdfs: document.getElementById("pdfs"),
  startBtn: document.getElementById("startBtn"),
  cancelBtn: document.getElementById("cancelBtn"),
  statusPill: document.getElementById("statusPill"),
  bar: document.getElementById("bar"),
  progressText: document.getElementById("progressText"),
  downloads: document.getElementById("downloads"),
  log: document.getElementById("log"),
  fileInfo: document.getElementById("fileInfo"),
  clearLogBtn: document.getElementById("clearLogBtn"),
};

let currentJobId = null;
let pollTimer = null;

function setStatus(s) {
  els.statusPill.textContent = s;
}
function setProgress(pct, text) {
  const x = Math.max(0, Math.min(100, pct || 0));
  els.bar.style.width = `${x}%`;
  els.progressText.textContent = text || "…";
}
function clearDownloads() { els.downloads.innerHTML = ""; }
function addDownload(label, url) {
  const a = document.createElement("a");
  a.href = url;
  a.target = "_blank";
  a.rel = "noreferrer";
  a.textContent = label;
  els.downloads.appendChild(a);
}
function logLine(line) {
  const t = new Date().toLocaleTimeString();
  els.log.textContent += `[${t}] ${line}\n`;
  els.log.scrollTop = els.log.scrollHeight;
}
function setUiRunning(r) {
  els.startBtn.disabled = r;
  els.cancelBtn.disabled = !r;
  els.pdfs.disabled = r;
}

async function startJob() {
  const files = els.pdfs.files;
  if (!files || files.length === 0) {
    logLine("Select at least one PDF.");
    return;
  }

  clearDownloads();
  setUiRunning(true);
  setStatus("uploading");
  setProgress(2, "Uploading…");

  const fd = new FormData();
  for (const f of files) fd.append("pdfs", f);
  fd.append("dpi", "150");
  fd.append("sleep_sec", "3");
  fd.append("model", "meta-llama/llama-4-scout-17b-16e-instruct");  

  try {
    const res = await fetch(`${API_BASE}/api/process`, { method: "POST", body: fd });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();
    currentJobId = data.job_id;

    logLine(`Job started: ${currentJobId}`);
    setStatus("running");
    setProgress(5, "Processing…");

    pollTimer = setInterval(pollJob, 1200);
    await pollJob();
  } catch (e) {
    logLine(`Start error: ${e.message || e}`);
    setUiRunning(false);
    setStatus("error");
    setProgress(0, "Failed");
  }
}

async function pollJob() {
  if (!currentJobId) return;

  try {
    const res = await fetch(`${API_BASE}/api/job/${currentJobId}`);
    if (!res.ok) throw new Error(await res.text());
    const j = await res.json();

    setStatus(j.status);
    setProgress(j.progress_pct, j.message);

    if (Array.isArray(j.log_tail)) {
      // naive append: backend sends last 40 lines; duplicates are okay for minimal app
      j.log_tail.forEach(logLine);
    }

    if (j.status === "done") {
      clearInterval(pollTimer);
      pollTimer = null;
      setUiRunning(false);

      clearDownloads();
      const d = j.downloads || {};
      if (d.receipts_unstamped_zip) addDownload("⬇️ receipts/unstamped (zip)", `${API_BASE}${d.receipts_unstamped_zip}`);
      if (d.receipts_stamped_zip) addDownload("⬇️ receipts/stamped (zip)", `${API_BASE}${d.receipts_stamped_zip}`);
      if (d.credit_notes_zip) addDownload("⬇️ credit_notes (zip)", `${API_BASE}${d.credit_notes_zip}`);
      if (d.all_zip) addDownload("⬇️ ALL OUTPUT (zip)", `${API_BASE}${d.all_zip}`);
      if (d.csv_log) addDownload("⬇️ classification_log.csv", `${API_BASE}${d.csv_log}`);

      logLine("Job completed.");
    }

    if (j.status === "error" || j.status === "cancelled") {
      clearInterval(pollTimer);
      pollTimer = null;
      setUiRunning(false);
      logLine(`Job ended: ${j.status} ${j.error ? "- " + j.error : ""}`);
    }
  } catch (e) {
    logLine(`Poll error: ${e.message || e}`);
  }
}

async function cancelJob() {
  if (!currentJobId) return;
  try {
    const res = await fetch(`${API_BASE}/api/job/${currentJobId}/cancel`, { method: "POST" });
    if (!res.ok) throw new Error(await res.text());
    logLine("Cancel requested.");
  } catch (e) {
    logLine(`Cancel error: ${e.message || e}`);
  }
}

els.pdfs.addEventListener("change", () => {
  const files = els.pdfs.files;
  if (!files || files.length === 0) {
    els.fileInfo.textContent = "No files selected.";
    return;
  }
  const names = Array.from(files).map(f => f.name);
  els.fileInfo.textContent = `${files.length} file(s): ${names.slice(0,4).join(", ")}${names.length > 4 ? "…" : ""}`;
});

els.startBtn.addEventListener("click", startJob);
els.cancelBtn.addEventListener("click", cancelJob);
els.clearLogBtn.addEventListener("click", () => els.log.textContent = "");

setStatus("idle");
setProgress(0, "Waiting…");
