# main.py

from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple
from io import BytesIO
import zipfile
import os

import pandas as pd

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import (
    JSONResponse,
    StreamingResponse,
    HTMLResponse,
    FileResponse,
)
from fastapi.staticfiles import StaticFiles
from urllib.parse import unquote

from processing import (
    run_pipeline_to_frames,
    make_excel_for_municipality,
    COL_MUNICIPALITY_MIN,
    SECTORS_MAP,  # لو احتجناه لاحقاً
)

app = FastAPI()

# ================= إعداد المسارات الأساسية ==================

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
INDEX_FILE = STATIC_DIR / "index.html"

DATA_DIR = BASE_DIR / "data"
RUNS_DIR = DATA_DIR / "runs"
os.makedirs(RUNS_DIR, exist_ok=True)

# ربط static (js, css, صور, html داخل مجلد static)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ================= تعريف القطاعات والبلديات ==================

SECTOR_MUNIS = {
    "abha": [
        "نطاق خدمة مدينة أبها",
        "فرع مدينة سلطان",
        "فرع مربه",
        "بلدية طبب الفرعيه",
        "بلدية الشعف الفرعيه",
        "بلدية السودة الفرعيه",
        "بلدية العرين الفرعية",
        "بلدية احد رفيدة",
    ],
    "khamis": [
        "بلدية الواديين",
        "بلدية خميس مشيط",
        "بلدية وادي بن هشبل",
        "بلدية طريب",
    ],
    "north": [
        "بلدية النماص",
        "بلدية تنومه",
        "بلدية بلقرن",
        "بلدية بلحمر",
        "بلدية بلسمر",
        "بلدية بني عمرو",
        "بلدية البشائر",
    ],
    "south": [
        "بلدية سراة عبيده",
        "بلدية ظهران الجنوب",
        "بلدية الحرجه",
        "بلدية الامواه",
        "بلدية الفرشه",
        "بلدية الربوعه",
    ],
    "west": [
        "بلدية محايل",
        "بلدية رجال المع",
        "بلدية بارق",
        "بلدية المجارده",
        "بلدية قنا",
        "بلدية بحر ابو سكينه",
        "بلدية الساحل",
        "بلدية البرك",
    ],
}

SECTOR_LABELS = {
    "abha": "قطاع أبها",
    "khamis": "قطاع الخميس",
    "north": "قطاع الشمال",
    "south": "قطاع الجنوب",
    "west": "قطاع الغرب",
}


@app.get("/api/meta/sectors")
def meta_sectors():
    """
    معلومات الفلاتر:
      - القطاعات (مفتاح + اسم عربي + قائمة البلديات لكل قطاع)
    تستخدمها الصفحة الرئيسية لبناء الـ dropdowns.
    """
    meta = {}
    for key, munis in SECTOR_MUNIS.items():
        meta[key] = {
            "label": SECTOR_LABELS.get(key, key),
            "municipalities": munis,
        }
    return meta


# ================= دوال مساعدة لإدارة التشغيلات ==================

def _list_run_ids() -> List[str]:
    """إرجاع قائمة بالـ run_id المخزّنة في data/runs"""
    run_ids = set()
    for p in RUNS_DIR.glob("*_summary.pkl"):
        name = p.name
        run_id = name.split("_summary.pkl")[0]
        if run_id:
            run_ids.add(run_id)
    return sorted(run_ids)


def _load_run(run_id: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """تحميل RAW_DF و SUMMARY_DF لتشغيل معيّن."""
    raw_path = RUNS_DIR / f"{run_id}_raw.pkl"
    summary_path = RUNS_DIR / f"{run_id}_summary.pkl"

    if not summary_path.exists():
        return None, None

    raw_df = pd.read_pickle(raw_path) if raw_path.exists() else None
    summary_df = pd.read_pickle(summary_path)
    return raw_df, summary_df


def _save_run(raw_df: pd.DataFrame, summary_df: pd.DataFrame) -> str:
    """
    حفظ تشغيل جديد في المجلد data/runs باسم run_id = timestamp،
    ثم تنظيف المجلد للإبقاء على آخر تشغيلين فقط.
    """
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = RUNS_DIR / f"{run_id}_raw.pkl"
    summary_path = RUNS_DIR / f"{run_id}_summary.pkl"

    raw_df.to_pickle(raw_path)
    summary_df.to_pickle(summary_path)

    # تنظيف: الإبقاء على آخر تشغيلين
    run_ids = _list_run_ids()
    if len(run_ids) > 2:
        for old_id in run_ids[:-2]:
            old_raw = RUNS_DIR / f"{old_id}_raw.pkl"
            old_summary = RUNS_DIR / f"{old_id}_summary.pkl"
            if old_raw.exists():
                old_raw.unlink()
            if old_summary.exists():
                old_summary.unlink()

    return run_id


def _get_latest_runs() -> Tuple[Optional[str], Optional[str]]:
    """
    إرجاع:
      latest_id = أحدث تشغيل
      prev_id   = التشغيل الذي قبله (إن وجد)
    """
    run_ids = _list_run_ids()
    if not run_ids:
        return None, None
    if len(run_ids) == 1:
        return run_ids[0], None
    return run_ids[-1], run_ids[-2]


# ================= واجهة HTML ==================

@app.get("/", response_class=HTMLResponse)
def serve_index():
    """الصفحة الرئيسية index.html"""
    if not INDEX_FILE.exists():
        return HTMLResponse(
            content="index.html غير موجود داخل مجلد static.",
            status_code=500,
        )
    return FileResponse(str(INDEX_FILE))


@app.get("/{page_name}", response_class=HTMLResponse)
def serve_html_page(page_name: str):
    """
    خدمة صفحات مثل:
      /upload  -> static/upload.html
      /abha    -> static/abha.html
    """
    file_path = STATIC_DIR / f"{page_name}.html"
    if file_path.exists():
        return FileResponse(str(file_path))
    return HTMLResponse(content="Page not found", status_code=404)


# ================= API: تشغيل المعالجة وتخزين النتائج ==================

@app.post("/api/process")
async def process_files(
    raw_today: UploadFile = File(...),      # ملف الزيارات (اليوم)
    ministry_new: UploadFile = File(...),   # ملف الوزارة (Asir)
    raw_prev: UploadFile = File(None),      # يُتجاهل حالياً (للخلفية فقط)
):
    """
    تشغيل البايبلاين وحفظ النتيجة باعتبارها "اليوم الحالي".
    بعد التشغيل:
      - نخزن raw + summary كتشغيل جديد
      - نُبقي فقط على آخر تشغيلين
      - نرجع إجماليات اليوم + اليوم السابق + الفروقات
    """
    raw_today_bytes = await raw_today.read()
    ministry_bytes = await ministry_new.read()

    raw_today_all, summary_today_all = run_pipeline_to_frames(
        raw_today_bytes,
        ministry_bytes,
    )

    new_run_id = _save_run(raw_today_all, summary_today_all)

    latest_id, prev_id = _get_latest_runs()

    _, latest_summary = _load_run(latest_id)
    if latest_summary is None or latest_summary.empty:
        return JSONResponse(
            {"error": "فشل تحميل بيانات اليوم بعد الحفظ."},
            status_code=500,
        )

    total_visited_today = int(latest_summary["عدد_تمت_الزيارة"].sum())
    total_not_today = int(latest_summary["عدد_لم_تزر"].sum())

    result = {
        "run_id": new_run_id,
        "totals_today": {
            "visited": total_visited_today,
            "not_visited": total_not_today,
        },
        "totals_prev": None,
        "totals_delta": None,
    }

    if prev_id is not None:
        _, prev_summary = _load_run(prev_id)
        if prev_summary is not None and not prev_summary.empty:
            total_visited_prev = int(prev_summary["عدد_تمت_الزيارة"].sum())
            total_not_prev = int(prev_summary["عدد_لم_تزر"].sum())

            result["totals_prev"] = {
                "visited": total_visited_prev,
                "not_visited": total_not_prev,
            }
            result["totals_delta"] = {
                "visited": total_visited_today - total_visited_prev,
                "not_visited": total_not_today - total_not_prev,
            }

    # ملخص البلديات لليوم الحالي (اختياري)
    muni_today = (
        latest_summary
        .groupby(COL_MUNICIPALITY_MIN)[["عدد_تمت_الزيارة", "عدد_لم_تزر", "إجمالي_الرخص"]]
        .sum()
        .reset_index()
    )
    result["muni_today"] = muni_today.to_dict(orient="records")

    # ملخص حسب التصنيف لليوم الحالي (اختياري)
    type_summary_today = (
        latest_summary
        .groupby("التصنيف")[["عدد_تمت_الزيارة", "عدد_لم_تزر"]]
        .sum()
        .reset_index()
    )
    result["type_summary_today"] = type_summary_today.to_dict(orient="records")

    result["sectors"] = SECTORS_MAP

    return JSONResponse(result)


# ================= API: إجماليات للبطاقات ==================

@app.get("/api/totals")
def get_totals():
    """
    إجماليات لكل المنطقة (آخر تشغيل) + اليوم السابق (إن وجد) + الفروقات.
    """
    latest_id, prev_id = _get_latest_runs()
    if latest_id is None:
        return JSONResponse(
            {"error": "لا توجد أي تشغيلات محفوظة. ارفع ملفات اليوم أولاً."},
            status_code=400,
        )

    _, summary_curr = _load_run(latest_id)
    if summary_curr is None or summary_curr.empty:
        return JSONResponse(
            {"error": "ملف الملخص للتشغيل الأخير فارغ أو غير موجود."},
            status_code=500,
        )

    curr_visited = int(summary_curr["عدد_تمت_الزيارة"].sum())
    curr_not = int(summary_curr["عدد_لم_تزر"].sum())
    curr_total = curr_visited + curr_not

    resp = {
        "visited": curr_visited,
        "not_visited": curr_not,
        "total": curr_total,
        "prev_visited": None,
        "prev_not_visited": None,
        "prev_total": None,
        "delta_visited": None,
        "delta_not_visited": None,
        "delta_total": None,
    }

    if prev_id is not None:
        _, summary_prev = _load_run(prev_id)
        if summary_prev is not None and not summary_prev.empty:
            prev_visited = int(summary_prev["عدد_تمت_الزيارة"].sum())
            prev_not = int(summary_prev["عدد_لم_تزر"].sum())
            prev_total = prev_visited + prev_not

            resp["prev_visited"] = prev_visited
            resp["prev_not_visited"] = prev_not
            resp["prev_total"] = prev_total

            resp["delta_visited"] = curr_visited - prev_visited
            resp["delta_not_visited"] = curr_not - prev_not
            resp["delta_total"] = curr_total - prev_total

    return resp


@app.get("/api/totals/sector/{sector_key}")
def get_totals_sector(sector_key: str):
    """إجماليات قطاع معيّن (اليوم + اليوم السابق + الفروقات)."""
    sector_key = sector_key.lower()
    if sector_key not in SECTOR_MUNIS:
        return JSONResponse({"error": "قطاع غير معروف"}, status_code=400)

    munis = SECTOR_MUNIS[sector_key]

    latest_id, prev_id = _get_latest_runs()
    if latest_id is None:
        return JSONResponse(
            {"error": "لا توجد أي تشغيلات محفوظة. ارفع ملفات اليوم أولاً."},
            status_code=400,
        )

    _, summary_curr = _load_run(latest_id)
    if summary_curr is None or summary_curr.empty:
        return JSONResponse(
            {"error": "بيانات اليوم غير متاحة."},
            status_code=500,
        )

    summary_curr_sec = summary_curr[summary_curr[COL_MUNICIPALITY_MIN].isin(munis)]
    curr_visited = int(summary_curr_sec["عدد_تمت_الزيارة"].sum())
    curr_not = int(summary_curr_sec["عدد_لم_تزر"].sum())
    curr_total = curr_visited + curr_not

    resp = {
        "sector": sector_key,
        "visited": curr_visited,
        "not_visited": curr_not,
        "total": curr_total,
        "prev_visited": None,
        "prev_not_visited": None,
        "prev_total": None,
        "delta_visited": None,
        "delta_not_visited": None,
        "delta_total": None,
    }

    if prev_id is not None:
        _, summary_prev = _load_run(prev_id)
        if summary_prev is not None and not summary_prev.empty:
            summary_prev_sec = summary_prev[summary_prev[COL_MUNICIPALITY_MIN].isin(munis)]

            prev_visited = int(summary_prev_sec["عدد_تمت_الزيارة"].sum())
            prev_not = int(summary_prev_sec["عدد_لم_تزر"].sum())
            prev_total = prev_visited + prev_not

            resp["prev_visited"] = prev_visited
            resp["prev_not_visited"] = prev_not
            resp["prev_total"] = prev_total

            resp["delta_visited"] = curr_visited - prev_visited
            resp["delta_not_visited"] = curr_not - prev_not
            resp["delta_total"] = curr_total - prev_total

    return resp


@app.get("/api/totals/municipality/{muni_name}")
def get_totals_municipality(muni_name: str):
    """
    إجماليات بلدية معيّنة (اليوم + اليوم السابق + الفروقات).
    muni_name يُرسل URL-encoded.
    """
    muni_name = unquote(muni_name)

    latest_id, prev_id = _get_latest_runs()
    if latest_id is None:
        return JSONResponse(
            {"error": "لا توجد أي تشغيلات محفوظة. ارفع ملفات اليوم أولاً."},
            status_code=400,
        )

    _, summary_curr = _load_run(latest_id)
    if summary_curr is None or summary_curr.empty:
        return JSONResponse(
            {"error": "بيانات اليوم غير متاحة."},
            status_code=500,
        )

    summary_curr_m = summary_curr[summary_curr[COL_MUNICIPALITY_MIN] == muni_name]
    curr_visited = int(summary_curr_m["عدد_تمت_الزيارة"].sum())
    curr_not = int(summary_curr_m["عدد_لم_تزر"].sum())
    curr_total = curr_visited + curr_not

    resp = {
        "municipality": muni_name,
        "visited": curr_visited,
        "not_visited": curr_not,
        "total": curr_total,
        "prev_visited": None,
        "prev_not_visited": None,
        "prev_total": None,
        "delta_visited": None,
        "delta_not_visited": None,
        "delta_total": None,
    }

    if prev_id is not None:
        _, summary_prev = _load_run(prev_id)
        if summary_prev is not None and not summary_prev.empty:
            summary_prev_m = summary_prev[summary_prev[COL_MUNICIPALITY_MIN] == muni_name]

            prev_visited = int(summary_prev_m["عدد_تمت_الزيارة"].sum())
            prev_not = int(summary_prev_m["عدد_لم_تزر"].sum())
            prev_total = prev_visited + prev_not

            resp["prev_visited"] = prev_visited
            resp["prev_not_visited"] = prev_not
            resp["prev_total"] = prev_total

            resp["delta_visited"] = curr_visited - prev_visited
            resp["delta_not_visited"] = curr_not - prev_not
            resp["delta_total"] = curr_total - prev_total

    return resp


# ================= API: بيانات الرسم البياني (اليوم vs اليوم السابق) ==================

def _chart_data_by_classification(
    munis_filter: Optional[list] = None,
    muni_name: Optional[str] = None,
):
    """
    إرجاع بيانات الرسم حسب التصنيف مع مقارنة اليوم الحالي / اليوم السابق.
    - current  : إجمالي (تمت + لم تُزر) في اليوم الحالي لكل تصنيف
    - previous : نفس الإجمالي في اليوم السابق (إن وجد)
    """
    latest_id, prev_id = _get_latest_runs()
    if latest_id is None:
        return {"labels": [], "current": [], "previous": []}

    # اليوم الحالي
    _, summary_curr = _load_run(latest_id)
    if summary_curr is None or summary_curr.empty:
        return {"labels": [], "current": [], "previous": []}

    df_curr = summary_curr.copy()
    if munis_filter:
        df_curr = df_curr[df_curr[COL_MUNICIPALITY_MIN].isin(munis_filter)]
    if muni_name:
        df_curr = df_curr[df_curr[COL_MUNICIPALITY_MIN] == muni_name]

    if df_curr.empty:
        return {"labels": [], "current": [], "previous": []}

    grp_curr = (
        df_curr
        .groupby("التصنيف")[["عدد_تمت_الزيارة", "عدد_لم_تزر"]]
        .sum()
    )
    curr_total = grp_curr["عدد_تمت_الزيارة"] + grp_curr["عدد_لم_تزر"]

    # اليوم السابق
    prev_total = None
    if prev_id is not None:
        _, summary_prev = _load_run(prev_id)
        if summary_prev is not None and not summary_prev.empty:
            df_prev = summary_prev.copy()
            if munis_filter:
                df_prev = df_prev[df_prev[COL_MUNICIPALITY_MIN].isin(munis_filter)]
            if muni_name:
                df_prev = df_prev[df_prev[COL_MUNICIPALITY_MIN] == muni_name]

            if not df_prev.empty:
                grp_prev = (
                    df_prev
                    .groupby("التصنيف")[["عدد_تمت_الزيارة", "عدد_لم_تزر"]]
                    .sum()
                )
                prev_total = grp_prev["عدد_تمت_الزيارة"] + grp_prev["عدد_لم_تزر"]

    labels = list(curr_total.index)
    if prev_total is not None:
        extra = [c for c in prev_total.index if c not in labels]
        labels.extend(extra)

    current_vals = [int(curr_total.get(lbl, 0)) for lbl in labels]
    if prev_total is not None:
        previous_vals = [int(prev_total.get(lbl, 0)) for lbl in labels]
    else:
        previous_vals = [0 for _ in labels]

    return {
        "labels": labels,
        "current": current_vals,
        "previous": previous_vals,
    }


@app.get("/api/chart-data")
def chart_data_all():
    """الرسم للمنطقة كاملة."""
    return _chart_data_by_classification()


@app.get("/api/chart-data/sector/{sector_key}")
def chart_data_sector(sector_key: str):
    """الرسم لقطاع معيّن."""
    munis = SECTOR_MUNIS.get(sector_key)
    if not munis:
        return {"labels": [], "current": [], "previous": []}
    return _chart_data_by_classification(munis_filter=munis)


@app.get("/api/chart-data/municipality/{muni_name}")
def chart_data_municipality(muni_name: str):
    """الرسم لبلدية معيّنة."""
    muni_name = unquote(muni_name)
    return _chart_data_by_classification(muni_name=muni_name)


# ================= API: تحميل ملف بلدية واحدة من أحدث تشغيل ==================

@app.get("/api/municipality/{muni_name}/excel")
def download_municipality_file(muni_name: str):
    """تحميل ملف Excel لبلدية معيّنة من أحدث تشغيل."""
    muni_name = unquote(muni_name)

    latest_id, _ = _get_latest_runs()
    if latest_id is None:
        return JSONResponse(
            {"error": "لا توجد أي تشغيلات محفوظة. ارفع ملفات اليوم أولاً."},
            status_code=400,
        )

    raw_df, summary_df = _load_run(latest_id)
    if raw_df is None or summary_df is None:
        return JSONResponse(
            {"error": "تعذر تحميل بيانات أحدث تشغيل."},
            status_code=500,
        )

    excel_bytes = make_excel_for_municipality(raw_df, summary_df, muni_name)
    if not excel_bytes:
        return JSONResponse(
            {"error": f"لا توجد بيانات للبلدية {muni_name} في أحدث تشغيل."},
            status_code=404,
        )

    file_like = BytesIO(excel_bytes)
    filename = "municipality.xlsx"  # اسم إنجليزي لتفادي مشاكل الترميز
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}

    return StreamingResponse(
        file_like,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


# ================= API: تحميل ملفات قطاع كامل (zip) من أحدث تشغيل ==================

@app.get("/api/download/sector/{sector_key}")
def download_sector_zip(sector_key: str):
    """
    إنشاء ملف ZIP يحتوي على ملفات كل بلدية في القطاع
    من أحدث تشغيل.
    """
    sector_key = sector_key.lower()
    if sector_key not in SECTOR_MUNIS:
        return JSONResponse({"error": "قطاع غير معروف"}, status_code=400)

    latest_id, _ = _get_latest_runs()
    if latest_id is None:
        return JSONResponse(
            {"error": "لا توجد أي تشغيلات محفوظة. ارفع ملفات اليوم أولاً."},
            status_code=400,
        )

    raw_df, summary_df = _load_run(latest_id)
    if raw_df is None or summary_df is None:
        return JSONResponse(
            {"error": "تعذر تحميل بيانات أحدث تشغيل."},
            status_code=500,
        )

    munis = SECTOR_MUNIS[sector_key]

    mem = BytesIO()
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for muni in munis:
            excel_bytes = make_excel_for_municipality(raw_df, summary_df, muni)
            if not excel_bytes:
                continue
            # اسم الملف داخل الـ ZIP بالعربي عادي
            internal_name = f"{muni}.xlsx"
            zf.writestr(internal_name, excel_bytes)

    mem.seek(0)

    # اسم ملف الـ zip في الهيدر يجب أن يكون ASCII لتفادي UnicodeEncodeError
    zip_filename = f"sector_{sector_key}.zip"
    headers = {"Content-Disposition": f'attachment; filename="{zip_filename}"'}

    return StreamingResponse(
        mem,
        media_type="application/zip",
        headers=headers,
    )
