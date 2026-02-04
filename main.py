from pathlib import Path
from datetime import datetime, date
from typing import Optional, List, Tuple
from io import BytesIO
import zipfile
import os
import secrets  # من أجل المقارنة الآمنة لكلمات المرور


import pandas as pd
import numpy as np 

from fastapi import (
    FastAPI,
    UploadFile,
    File,
    Depends,
    HTTPException,
    status,
)
from fastapi.responses import (
    JSONResponse,
    HTMLResponse,
    FileResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from urllib.parse import unquote

from processing import (
    run_pipeline_to_frames,
    make_excel_for_municipality,
    COL_MUNICIPALITY_MIN,
    SECTORS_MAP,
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


# ================= دوال مساعدة لإدارة التشغيلات (runs) ==================


def _list_run_ids() -> List[str]:
    """إرجاع قائمة بالـ run_id المخزّنة في data/runs (مرتّبة)."""
    run_ids = set()
    for p in RUNS_DIR.glob("*_summary.pkl"):
        name = p.name
        run_id = name.split("_summary.pkl")[0]
        if run_id:
            run_ids.add(run_id)
    return sorted(run_ids)


def _normalize_summary_columns(summary_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    توحيد أسماء الأعمدة في جدول الملخص حتى تطابق ما يتوقعه main.py:
      - تمت الزيارة
      - لم تزار
    بغض النظر عن الأسماء التي خرجت من processing.py.
    """
    if summary_df is None:
        return None

    df = summary_df.copy()
    rename_map = {}

    if "عدد_تمت_الزيارة" in df.columns and "تمت الزيارة" not in df.columns:
        rename_map["عدد_تمت_الزيارة"] = "تمت الزيارة"

    if "عدد_لم_تزر" in df.columns and "لم تزار" not in df.columns:
        rename_map["عدد_لم_تزر"] = "لم تزار"

    if "لم تُزر" in df.columns and "لم تزار" not in df.columns:
        rename_map["لم تُزر"] = "لم تزار"

    if rename_map:
        df = df.rename(columns=rename_map)

    return df

def _df_to_json_records(df: Optional[pd.DataFrame]) -> list[dict]:
    """
    تحويل DataFrame إلى قائمة سجلات جاهزة لـ JSON
    مع تنظيف كل القيم غير الصالحة (NaN, inf, -inf, تواريخ...).
    """
    if df is None or df.empty:
        return []

    df2 = df.copy()

    # 1) استبدال +inf / -inf في الأعمدة الرقمية بـ NaN
    for col in df2.columns:
        if pd.api.types.is_numeric_dtype(df2[col]):
            df2[col] = df2[col].replace([np.inf, -np.inf], np.nan)

    # 2) تحويل أعمدة التواريخ بأنواعها إلى نص
    dt_cols = df2.select_dtypes(include=["datetime64[ns]"]).columns
    if len(dt_cols) > 0:
        df2[dt_cols] = df2[dt_cols].astype(str)

    dtz_cols = df2.select_dtypes(include=["datetimetz"]).columns
    if len(dtz_cols) > 0:
        df2[dtz_cols] = df2[dtz_cols].astype(str)

    td_cols = df2.select_dtypes(include=["timedelta64[ns]"]).columns
    if len(td_cols) > 0:
        df2[td_cols] = df2[td_cols].astype(str)

    # 3) تحويل أي Timestamp موجود في أعمدة object إلى نص
    for col in df2.columns:
        if df2[col].dtype == "object":
            df2[col] = df2[col].apply(
                lambda x: x.isoformat() if isinstance(x, (datetime, pd.Timestamp)) else x
            )

    # 4) تحويل NaN / pd.NA إلى None
    df2 = df2.where(pd.notnull(df2), None)

    # 5) بعد التحويل إلى dict، تنظيف أي float غير منتهٍ (inf/-inf/nan) على مستوى Python
    import math

    records = df2.to_dict(orient="records")
    cleaned_records = []
    for row in records:
        clean_row = {}
        for k, v in row.items():
            if isinstance(v, float) and not math.isfinite(v):
                clean_row[k] = None
            else:
                clean_row[k] = v
        cleaned_records.append(clean_row)

    return cleaned_records


def _load_run(run_id: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    تحميل RAW_DF و SUMMARY_DF لتشغيل معيّن، مع توحيد أسماء أعمدة الملخص.
    """
    raw_path = RUNS_DIR / f"{run_id}_raw.pkl"
    summary_path = RUNS_DIR / f"{run_id}_summary.pkl"

    if not summary_path.exists():
        return None, None

    raw_df = pd.read_pickle(raw_path) if raw_path.exists() else None
    summary_df = pd.read_pickle(summary_path)
    summary_df = _normalize_summary_columns(summary_df)

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


def _run_date_str(run_id: Optional[str]) -> Optional[str]:
    """
    يأخذ run_id بصيغة YYYYMMDD_HHMMSS ويرجع التاريخ بصيغة "YYYY-MM-DD".
    لو فشل يرجع None.
    """
    if not run_id:
        return None
    try:
        day_str = run_id.split("_")[0]
        d = datetime.strptime(day_str, "%Y%m%d").date()
        return d.isoformat()
    except Exception:
        return None


def _strip_styles_from_xlsx(xlsx_bytes: bytes) -> bytes:
    """
    إزالة ملف التنسيقات xl/styles.xml من ملف XLSX (إن وجد)،
    ثم إعادة بنائه في الذاكرة. الهدف: تجاوز مشاكل openpyxl مع بعض الـ Styles.
    لو لم يكن الملف ZIP صالح (ليس XLSX)، نرجع البايتات كما هي.
    """
    bio_in = BytesIO(xlsx_bytes)
    try:
        with zipfile.ZipFile(bio_in, "r") as zin:
            bio_out = BytesIO()
            with zipfile.ZipFile(bio_out, "w", compression=zipfile.ZIP_DEFLATED) as zout:
                for item in zin.infolist():
                    name = item.filename
                    # نحذف ملف التنسيقات
                    if name.lower() == "xl/styles.xml":
                        continue
                    data = zin.read(name)
                    zout.writestr(item, data)
        return bio_out.getvalue()
    except zipfile.BadZipFile:
        # ليس ملف XLSX → نرجعه كما هو
        return xlsx_bytes


# ================= نظام المستخدمين والصلاحيات (Basic Auth) ==================

security = HTTPBasic()

USERS = {
    "admin": {
        "password": "Pass123",
        "role": "admin",
        "sector": None,
    },
    "N1122": {
        "password": "NORTH_PASSWORD",
        "role": "sector",
        "sector": "north",
    },
    "S1122": {
        "password": "SOUTH_PASSWORD",
        "role": "sector",
        "sector": "south",
    },
    "E1122": {
        "password": "EAST_PASSWORD",
        "role": "sector",
        "sector": "east",
    },
    "W1122": {
        "password": "WEST_PASSWORD",
        "role": "sector",
        "sector": "west",
    },
    "K1122": {
        "password": "KHAMIS_PASSWORD",
        "role": "sector",
        "sector": "khamis",
    },
    "A1122": {
        "password": "Aa123456",
        "role": "sector",
        "sector": "abha",
    },
}


def get_current_user(credentials: HTTPBasicCredentials = Depends(security)) -> dict:
    """
    استخراج المستخدم الحالي من Basic Auth.
    """
    user_info = USERS.get(credentials.username)
    if not user_info or not secrets.compare_digest(
        credentials.password, user_info["password"]
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="اسم المستخدم أو كلمة المرور غير صحيحة",
            headers={"WWW-Authenticate": "Basic"},
        )

    return {
        "username": credentials.username,
        "role": user_info["role"],
        "sector": user_info["sector"],
    }


# ================= تعريف القطاعات والبلديات ==================

SECTOR_MUNIS = {
    "north": [
        "بلدية النماص",
        "بلدية تنومه",
        "بللقرن",
        "بلدية بللسمر",
        "بلدية بللحمر",
        "بلدية بني عمرو",
        "بلدية البشائر",
    ],
    "south": [
        "بلدية سراة عبيده",
        "بلدية ظهران الجنوب",
        "بلدية الحرجة",
        "بلدية الامواه",
        "بلدية الفرشة",
        "بلدية الربوعة",
    ],
    "east": [
        "بلدية بيشة",
        "بلدية تثليث",
        "بلدية ثنية وتباله",
        "بلدية الحازمي",
        "بلدية الصبيخة",
        "بلدية النقيع",
    ],
    "west": [
        "بلدية محايل عسير",
        "بلدية رجال المع",
        "بلدية المجاردة",
        "بلدية بارق",
        "بلدية الساحل",
        "بلدية البرك",
        "بحر ابو سكينة",
        "بلدية قنا",
    ],
    "khamis": [
        "بلدية خميس مشيط",
        "بلدية وادي هشبل",
        "بلدية طريب",
    ],
    "abha": [
        "نطاق خدمة مدينة أبها",
        "بلدية الواديين",
        "بلدية احد رفيدة",
        "Asir",
        "بلدية العرين الفرعية",
        "فرع مدينة سلطان",
        "فرع مربه",
        "فرع الشعف",
        "فرع طبب",
        "فرع السوده",
    ],
}

SECTOR_LABELS = {
    "north": "قطاع الشمال",
    "south": "قطاع الجنوب",
    "east": "قطاع الشرق",
    "west": "قطاع الغرب",
    "khamis": "قطاع خميس مشيط",
    "abha": "قطاع أبها",
}


@app.get("/api/meta/sectors")
def meta_sectors(current_user: dict = Depends(get_current_user)):
    meta = {}

    if current_user["role"] == "sector":
        sector_key = current_user["sector"]
        if sector_key in SECTOR_MUNIS:
            meta[sector_key] = {
                "label": SECTOR_LABELS.get(sector_key, sector_key),
                "municipalities": SECTOR_MUNIS[sector_key],
            }
        return meta

    for key, munis in SECTOR_MUNIS.items():
        meta[key] = {
            "label": SECTOR_LABELS.get(key, key),
            "municipalities": munis,
        }
    return meta


# ================= واجهة HTML ==================

@app.get("/api/login")
def login_check(current_user: dict = Depends(get_current_user)):
    return {"status": "ok", "user": current_user["username"], "role": current_user["role"]}


# ================= واجهة HTML ==================

@app.get("/", response_class=HTMLResponse)
def serve_index():
    if not INDEX_FILE.exists():
        return HTMLResponse(
            content="index.html غير موجود داخل مجلد static.",
            status_code=500,
        )
    return FileResponse(str(INDEX_FILE))


@app.get("/{page_name}", response_class=HTMLResponse)
def serve_html_page_no_ext(page_name: str):
    """
    تسهيل الوصول للصفحات بدون كتابة .html في الرابط.
    """
    file_path = STATIC_DIR / f"{page_name}.html"
    if file_path.exists():
        return FileResponse(str(file_path))
    
    # التحقق مما إذا كان الملف موجودًا مباشرة (CSS/JS/images)
    file_path_direct = STATIC_DIR / page_name
    if file_path_direct.is_file():
        return FileResponse(str(file_path_direct))

    return HTMLResponse(content="Page not found locally", status_code=404)


@app.get("/{page_name}.html", response_class=HTMLResponse)
def serve_html_page(page_name: str):
    file_path = STATIC_DIR / f"{page_name}.html"
    if file_path.exists():
        return FileResponse(str(file_path))
    return HTMLResponse(content="Page not found", status_code=404)


# ================= API: تشغيل المعالجة وتخزين النتائج ==================

@app.post("/api/process")
async def process_files(
    raw_today: UploadFile = File(...),
    ministry_new: UploadFile = File(...),
    raw_prev: UploadFile = File(None),
    current_user: dict = Depends(get_current_user),
):
    """
    تشغيل البايبلاين وحفظ النتيجة باعتبارها "اليوم الحالي" (أدمن فقط).
    """
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="فقط الأدمن يمكنه رفع الملفات وتشغيل المعالجة.",
        )

    raw_today_bytes = await raw_today.read()
    ministry_bytes = await ministry_new.read()

    # المحاولة الأولى: على الملفات كما هي
    try:
        raw_today_all, summary_today_all = run_pipeline_to_frames(
            raw_today_bytes,
            ministry_bytes,
        )
    except Exception as e:
        msg = str(e)
        print("ERROR in /api/process (first attempt):", type(e), msg)

        # محاولة ثانية بعد إزالة styles.xml من كلا الملفين
        cleaned_raw = _strip_styles_from_xlsx(raw_today_bytes)
        cleaned_ministry = _strip_styles_from_xlsx(ministry_bytes)

        try:
            raw_today_all, summary_today_all = run_pipeline_to_frames(
                cleaned_raw,
                cleaned_ministry,
            )
        except Exception as e2:
            msg2 = str(e2)
            print("ERROR in /api/process (fallback cleaned):", type(e2), msg2)
            return JSONResponse(
                {
                    "error": (
                        f"خطأ أثناء قراءة ملفات الإكسل حتى بعد تنظيف التنسيقات: "
                        f"{type(e2).__name__}: {msg2}"
                    )
                },
                status_code=400,
            )

    new_run_id = _save_run(raw_today_all, summary_today_all)

    latest_id, prev_id = _get_latest_runs()

    _, latest_summary = _load_run(latest_id)
    if latest_summary is None or latest_summary.empty:
        return JSONResponse(
            {"error": "فشل تحميل بيانات اليوم بعد الحفظ."},
            status_code=500,
        )

    total_visited_today = int(latest_summary["تمت الزيارة"].sum())
    total_not_today = int(latest_summary["لم تزار"].sum())

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
            total_visited_prev = int(prev_summary["تمت الزيارة"].sum())
            total_not_prev = int(prev_summary["لم تزار"].sum())

            result["totals_prev"] = {
                "visited": total_visited_prev,
                "not_visited": total_not_prev,
            }
            result["totals_delta"] = {
                "visited": total_visited_today - total_visited_prev,
                "not_visited": total_not_today - total_not_prev,
            }

    muni_today = (
        latest_summary
        .groupby(COL_MUNICIPALITY_MIN)[["تمت الزيارة", "لم تزار", "إجمالي_الرخص"]]
        .sum()
        .reset_index()
    )
    result["muni_today"] = muni_today.to_dict(orient="records")

    type_summary_today = (
        latest_summary
        .groupby("التصنيف")[["تمت الزيارة", "لم تزار"]]
        .sum()
        .reset_index()
    )
    result["type_summary_today"] = type_summary_today.to_dict(orient="records")

    result["sectors"] = SECTORS_MAP

    return JSONResponse(result)


# ================= API: إجماليات للبطاقات ==================

@app.get("/api/totals")
def get_totals(current_user: dict = Depends(get_current_user)):
    if current_user["role"] == "sector":
        sector_key = current_user["sector"]
        if not sector_key:
            raise HTTPException(
                status_code=500,
                detail="مستخدم قطاع بلا sector محدد.",
            )
        return get_totals_sector(sector_key, current_user=current_user)

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

    curr_visited = int(summary_curr["تمت الزيارة"].sum())
    curr_not = int(summary_curr["لم تزار"].sum())
    curr_total = curr_visited + curr_not

    curr_date_str = _run_date_str(latest_id)
    prev_date_str = _run_date_str(prev_id)

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
        "current_run_id": latest_id,
        "prev_run_id": prev_id,
        "current_run_date": curr_date_str,
        "prev_run_date": prev_date_str,
    }

    if prev_id is not None:
        _, summary_prev = _load_run(prev_id)
        if summary_prev is not None and not summary_prev.empty:
            prev_visited = int(summary_prev["تمت الزيارة"].sum())
            prev_not = int(summary_prev["لم تزار"].sum())
            prev_total = prev_visited + prev_not

            resp["prev_visited"] = prev_visited
            resp["prev_not_visited"] = prev_not
            resp["prev_total"] = prev_total

            resp["delta_visited"] = curr_visited - prev_visited
            resp["delta_not_visited"] = curr_not - prev_not
            resp["delta_total"] = curr_total - prev_total

    return resp


@app.get("/api/totals/sector/{sector_key}")
def get_totals_sector(
    sector_key: str,
    current_user: dict = Depends(get_current_user),
):
    sector_key = sector_key.lower()

    if current_user["role"] == "sector" and current_user["sector"] != sector_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="غير مسموح لك الوصول إلى هذا القطاع.",
        )

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
    curr_visited = int(summary_curr_sec["تمت الزيارة"].sum())
    curr_not = int(summary_curr_sec["لم تزار"].sum())
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

            prev_visited = int(summary_prev_sec["تمت الزيارة"].sum())
            prev_not = int(summary_prev_sec["لم تزار"].sum())
            prev_total = prev_visited + prev_not

            resp["prev_visited"] = prev_visited
            resp["prev_not_visited"] = prev_not
            resp["prev_total"] = prev_total

            resp["delta_visited"] = curr_visited - prev_visited
            resp["delta_not_visited"] = curr_not - prev_not
            resp["delta_total"] = curr_total - prev_total

    return resp


@app.get("/api/totals/municipality/{muni_name}")
def get_totals_municipality(
    muni_name: str,
    current_user: dict = Depends(get_current_user),
):
    muni_name = unquote(muni_name)

    if current_user["role"] == "sector":
        sector_key = current_user["sector"]
        allowed_munis = SECTOR_MUNIS.get(sector_key, [])
        if muni_name not in allowed_munis:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="غير مسموح لك الوصول إلى هذه البلدية.",
            )

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
    curr_visited = int(summary_curr_m["تمت الزيارة"].sum())
    curr_not = int(summary_curr_m["لم تزار"].sum())
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

            prev_visited = int(summary_prev_m["تمت الزيارة"].sum())
            prev_not = int(summary_prev_m["لم تزار"].sum())
            prev_total = prev_visited + prev_not

            resp["prev_visited"] = prev_visited
            resp["prev_not_visited"] = prev_not
            resp["prev_total"] = prev_total

            resp["delta_visited"] = curr_visited - prev_visited
            resp["delta_not_visited"] = curr_not - prev_not
            resp["delta_total"] = curr_total - prev_total

    return resp


# ================= API: رسم بياني مقارن ==================

@app.get("/api/chart-data/compare")
def chart_data_compare(
    scope: str = "all",
    sector: Optional[str] = None,
    municipality: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
):
    if current_user["role"] == "sector":
        user_sector = current_user["sector"]

        if scope == "all":
            scope = "sector"
            sector = user_sector

        elif scope == "sector":
            if sector is None or sector.lower() != user_sector:
                sector = user_sector

        elif scope == "municipality":
            muni_decoded = unquote(municipality) if municipality else None
            allowed = SECTOR_MUNIS.get(user_sector, [])
            if not muni_decoded or muni_decoded not in allowed:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="غير مسموح لك الوصول إلى هذه البلدية في الرسم البياني.",
                )

    latest_id, prev_id = _get_latest_runs()
    if latest_id is None:
        return {
            "labels": [],
            "current_visited": [],
            "current_not": [],
            "prev_visited": [],
            "prev_not": [],
            "has_prev": False,
        }

    _, summary_curr = _load_run(latest_id)
    summary_prev = None
    if prev_id is not None:
        _, summary_prev = _load_run(prev_id)

    if summary_curr is None or summary_curr.empty:
        return {
            "labels": [],
            "current_visited": [],
            "current_not": [],
            "prev_visited": [],
            "prev_not": [],
            "has_prev": False,
        }

    def filter_df(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if df is None or df.empty:
            return None

        if scope == "sector" and sector:
            key = sector.lower()
            munis = SECTOR_MUNIS.get(key)
            if not munis:
                return df.iloc[0:0]
            return df[df[COL_MUNICIPALITY_MIN].isin(munis)]

        if scope == "municipality" and municipality:
            muni_name = unquote(municipality)
            return df[df[COL_MUNICIPALITY_MIN] == muni_name]

        return df

    summary_curr = filter_df(summary_curr)
    summary_prev = filter_df(summary_prev)

    def group_by_type(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(
                columns=["تمت الزيارة", "لم تزار"],
                index=pd.Index([], name="التصنيف"),
            )
        return df.groupby("التصنيف")[["تمت الزيارة", "لم تزار"]].sum()

    g_curr = group_by_type(summary_curr)
    g_prev = group_by_type(summary_prev)

    has_prev = summary_prev is not None and not summary_prev.empty

    all_cats = sorted(set(g_curr.index) | set(g_prev.index))

    current_visited = []
    current_not = []
    prev_visited = []
    prev_not = []

    for cat in all_cats:
        if cat in g_curr.index:
            current_visited.append(int(g_curr.loc[cat, "تمت الزيارة"]))
            current_not.append(int(g_curr.loc[cat, "لم تزار"]))
        else:
            current_visited.append(0)
            current_not.append(0)

        if has_prev and cat in g_prev.index:
            prev_visited.append(int(g_prev.loc[cat, "تمت الزيارة"]))
            prev_not.append(int(g_prev.loc[cat, "لم تزار"]))
        else:
            prev_visited.append(0)
            prev_not.append(0)

    return {
        "labels": all_cats,
        "current_visited": current_visited,
        "current_not": current_not,
        "prev_visited": prev_visited,
        "prev_not": prev_not,
        "has_prev": has_prev,
    }


# ================= API: تفاصيل بلدية ==================
@app.get("/api/municipality/{muni_name}/details")
def get_municipality_details(muni_name: str):
    """
    يرجع بيانات بلدية معيّنة (أحدث تشغيل):
      - raw: سجلات زيارات مكين لهذه البلدية (الزيارات وحالاتها فقط)
      - summary: ملخص مجمّع حسب التصنيف لهذه البلدية
    """
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
            {"error": "تعذّر تحميل بيانات أحدث تشغيل."},
            status_code=500,
        )

    # نشتغل على نسخة منفصلة لكل بلدية
    df_muni_raw = raw_df[raw_df[COL_MUNICIPALITY_MIN] == muni_name].copy()
    df_muni_sum = summary_df[summary_df[COL_MUNICIPALITY_MIN] == muni_name].copy()

    # 🔹 "البيانات الخام" = زيارات مكين فقط
    # أي صف ما فيه "حالة الزيارة" → من قالب الوزارة بدون زيارة فعلية → نشيله
    if "حالة الزيارة" in df_muni_raw.columns:
        df_muni_raw = df_muni_raw[df_muni_raw["حالة الزيارة"].notna()].copy()

    if df_muni_raw.empty and df_muni_sum.empty:
        return JSONResponse(
            {"error": f"لا توجد بيانات للبلدية {muni_name} في أحدث تشغيل."},
            status_code=404,
        )

    try:
        raw_records = _df_to_json_records(df_muni_raw)
        summary_records = _df_to_json_records(df_muni_sum)
    except Exception as e:
        return JSONResponse(
            {
                "error": "خطأ غير متوقع في تفاصيل البلدية.",
                "detail": str(e),
            },
            status_code=500,
        )

    return {
        "municipality": muni_name,
        "raw": raw_records,
        "summary": summary_records,
    }
