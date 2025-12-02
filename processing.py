# processing.py
import pandas as pd
from io import BytesIO

# إعدادات عامة
RAW_SHEET_NAME = "1_الزيارات وحالاتها"

COL_LICENSE_RAW  = "رقم الرخصة"
COL_VISIT_STATUS = "حالة الزيارة"

COL_LICENSE_MIN      = "license_id"
COL_MUNICIPALITY_MIN = "MUNICIPALITY_EN"

SECTIONS = [
    "الصحية",
    "المباني",
    "الأسواق",
    "الايرادات",
    "الحفريات",
    "السكن الجماعي",
]

SECTORS_MAP = {
    "قطاع أبها": [
        "نطاق خدمة الامانه",
        "بلدية مدينة سلطان الفرعيه",
        "بلدية مربه الفرعيه",
        "بلدية طبب الفرعيه",
        "بلدية الشعف الفرعيه",
        "بلدية السودة الفرعيه",
        "بلدية العرين الفرعيه",
        "بلدية أحد رفيدة",
    ],
    "قطاع الخميس": [
        "بلدية الواديين",
        "بلدية خميس مشيط",
        "بلدية وادي بن هشبل",
        "بلدية طريب",
    ],
    "قطاع الشمال": [
        "بلدية النماص",
        "بلدية تنومة",
        "بلدية بلقرن",
        "بلدية بلحمر",
        "بلدية بللسمر",
        "بلدية بني عمرو",
        "بلدية البشائر",
    ],
    "قطاع الجنوب": [
        "بلدية سراة عبيده",
        "بلدية ظهران الجنوب",
        "بلدية الحرجة",
        "بلدية الامواه",
        "بلدية الفرشة",
        "بلدية الربوعة",
    ],
    "القطاع الغربي": [
        "بلدية محايل",
        "بلدية رجال المع",
        "بلدية بارق",
        "بلدية المجاردة",
        "بلدية قنا",
        "بلدية بحر ابو سكينة",
        "بلدية الساحل",
        "بلدية البرك",
    ],
}


def to_license_str(value):
    if pd.isna(value):
        return None
    try:
        return str(int(value))
    except Exception:
        return str(value)


def safe_sheet_name(name: str) -> str:
    invalid = ['\\', '/', '*', '?', ':', '[', ']']
    for ch in invalid:
        name = name.replace(ch, '_')
    return name[:31]


def safe_file_name(name: str) -> str:
    invalid = ['\\', '/', '*', '?', ':', '"', '<', '>', '|']
    for ch in invalid:
        name = name.replace(ch, '_')
    return name.strip() or "بلدية_غير_معروفة"


def build_status_column(df_merged: pd.DataFrame) -> pd.DataFrame:
    if COL_VISIT_STATUS not in df_merged.columns:
        df_merged["الحالات"] = "لم تُزر"
    else:
        df_merged["الحالات"] = df_merged[COL_VISIT_STATUS].apply(
            lambda x: "تمت الزيارة" if pd.notna(x) and str(x).strip() != "" else "لم تُزر"
        )
    return df_merged


def summarize_by_municipality(df_merged: pd.DataFrame) -> pd.DataFrame:
    df_unique = df_merged[[COL_MUNICIPALITY_MIN, "license_id_str", "الحالات"]].drop_duplicates()

    counts = df_unique.pivot_table(
        index=COL_MUNICIPALITY_MIN,
        columns="الحالات",
        values="license_id_str",
        aggfunc=pd.Series.nunique,
        fill_value=0
    )

    for col in ["تمت الزيارة", "لم تُزر"]:
        if col not in counts.columns:
            counts[col] = 0

    counts = counts.rename(columns={
        "تمت الزيارة": "عدد_تمت_الزيارة",
        "لم تُزر": "عدد_لم_تزر",
    })

    counts["إجمالي_الرخص"] = counts["عدد_تمت_الزيارة"] + counts["عدد_لم_تزر"]

    ratio = counts.apply(
        lambda r: r["عدد_تمت_الزيارة"] / r["إجمالي_الرخص"] if r["إجمالي_الرخص"] > 0 else 0,
        axis=1,
    )
    counts["النسبة"] = (ratio * 100).round(0).astype(int).astype(str) + "%"

    return counts.reset_index()


def run_pipeline_to_frames(raw_bytes: bytes, ministry_bytes: bytes):
    # raw (مكين)
    df_raw = pd.read_excel(BytesIO(raw_bytes), sheet_name=RAW_SHEET_NAME)
    df_raw["license_id_str"] = df_raw[COL_LICENSE_RAW].apply(to_license_str)

    # ministry (Asir)
    xls = pd.ExcelFile(BytesIO(ministry_bytes))
    available_sheets = set(xls.sheet_names)

    all_raw = []
    all_summary = []

    for section_name in SECTIONS:
        if section_name not in available_sheets:
            continue

        df_min = pd.read_excel(xls, sheet_name=section_name)
        df_min["license_id_str"] = df_min[COL_LICENSE_MIN].apply(to_license_str)

        df_merged = df_min.merge(
            df_raw,
            how="left",
            on="license_id_str",
            suffixes=("_وزارة", "_مكين"),
        )

        df_merged = build_status_column(df_merged)
        df_merged["التصنيف"] = section_name
        all_raw.append(df_merged)

        summary = summarize_by_municipality(df_merged)
        summary["التصنيف"] = section_name
        all_summary.append(summary)

    raw_all = pd.concat(all_raw, ignore_index=True) if all_raw else pd.DataFrame()
    summary_all = pd.concat(all_summary, ignore_index=True) if all_summary else pd.DataFrame()

    return raw_all, summary_all


def make_excel_for_municipality(raw_all: pd.DataFrame,
                                summary_all: pd.DataFrame,
                                muni_name: str) -> bytes:
    df_muni_raw = raw_all[raw_all[COL_MUNICIPALITY_MIN] == muni_name]
    df_muni_sum = summary_all[summary_all[COL_MUNICIPALITY_MIN] == muni_name]

    if df_muni_raw.empty and df_muni_sum.empty:
        return b""

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        if not df_muni_raw.empty:
            df_muni_raw.to_excel(
                writer,
                sheet_name=safe_sheet_name("البيانات_الخام"),
                index=False,
            )

        if not df_muni_sum.empty:
            for sec in df_muni_sum["التصنيف"].unique():
                df_sec = df_muni_sum[df_muni_sum["التصنيف"] == sec]
                sheet_name = safe_sheet_name(f"{sec}_ملخص")
                df_sec.to_excel(writer, sheet_name=sheet_name, index=False)

    buffer.seek(0)
    return buffer.getvalue()
