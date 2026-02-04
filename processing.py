# processing.py
import pandas as pd
from io import BytesIO

# إعدادات عامة
RAW_SHEET_NAME = "1_الزيارات وحالاتها"

# أعمدة ملف مكين (الزيارات)
COL_LICENSE_RAW  = "رقم الرخصة"
COL_VISIT_STATUS = "حالة الزيارة"

# أعمدة ملف الوزارة (قالب الرخص)
COL_LICENSE_MIN      = "license_id"
COL_MUNICIPALITY_MIN = "MUNICIPALITY_EN"

# الأقسام / التصنيفات
SECTIONS = [
    "الصحية",
    "المباني",
    "الأسواق",
    "الايرادات",
    "الحفريات",
    "السكن الجماعي",
]

# فقط لو تحتاجها في الواجهة (كما في main.py)
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

# الحالات اللي نريد استبعادها تمامًا من ملف الزيارات
BLOCK_STATUSES = [
    "بانتظار التفتيش",
    "تم حذف الزيارة من قبل المراقب",
    "ملغاه",
    "ملغاة",
]


def to_license_str(value):
    """توحيد رقم الرخصة كسلسلة نصية (بدون كسور)."""
    if pd.isna(value):
        return None
    try:
        return str(int(float(value)))
    except Exception:
        return str(value)


def safe_sheet_name(name: str) -> str:
    """اسم ورقة إكسل آمن."""
    invalid = ['\\', '/', '*', '?', ':', '[', ']']
    for ch in invalid:
        name = name.replace(ch, '_')
    return name[:31]


def safe_file_name(name: str) -> str:
    """اسم ملف آمن."""
    invalid = ['\\', '/', '*', '?', ':', '"', '<', '>', '|']
    for ch in invalid:
        name = name.replace(ch, '_')
    return name.strip() or "بلدية_غير_معروفة"


def build_status_column(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    إنشاء عمود "الحالات" في جدول الدمج:
      - "تمت الزيارة" إذا كان لـ license زيارة معتبرة.
      - "لم تُزر" إذا لا توجد زيارة معتبرة.
    ملاحظة: ملف الوزارة هو الـ Universe، فكل رخصة تظهر مرة واحدة في df_merged.
    """
    if COL_VISIT_STATUS not in df_merged.columns:
        df_merged["الحالات"] = "لم تُزر"
        return df_merged

    def _map_status(x):
        if pd.isna(x):
            return "لم تُزر"
        s = str(x).strip()
        if s in BLOCK_STATUSES:
            # المفروض تكون هذه الحالات مستبعدة من الأساس من df_raw،
            # لكن لو وصلت هنا نعاملها كأنها "لا زيارة".
            return "لم تُزر"
        # غير ذلك = تعتبر زيارة مكتملة (تمت)
        return "تمت الزيارة"

    df_merged["الحالات"] = df_merged[COL_VISIT_STATUS].apply(_map_status)
    return df_merged


def summarize_by_municipality(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    نفس منطق البايفوت في الإكسل تقريباً:

      Universe = df_merged (من ملف الوزارة بعد الدمج)
      لكل (بلدية، تصنيف):

        إجمالي_الرخص = عدد الرخص المميزة (Distinct license_id_str)
        تمت الزيارة  = عدد الرخص المميزة التي حالتها "تمت الزيارة"
        لم تزار      = إجمالي_الرخص - تمت الزيارة
    """
    if df_merged.empty:
        return pd.DataFrame(
            columns=[
                COL_MUNICIPALITY_MIN,
                "التصنيف",
                "إجمالي_الرخص",
                "تمت الزيارة",
                "لم تزار",
            ]
        )

    # نضمن وجود التصنيف
    if "التصنيف" not in df_merged.columns:
        df_merged = df_merged.copy()
        df_merged["التصنيف"] = "غير_معروف"

    # إجمالي الرخص (Distinct per بلدية + تصنيف)
    total_df = (
        df_merged
        .groupby([COL_MUNICIPALITY_MIN, "التصنيف"])["license_id_str"]
        .nunique()
        .reset_index(name="إجمالي_الرخص")
    )

    # تمت الزيارة (Distinct per بلدية + تصنيف للحالات = تمت الزيارة)
    visited_mask = df_merged["الحالات"] == "تمت الزيارة"
    visited_df = (
        df_merged[visited_mask]
        .groupby([COL_MUNICIPALITY_MIN, "التصنيف"])["license_id_str"]
        .nunique()
        .reset_index(name="تمت الزيارة")
    )

    # دمج الإجمالي مع تمت الزيارة
    summary = total_df.merge(
        visited_df,
        on=[COL_MUNICIPALITY_MIN, "التصنيف"],
        how="left",
    )
    summary["تمت الزيارة"] = summary["تمت الزيارة"].fillna(0).astype(int)

    # لم تزار = إجمالي - تمت
    summary["لم تزار"] = summary["إجمالي_الرخص"] - summary["تمت الزيارة"]
    summary["لم تزار"] = summary["لم تزار"].astype(int)

    return summary


def run_pipeline_to_frames(raw_bytes: bytes, ministry_bytes: bytes):
    """
    يشغّل البايبلاين الكامل:
      - قراءة ملف مكين (raw_today)
      - قراءة ملف الوزارة (ministry)
      - دمج على رقم الرخصة
      - حساب الحالات
      - إنتاج:
          raw_all: كل السجلات المدمجة لكل الأقسام
          summary_all: ملخص (Distinct) لكل بلدية + تصنيف
    """
    # --------- 1) raw (مكين) ---------
    df_raw = pd.read_excel(BytesIO(raw_bytes), sheet_name=RAW_SHEET_NAME)

    # توحيد رقم الرخصة
    df_raw["license_id_str"] = df_raw[COL_LICENSE_RAW].apply(to_license_str)

    # استبعاد الحالات التي لا نريد احتسابها إطلاقًا
    if COL_VISIT_STATUS in df_raw.columns:
        df_raw = df_raw[
            ~df_raw[COL_VISIT_STATUS]
            .astype(str)
            .str.strip()
            .isin(BLOCK_STATUSES)
        ]

    # --------- 2) ministry (قالب الرخص) ---------
    xls = pd.ExcelFile(BytesIO(ministry_bytes))
    available_sheets = set(xls.sheet_names)

    all_raw = []
    all_summary = []

    for section_name in SECTIONS:
        if section_name not in available_sheets:
            continue

        df_min = pd.read_excel(xls, sheet_name=section_name)
        df_min["license_id_str"] = df_min[COL_LICENSE_MIN].apply(to_license_str)

        # دمج الوزارة مع مكين على رقم الرخصة
        df_merged = df_min.merge(
            df_raw,
            how="left",
            on="license_id_str",
            suffixes=("_وزارة", "_مكين"),
        )

        # عمود الحالة الموحّد
        df_merged = build_status_column(df_merged)

        # نضيف عمود التصنيف (اسم الشيت)
        df_merged["التصنيف"] = section_name

        # نحفظ الخام
        all_raw.append(df_merged)

        # ملخص حسب البلدية + التصنيف
        summary = summarize_by_municipality(df_merged)
        all_summary.append(summary)

    raw_all = pd.concat(all_raw, ignore_index=True) if all_raw else pd.DataFrame()
    summary_all = pd.concat(all_summary, ignore_index=True) if all_summary else pd.DataFrame()

    return raw_all, summary_all


def make_excel_for_municipality(
    raw_all: pd.DataFrame,
    summary_all: pd.DataFrame,
    muni_name: str,
) -> bytes:
    """
    تكوين ملف إكسل بلدية واحدة:
      - شيت للبيانات الخام
      - شيت/شيتات للملخص لكل تصنيف
    """
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
