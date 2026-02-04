import pandas as pd

raw_path = r"C:\Users\itsno\Desktop\ComplianceOperations\الزيارات من 1 نوفمبر الى 7 ديسمبر.xlsx"
asir_path = r"C:\Users\itsno\Desktop\ComplianceOperations\Asir الجديد.xlsx"

print("=== قراءة ملف الزيارات ===")
raw = pd.read_excel(raw_path)
print("عدد الصفوف:", len(raw))
print("الأعمدة:")
print(list(raw.columns))

print("\n=== البحث عن أعمدة البلديات في ملف الزيارات ===")
for col in raw.columns:
    if "بلد" in col or "municip" in col.lower():
        print(f"\n--- {col} ---")
        print(raw[col].dropna().unique()[:40])

print("\n\n=== قراءة ملف الوزارة (Asir) ===")
asir = pd.read_excel(asir_path)
print("عدد الصفوف:", len(asir))
print("الأعمدة:")
print(list(asir.columns))

print("\n=== البحث عن أعمدة البلديات في ملف الوزارة ===")
for col in asir.columns:
    if "بلد" in col or "municip" in col.lower():
        print(f"\n--- {col} ---")
        print(asir[col].dropna().unique()[:40])
