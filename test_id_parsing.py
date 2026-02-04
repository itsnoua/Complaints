
import pandas as pd

def to_license_str_original(value):
    if pd.isna(value):
        return None
    try:
        return str(int(value))
    except Exception:
        return str(value)

def to_license_str_fixed(value):
    if pd.isna(value):
        return None
    try:
        # Try converting to float first to handle "12345.0" string
        f = float(value)
        return str(int(f))
    except Exception:
        return str(value)

test_values = [
    12345,
    12345.0,
    "12345",
    "12345.0",
    "abc",
    None,
    float("nan")
]

print(f"{'Value':<15} | {'Original':<15} | {'Fixed':<15}")
print("-" * 50)
for v in test_values:
    orig = to_license_str_original(v)
    fixed = to_license_str_fixed(v)
    print(f"{str(v):<15} | {str(orig):<15} | {str(fixed):<15}")
