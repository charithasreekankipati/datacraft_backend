import pandas as pd
def mask_value(value):
    if pd.isna(value):
        return value
    value = str(value)
    if len(value) <= 2:
        return "*" * len(value)
    return value[0] + "*" * (len(value)-2) + value[-1]


def apply_masking(df, sensitivity_map):
    df = df.copy()
    for col, label in sensitivity_map.items():
        if label != "NON_SENSITIVE":
            df[col] = df[col].apply(mask_value)
    return df
