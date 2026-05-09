import os
import pandas as pd

def build_date(row):
    # 1) Try FL_DATE first
    d = pd.to_datetime(row.get("FL_DATE"), errors="coerce")
    if pd.notna(d):
        return d.strftime("%Y-%m-%d")

    # 2) Fallback to YEAR / MONTH / DAY_OF_MONTH
    try:
        y = int(row.get("YEAR"))
        m = int(row.get("MONTH"))
        day = int(row.get("DAY_OF_MONTH"))
        return pd.Timestamp(y, m, day).strftime("%Y-%m-%d")
    except:
        return None

def main():
    folder = os.path.join(os.path.dirname(__file__), "original")
    output_folder = os.path.join(os.path.dirname(__file__), "cleaned")
    os.makedirs(output_folder, exist_ok=True)
    csv_files = [f for f in os.listdir(folder) if f.endswith(".csv")]

    if not csv_files:
        print("No raw CSV files found to process.")
        return

    for input_csv in csv_files:
        output_csv = os.path.join(output_folder, "cleaned_" + input_csv)
        print(f"Processing {input_csv}...")

        df = pd.read_csv(os.path.join(folder, input_csv), low_memory=False)

        # Keep only needed columns, but also keep YEAR/MONTH/DAY_OF_MONTH as fallback
        keep_cols = [
            "YEAR",
            "MONTH",
            "DAY_OF_MONTH",
            "FL_DATE",
            "OP_UNIQUE_CARRIER",
            "OP_CARRIER_FL_NUM",
            "ORIGIN",
            "DEST",
            "CRS_DEP_TIME",
            "CRS_ARR_TIME",
            "CRS_ELAPSED_TIME",
            "ACTUAL_ELAPSED_TIME",
            "DISTANCE",
            "DEP_DELAY",
            "ARR_DELAY",
            "CANCELLED",
        ]

        missing = [c for c in keep_cols if c not in df.columns]
        if missing:
            print(f"Missing columns in {input_csv}: {missing}")
            continue

        df = df[keep_cols].copy()

        # Rename columns
        df = df.rename(columns={
            "FL_DATE": "fl_date",
            "OP_UNIQUE_CARRIER": "carrier",
            "OP_CARRIER_FL_NUM": "flight_num",
            "ORIGIN": "origin",
            "DEST": "dest",
            "CRS_DEP_TIME": "scheduled_dep_time",
            "CRS_ARR_TIME": "scheduled_arr_time",
            "CRS_ELAPSED_TIME": "scheduled_elapsed_minutes",
            "ACTUAL_ELAPSED_TIME": "actual_elapsed_minutes",
            "DISTANCE": "distance",
            "DEP_DELAY": "dep_delay",
            "ARR_DELAY": "arr_delay",
            "CANCELLED": "cancelled",
        })

        def hhmm_to_time(val):
            if pd.isna(val):
                return None
            try:
                val = int(float(val))  # handles 530, 530.0, etc.
                hours = val // 100
                minutes = val % 100

                if hours > 23 or minutes > 59:
                    return None

                return f"{hours:02d}:{minutes:02d}:00"
            except:
                return None

        df["scheduled_dep_time"] = df["scheduled_dep_time"].apply(hhmm_to_time)
        df["scheduled_arr_time"] = df["scheduled_arr_time"].apply(hhmm_to_time)

        df["flight_num"] = pd.to_numeric(df["flight_num"], errors="coerce")
        df["carrier"] = df["carrier"].fillna("").astype(str)

        # Build clean fl_date
        df["fl_date"] = df.apply(build_date, axis=1)

        # Remove rows where date could not be fixed
        df = df[df["fl_date"].notna()].copy()

        # Convert numeric columns
        num_cols = [
            "scheduled_elapsed_minutes",
            "actual_elapsed_minutes",
            "distance",
            "dep_delay",
            "arr_delay",
            "flight_num",
        ]

        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

        # Convert cancelled to boolean
        cancelled = df["cancelled"].astype(str).str.strip().str.lower()
        df["cancelled"] = cancelled.isin(["1", "true", "t", "yes", "y"])

        # Generate synthetic price
        duration = df["actual_elapsed_minutes"].fillna(df["scheduled_elapsed_minutes"]).fillna(0)
        distance = df["distance"].fillna(0)
        df["price"] = (distance * 0.10) + (duration * 0.50)
        df["price"] = df["price"].round(2)

        df["flight_name"] = df["carrier"] + " " + df["flight_num"].astype(str)

        # Drop fallback columns before saving
        df = df.drop(columns=["YEAR", "MONTH", "DAY_OF_MONTH"])

        # Save clean CSV
        df.to_csv(output_csv, index=False)
        print(f"Saved cleaned file to: {output_csv}")
        print("Rows saved:", len(df))
        print("Columns:", list(df.columns))

    print("Done.")

if __name__ == "__main__":
    main()
