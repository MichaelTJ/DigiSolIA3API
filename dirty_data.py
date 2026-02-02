"""
Dirty ~1% of records in each group so Location_Status_Hash and Env_Time_Hash
no longer match the stored values (for student hash-mismatch exercises).

- Group 1: Latitude, Longitude, Inventory_Level, Shipment_Status -> Location_Status_Hash
- Group 2: Temperature, Humidity, Waiting_Time -> Env_Time_Hash

Hashes are left unchanged so they no longer match the (corrupted) data.
"""
import csv
import random

INPUT_FILE = "smart_logistics_dataset_transformed.csv"
OUTPUT_FILE = "smart_logistics_dataset_transformed.csv"
SHIPMENT_STATUSES = ("Delayed", "In Transit", "Delivered")
SEED = 42


def main():
    random.seed(SEED)

    with open(INPUT_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    n = len(rows)
    dirty_count = max(1, n // 100)  # 1% of rows

    # Group 1: corrupt Latitude, Longitude, Inventory_Level, Shipment_Status
    group1_indices = set(random.sample(range(n), dirty_count))
    for i in group1_indices:
        r = rows[i]
        # Corrupt so hash no longer matches
        r["Latitude"] = str(round(float(r["Latitude"]) + random.uniform(0.5, 2.0), 4))
        r["Longitude"] = str(round(float(r["Longitude"]) + random.uniform(0.5, 2.0), 4))
        r["Inventory_Level"] = str(int(r["Inventory_Level"]) + random.randint(1, 15))
        others = [s for s in SHIPMENT_STATUSES if s != r["Shipment_Status"]]
        r["Shipment_Status"] = random.choice(others)

    # Group 2: corrupt Temperature, Humidity, Waiting_Time
    group2_indices = set(random.sample(range(n), dirty_count))
    for i in group2_indices:
        r = rows[i]
        r["Temperature"] = str(round(float(r["Temperature"]) + random.uniform(0.3, 1.5), 1))
        r["Humidity"] = str(round(float(r["Humidity"]) + random.uniform(0.3, 1.5), 1))
        r["Waiting_Time"] = str(int(r["Waiting_Time"]) + random.randint(1, 10))

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    overlap = len(group1_indices & group2_indices)
    print(f"Dirtied {dirty_count} rows for Location group (Lat/Long/Inv/Shipment)")
    print(f"Dirtied {dirty_count} rows for Env/Time group (Temp/Humidity/Waiting)")
    print(f"Overlap: {overlap} rows corrupted in both groups")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
