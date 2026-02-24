import csv
import os

# --- Configuration ---
source_folder = '/Users/abisheka.vc/Downloads'
csv_filename = 'SPLIT.csv'
output_filename = 'update_fix1.sql'
# ---------------------

csv_file_path = os.path.join(source_folder, csv_filename)
output_file_path = os.path.join(source_folder, output_filename)

if not os.path.exists(csv_file_path):
    print(f"Error: Could not find {csv_file_path}")
else:
    print(f"Reading {csv_filename} as a Tab-Separated file...")

    queries = []
    row_count = 0

    try:
        with open(csv_file_path, mode='r', encoding='utf-8-sig') as f:
            # Added delimiter='\t' to handle the tabs in your file
            reader = csv.DictReader(f, delimiter='\t')

            # Clean up field names to remove any stray spaces
            if reader.fieldnames:
                reader.fieldnames = [name.strip() for name in reader.fieldnames]

            for row in reader:
                # Use .get() and .strip() for safety
                record_id = row.get('id', '').strip()
                vendor_id = row.get('vendor_pickup_location_id', '').strip()

                if record_id and vendor_id:
                    # Construct the JSON string
                    json_val = f'{{"vendorPickupLocationId":"{vendor_id}"}}'

                    # Construct the SQL
                    query = f"update pickup_location set vendor_pickup_location_id = '{json_val}', registration_status = 'REGISTERED' where id = {record_id};"

                    queries.append(query)
                    row_count += 1

        if row_count > 0:
            with open(output_file_path, 'w', encoding='utf-8') as f:
                for q in queries:
                    f.write(q + "\n")
            print("------------------------------------------------------")
            print(f"SUCCESS! Processed {row_count} rows.")
            print(f"File saved to: {output_file_path}")
            print("------------------------------------------------------")
        else:
            print("Error: Still could not map the columns. Please check header spelling.")

    except Exception as e:
        print(f"An error occurred: {e}")
