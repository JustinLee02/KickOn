import csv
from datetime import datetime
import os

# 2024년 기준 이적 여부를 찾기 위해 2025년~ 합류한 선수들은 데이터 셋에서 제외

def filter_year(input_file, output_file, cut_off):
    with open(input_file, newline='', encoding='utf-8') as csv_in, \
    open(output_file, "w", newline="", encoding='utf-8') as csv_out:

        reader = csv.DictReader(csv_in)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(csv_out, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            joined_str = row.get('Joined', "").strip()
            if not joined_str:
                continue
            try:
                joined_date = datetime.strptime(joined_str, "%b %d, %Y")
            except Exception as e:
                print("Error", e)
                continue

            if joined_date < cut_off:
                writer.writerow(row)
    print(f"Filtered CSV saved as {output_file}")

if __name__ == "__main__":
    input_path = os.path.expanduser('/data/ULSAN_HD_FC/transfermarkt_ULSAN_HD_FC_output.csv')
    output_path = 'filtered_ULSAN.csv'

    filter_year(input_path, output_path, datetime(2025, 1, 1))
