import csv

input_path = 'transfermarkt_Manchester United_output_2023.csv'
output_path = 'MANCHESTERUNITED_V2/transfermarkt_Manchester United_output_2023_cleaned.csv'

with open(input_path, mode='r', encoding='utf-8') as infile, \
     open(output_path, mode='w', encoding='utf-8', newline='') as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    for row in reader:
        # 첫 번째 컬럼이 ***로 시작하면 건너뛰기
        if row and not row[0].startswith('***'):
            writer.writerow(row)