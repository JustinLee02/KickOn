import pandas as pd
import boto3
import io
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # ─── 설정 ───────────────────────────────────
    bucket           = "kickon-ml-data-bucket"
    raw_prefix       = "EPL/Crawl_Data/"                # 원본 CSV가 쌓이는 S3 경로
    processed_prefix = "EPL/Crawl_Data/processed/"      # 전처리된 CSV를 모아둘 경로
    archive_prefix   = "EPL/Crawl_Data/archive/"        # 처리된 원본을 옮겨둘 경로
    combined_key     = processed_prefix + "combined.csv"# 최종 합본 파일 경로
    # ────────────────────────────────────────────

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    processed_dfs = []

    # 1) raw_prefix 아래 신규 CSV를 읽어 전처리 → processed_dfs에 저장, 원본은 archive로 이동
    for page in paginator.paginate(Bucket=bucket, Prefix=raw_prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.lower().endswith('.csv'):
                continue

            filename = key.split('/')[-1]
            print(f"▶ 처리 중: {filename}")

            # 원본 읽기
            resp = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(resp['Body'])

            # name/Name 컬럼 제거
            df = df.drop(columns=['name', 'Name'], errors='ignore')
            processed_dfs.append(df)

            # 원본을 archive로 복사 후 삭제
            dst_arch = archive_prefix + filename
            copy_source = {'Bucket': bucket, 'Key': key}
            s3.copy_object(Bucket=bucket, CopySource=copy_source, Key=dst_arch)
            s3.delete_object(Bucket=bucket, Key=key)
            print(f"  • 원본 이동 → s3://{bucket}/{dst_arch}")

    # 2) 기존 combined.csv 불러오기 (있으면), 없으면 빈 DataFrame
    try:
        resp = s3.get_object(Bucket=bucket, Key=combined_key)
        existing_df = pd.read_csv(resp['Body'], header=None)
        print("▶ 기존 combined.csv 불러옴")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            existing_df = pd.DataFrame()
            print("▶ 기존 combined.csv 없음, 새로 생성")
        else:
            raise

    # 3) 기존 합본 + 신규 데이터를 합치기
    all_dfs = processed_dfs
    if not existing_df.empty:
        all_dfs = [existing_df] + processed_dfs

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        # 4) 합본을 S3에 overwrite
        buf = io.StringIO()
        combined_df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        s3.put_object(Bucket=bucket, Key=combined_key, Body=buf.getvalue())
        print(f"▶ 전처리된 {len(processed_dfs)}개 파일을 기존 합본과 병합하여 s3://{bucket}/{combined_key}에 저장")
    else:
        print("▶ 병합할 데이터가 없습니다.")

    return {
        'statusCode': 200,
        'body': f"Processed {len(processed_dfs)} files; combined.csv updated."
    }

