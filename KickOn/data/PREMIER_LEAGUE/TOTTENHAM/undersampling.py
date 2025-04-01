import pandas as pd

df = pd.read_csv("tottenham_merged.csv")  # 실제 파일 경로로 변경

# Transfer가 1인 데이터는 모두 유지
df_transfer1 = df[df["Transfer"] == 1]

# Transfer가 0인 데이터 중 일부만 샘플링
df_transfer0 = df[df["Transfer"] == 0].sample(frac=0.7, random_state=42)

# 두 데이터프레임 결합
df_balanced = pd.concat([df_transfer1, df_transfer0])

# 결과를 새 CSV 파일로 저장
df_balanced.to_csv("tottenham_balanced_data.csv", index=False)