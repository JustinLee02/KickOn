import os
import re
import requests
import dateutil.parser
import openai
import json
from bs4 import BeautifulSoup, NavigableString
from urllib.parse import quote_plus
from datetime import datetime
import feedparser
import boto3
import pandas as pd
from sklearn.metrics import accuracy_score

# ─────────────────────────────────────
# 설정
BASE_URL       = "https://www.transfermarkt.com"
openai.api_key = ""

SM_RUNTIME        = boto3.client("sagemaker-runtime", region_name="ap-northeast-2")
S3                = boto3.client("s3")
BUCKET            = "kickon-ml-data-bucket"
ARCHIVE_PREFIX = "EPL/Crawl_Data/archive/"
POSITION_MAPPING  = {"Goalkeeper":0,"Defender":1,"Midfield":2,"Attack":3}

# ─────────────────────────────────────
def search_player_requests(player_name: str) -> BeautifulSoup:
    q    = quote_plus(player_name, safe='')
    url  = f"{BASE_URL}/schnellsuche/ergebnis/schnellsuche?query={q}"
    hdrs = {"User-Agent":"Mozilla/5.0","Accept-Language":"en-US,en;q=0.9"}
    resp = requests.get(url, headers=hdrs); resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def get_player_profile(player_name: str) -> dict:
    prof = search_player_requests(player_name)
    first = prof.select_one("table.items tbody tr td:nth-of-type(2) a")
    if not first:
        raise ValueError(f"No results for '{player_name}'")
    prof_url = BASE_URL + first["href"]
    hdrs     = {"User-Agent":"Mozilla/5.0","Accept-Language":"en-US,en;q=0.9"}
    prof     = BeautifulSoup(requests.get(prof_url, headers=hdrs).text, "html.parser")

    spans = prof.select("span.info-table__content--regular")
    dob   = next((s for s in spans if "Date of birth/Age:" in s.get_text()), None)
    age   = None
    if dob:
        raw = dob.find_next_sibling("span").get_text(strip=True)
        m   = re.search(r"\((\d+)\)", raw)
        age = int(m.group(1)) if m else None

    pos_label   = prof.find("span", string="Position:")
    position    = pos_label.find_next_sibling("span").get_text(strip=True) if pos_label else None

    mv_elem     = prof.select_one("a.data-header__market-value-wrapper")
    market_value= None
    if mv_elem:
        market_value = next((t.strip() for t in mv_elem.contents
                             if isinstance(t, NavigableString) and t.strip()), None)

    def get_info(label:str):
        lbl = prof.find("span", string=label)
        return lbl.find_next_sibling("span").get_text(strip=True) if lbl else None

    return {
        "age": age,
        "position": position,
        "market_value": market_value,
        "joined": get_info("Joined:"),
        "contract_exp": get_info("Contract expires:"),
        "appearance": 0,
        "goals": 0,
        "assists": 0,
        "rank": 0,
    }

def to_timestamp(date_str: str, fmt: str = "%b %d, %Y") -> int:
    if not date_str:
        return 0
    return int(datetime.strptime(date_str, fmt).timestamp())

def map_position(pos_str: str) -> int:
    return POSITION_MAPPING.get(pos_str, -1)

def get_rss_summaries(player_name: str, max_entries: int = 5) -> list[dict]:
    q    = quote_plus(f"{player_name} transfer rumors")
    url  = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)
    out  = []
    for entry in feed.entries[:max_entries]:
        summary = BeautifulSoup(entry.get("summary",""),"html.parser").get_text(" ",strip=True)
        out.append({"summary": summary})
    return out

def classify_with_gpt(articles: list[dict]):
    payloads = [f"ARTICLE_{i+1}\n\n{a['summary']}" for i,a in enumerate(articles)]
    user_msg = "\n\n---\n\n".join(payloads)
    system_prompt = """
You are a football transfer prediction assistant.
For each ARTICLE_i, estimate the 0–100 probability it reports a genuine transfer.
Return ONLY valid JSON:
{ "per_article":[12,55,88], "overall_probability":67 }
""".strip()

    max_tokens = 256
    for _ in range(3):
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            response_format={"type":"json_object"},
            temperature=0.0,
            max_tokens=max_tokens,
            messages=[
                {"role":"system", "content":system_prompt},
                {"role":"user",   "content":user_msg},
            ],
        )
        content = resp.choices[0].message.content
        if resp.choices[0].finish_reason=="stop":
            try:
                data = json.loads(content)
                return [p/100.0 for p in data["per_article"]], data["overall_probability"]/100.0
            except:
                pass
        max_tokens *= 2
    return [], 0.0

def combine_fixed(base_prob, ai_score, w=0.1):
    return w*base_prob + (1-w)*ai_score

def handler(player_name: str) -> float:
    info = get_player_profile(player_name)
    features = {
        "age": info["age"],
        "market_value": float(re.sub(r"[^\d.]", "", info["market_value"] or "0")),
        "joined_ts": to_timestamp(info["joined"]),
        "expires_ts": to_timestamp(info["contract_exp"]),
        "appearance": info["appearance"],
        "goals": info["goals"],
        "assists": info["assists"],
        "rank": info["rank"],
        "position": map_position(info["position"]),
    }
    csv_input = ",".join(str(features[k]) for k in [
        "age","market_value","joined_ts","expires_ts",
        "appearance","goals","assists","rank","position"
    ])
    resp      = SM_RUNTIME.invoke_endpoint(
                   EndpointName="xgboost-kickon-endpoint-v9",
                   ContentType="text/csv",
                   Body=csv_input
               )
    base_prob = float(resp["Body"].read().decode())
    _, ai_score = classify_with_gpt(get_rss_summaries(player_name))
    return combine_fixed(base_prob, ai_score)

def predict_transfer(player_name: str) -> float:
    info = get_player_profile(player_name)
    features = {
        "age":         info["age"] if info["age"] is not None else 0,
        "market_value": float(re.sub(r"[^\d.]", "", info["market_value"] or "0")),
        "joined_ts":   to_timestamp(info["joined"]),    # to_timestamp 이미 None→0 처리됨
        "expires_ts":  to_timestamp(info["contract_exp"]),
        "appearance":  info["appearance"] or 0,
        "goals":       info["goals"] or 0,
        "assists":     info["assists"] or 0,
        "rank":        info["rank"] or 0,
        "position":    map_position(info["position"]),  # map_position(None)→-1
    }
    csv_input = ",".join(str(features[k]) for k in [
        "age","market_value","joined_ts","expires_ts",
        "appearance","goals","assists","rank","position"
    ])
    resp      = SM_RUNTIME.invoke_endpoint(
                   EndpointName="xgboost-kickon-endpoint-v9",
                   ContentType="text/csv",
                   Body=csv_input
               )
    base_prob = float(resp["Body"].read().decode())
    _, ai_score = classify_with_gpt(get_rss_summaries(player_name))
    return combine_fixed(base_prob, ai_score)

# ─────────────────────────────────────
# 백테스트 실행
if __name__ == "__main__":
    START_KEY = "EPL/Crawl_Data/archive/team_0009_Aston Villa.csv"
    _resume = False
    # 1) S3에서 archive 폴더 밑 CSV 목록 가져오기
    resp = S3.list_objects_v2(Bucket=BUCKET, Prefix=ARCHIVE_PREFIX)
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if not key.lower().endswith(".csv"):
            continue
            
        if not _resume:
            if key != START_KEY:
                print(f"▶ 스킵: {key}")
                continue
            else:
                print(f"▶ 여기서부터 재개: {key}")
                _resume = True


        print(f"\n=== Testing file: {key} ===")
        # 2) 해당 파일 로드
        body = S3.get_object(Bucket=BUCKET, Key=key)["Body"]
        df   = pd.read_csv(body)

        # required columns: ['name','transfer']
        y_true, y_pred = [], []
        for _, row in df.iterrows():
            name   = row["name"]
            actual = int(row["transfer"])
            try:
                prob = predict_transfer(name)
            except Exception as e:
                print(f"⚠️ {name} skipped: {e}")
                continue
            pred = 1 if prob >= 0.6 else 0
            y_true.append(actual)
            y_pred.append(pred)
            print(f"{name:20s} → prob={prob:.3f}, pred={pred}, actual={actual}")

        if y_pred:
            acc = accuracy_score(y_true, y_pred)
            print(f"\n▶ {key} 백테스트 정확도: {acc:.3f}")
        else:
            print("▶ No valid predictions for this file.")
