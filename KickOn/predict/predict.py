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

# ─────────────────────────────────────
# 설정
BASE_URL = "https://www.transfermarkt.com"
openai.api_key = ""


SM_RUNTIME = boto3.client("sagemaker-runtime", region_name="ap-northeast-2")

POSITION_MAPPING = {
    "Goalkeeper": 0,
    "Defender":   1,
    "Midfield":   2,
    "Attack":     3,
}

# ─────────────────────────────────────
def search_player_requests(player_name: str) -> BeautifulSoup:
    q = quote_plus(player_name, safe='')
    url = f"{BASE_URL}/schnellsuche/ergebnis/schnellsuche?query={q}"
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def get_player_profile(player_name: str) -> dict:
    soup = search_player_requests(player_name)
    first = soup.select_one("table.items tbody tr td:nth-of-type(2) a")
    if not first:
        raise ValueError(f"No results found for '{player_name}'")
    profile_path = first["href"]
    profile_url = BASE_URL + profile_path

    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
    resp = requests.get(profile_url, headers=headers)
    resp.raise_for_status()
    prof = BeautifulSoup(resp.text, "html.parser")

    spans = prof.select("span.info-table__content--regular")
    dob = next((s for s in spans if "Date of birth/Age:" in s.get_text()), None)
    age = None
    if dob:
        raw = dob.find_next_sibling("span").get_text(strip=True)
        m = re.search(r"\((\d+)\)", raw)
        if m:
            age = int(m.group(1))

    pos_label = prof.find("span", string="Position:")
    position = pos_label.find_next_sibling("span").get_text(strip=True) if pos_label else None

    mv_elem = prof.select_one("a.data-header__market-value-wrapper")
    market_value = None
    if mv_elem:
        market_value = next((t.strip() for t in mv_elem.contents if isinstance(t, NavigableString) and t.strip()), None)

    def get_info(label: str) -> str:
        lbl = prof.find("span", string=label)
        return lbl.find_next_sibling("span").get_text(strip=True) if lbl else None

    joined = get_info("Joined:")
    contract_exp = get_info("Contract expires:")

    return {
        "name": player_name,
        "age": age,
        "position": position,
        "market_value": market_value,
        "joined": joined,
        "contract_exp": contract_exp,
        "appearance": 0,
        "goals": 0,
        "assists": 0,
        "rank": 0,
    }

def to_timestamp(date_str: str, fmt: str = "%b %d, %Y") -> int:
    dt = datetime.strptime(date_str, fmt)
    return int(dt.timestamp())

def map_position(pos_str: str) -> int:
    return POSITION_MAPPING.get(pos_str, -1)

def get_rss_summaries(player_name: str, max_entries: int = 5) -> list[dict]:
    q = quote_plus(f"{player_name} transfer rumors")
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)
    out = []
    for entry in feed.entries[:max_entries]:
        raw = entry.get("summary", "")
        summary = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
        out.append({
            "url":       entry.link,
            "summary":   summary,
            "published": entry.get("published", "")
        })
    return out


def filter_articles_by_date(articles: list[dict], start: str, end: str) -> list[dict]:
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    out = []
    for art in articles:
        try:
            pd = dateutil.parser.parse(art["published"])
            if start_dt <= pd <= end_dt:
                art["date"] = pd.date().isoformat()
                out.append(art)
        except:
            continue
    return out

def classify_with_gpt(articles: list[dict]) -> dict:
    payloads = [
        f"ARTICLE_{i+1}\n\n{a['summary']}"
        for i, a in enumerate(articles)
    ]
    user_msg = "\n\n---\n\n".join(payloads)

    print("===== USER MESSAGE BEGIN =====")
    print(user_msg)
    print("===== USER MESSAGE END =====")

    system_prompt = """
You are a football transfer prediction assistant.
For each ARTICLE_i (plain-text summary below), estimate the 0–100 probability it reports a genuine, still-possible transfer move.
Then give an overall_probability (0–100) for the transfer happening.

⚠️ Special rules:
• If any ARTICLE_i reports a recent contract extension or re-signing, reduce that ARTICLE_i’s probability by at least 30 points and bias the overall_probability downward accordingly.
• If any ARTICLE_i says the player’s current club is actively interested in keeping them (e.g. “club wants to keep,” “offer new deal”), boost that ARTICLE_i’s probability by at least 20 points and bias the overall_probability upward accordingly.
• If any ARTICLE_i contains the keyword “FA” (or “Free Agent”), boost that ARTICLE_i’s probability by at least 20 points and bias overall_probability upward accordingly.

Return ONLY valid JSON like:
{
  "per_article":[12,55,88],
  "overall_probability":67
}
""".strip()

    max_tokens = 256
    for _ in range(3):
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_msg},
            ],
        )
        content = resp.choices[0].message.content
        if resp.choices[0].finish_reason == "stop":
            try:
                data = json.loads(content)
                pa = [p/100.0 for p in data["per_article"]]
                ov = data["overall_probability"] / 100.0
                return pa, ov
            except (KeyError, json.JSONDecodeError):
                pass
        max_tokens *= 2
    return [], 0.0


def combine_fixed(base_prob, ai_score, w=0.3):
    return w * base_prob + (1 - w) * ai_score

# 최종 handler

def handler(event, context):
    # CORS 프리플라이트 요청 대응
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "*",
            },
            "body": ""
        }

    qs = event.get("queryStringParameters") or {}
    player_name = qs.get("player_name")
    if not player_name:
        return {
            "statusCode": 400,
            "body": "Missing 'player_name' in query string"
        }

    info = get_player_profile(player_name)

    features = {
        "age":        info["age"],
        "market_value": float(re.sub(r"[^\d.]", "", info["market_value"] or "0")),
        "joined_ts":  to_timestamp(info["joined"]),
        "expires_ts": to_timestamp(info["contract_exp"]),
        "appearance": info["appearance"],
        "goals":      info["goals"],
        "assists":    info["assists"],
        "rank":       info["rank"],
        "position":   map_position(info["position"]),
    }

    csv_input = ",".join(str(features.get(k, 0)) for k in [
        "age", "market_value", "joined_ts", "expires_ts",
        "appearance", "goals", "assists", "rank", "position"
    ])
    resp = SM_RUNTIME.invoke_endpoint(
        EndpointName="xgboost-kickon-endpoint-v9",
        ContentType="text/csv",
        Body=csv_input
    )
    body = resp["Body"].read()
    base_prob = float(body.decode('utf-8'))

    arts = get_rss_summaries(player_name, max_entries=5)

    per_article, ai_score = classify_with_gpt(arts)
    final_chance = combine_fixed(base_prob, ai_score, w=0.1)

    # 민서가 수정함
    resp_body = {
         "player_name":     player_name,
         "transfer_chance": final_chance,
     }

    return {
         "statusCode": 200,
         "headers": {
             "Access-Control-Allow-Origin":  "*",
             "Access-Control-Allow-Headers": "*",
             "Access-Control-Allow-Methods": "*",
         },
         "body": json.dumps(resp_body, ensure_ascii=False)
     }
