import os
import re
import csv
import json
import boto3
import requests
from io import StringIO
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# transfermkt 대상 크롤링
BASE_URL = "https://www.transfermarkt.com"
START_URL = f"{BASE_URL}/laliga/startseite/wettbewerb/ES1"

# User-Agent 헤더
HEADERS = {
    "User-Agent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}

def to_ts_safe(date_str: str, fmt: str = "%b %d, %Y") -> int:
    """
    date_str 이 '-' 또는 빈 문자열이거나 포맷이 안 맞으면 0 반환,
    그렇지 않으면 Unix timestamp 반환.
    """
    if not date_str or date_str.strip() == "-":
        return 0
    try:
        return int(datetime.strptime(date_str, fmt).timestamp())
    except ValueError:
        return 0

def fetch_team_links(since_timestamp=None):
    res = requests.get(START_URL, headers=HEADERS, timeout=10)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    out = []
    for a in soup.select("td.hauptlink.no-border-links a"):
        # title 속성이 없으면 a.text.strip() 사용
        team_name = a.get("title") or a.get_text(strip=True)
        href      = a.get("href")
        full_url  = BASE_URL + href
        out.append({"team": team_name, "url": full_url})

    return out

def fetch_player_links(team_url):
    res  = requests.get(team_url, headers=HEADERS, timeout=(5, 30))
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    links = []
    
    for a in soup.select("table.inline-table td.hauptlink a"):
        href = a.get("href", "")
        
        if "/profil/spieler" in href:
            links.append(BASE_URL + href)

    return list(dict.fromkeys(links))

# ─── Session + Retry 설정 ───
session = requests.Session()
retry_strategy = Retry(
    total=5,                # 최대 3회 재시도
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def build_perf_url(player_url, competition="ES1", season="2023"):
    p = urlparse(player_url)
    perf_path = p.path.replace("/profil/", "/leistungsdatendetails/")
    perf_path = f"{perf_path}/wettbewerb/{competition}/saison/{season}"
    return BASE_URL + perf_path

def transfer_label_from_joined(joined_str: str, base_season: str) -> int:
    """
    base_season 종료일(6/30) 이후에 이적했다면 1, 아니면 0
    예: base_season="2023/24" → season_end = 2024-06-30
    """
    # (1) joined_str 예: "Jan 20, 2025" → datetime
    joined_dt = datetime.strptime(joined_str, "%b %d, %Y")

    # (2) 시즌 종료 연도 계산 ("/24" → 2024)
    end_year = 2000 + int(base_season.split("/")[1])
    season_end = datetime(end_year, 6, 30)

    return 1 if joined_dt > season_end else 0

def fetch_player_info(player_url):
    res = requests.get(player_url, headers=HEADERS, timeout=10)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    # 1) 이름
    name_tag = soup.select_one("header.data-header strong")
    name = name_tag.get_text(strip=True) if name_tag else None

    # 2) Market Value (header)
    mv_wrapper = soup.select_one("a.data-header__market-value-wrapper")
    market_value = None
    if mv_wrapper:
        last = mv_wrapper.select_one("p.data-header__last-update")
        if last:
            last.extract()
        raw_mv = mv_wrapper.get_text(strip=True)
        m = re.search(r"([\d\.,]+)", raw_mv)
        market_value = m.group(1) if m else None

    # 3)
    info_div = soup.select_one("div.spielerdatenundfakten div.info-table")
    data = {
        "name": name,
        "market_value": market_value,
    }

    # 4) Appearance & Goals & Assists
    if info_div:
        spans = info_div.select("span.info-table__content")
        for i in range(0, len(spans), 2):
            label = spans[i].get_text(strip=True).rstrip(":")
            val = spans[i+1].get_text(strip=True)

            if label == "Date of birth/Age":
                m = re.search(r"\((\d+)\)", val)
                data["age"] = int(m.group(1)) if m else None
            elif label == "Position":
                data["position"] = val
            elif label == "Market value":
                data["market_value"] = val
            elif label == "Joined":
                data["joined"] = val
            elif label == "Contract expires":
                data["contract_expires"] = val
        perf_url = build_perf_url(player_url)

        data["appearances"] = data["goals"] = data["assists"] = 0

        try:
            r2 = session.get(perf_url, headers=HEADERS, timeout=(5, 15))
            r2.raise_for_status()
        except requests.exceptions.Timeout:
            print(f"Timeout fetching performance data: {perf_url}")
            return data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching performance data: {e}")
            return data

    # 3) AJAX 상세 페이지 요청
        r2 = requests.get(perf_url, headers=HEADERS, timeout=(5, 30))
        r2.raise_for_status()
        s2 = BeautifulSoup(r2.text, "html.parser")

    # 4) table.items > tfoot > tr > td.zentriert
        tds = s2.select("table.items tfoot tr td.zentriert")
        if len(tds) > 0:
            txt = tds[0].get_text(strip=True)
            data["appearances"] = int(txt) if txt.isdigit() else 0
        if len(tds) > 1:
            txt = tds[1].get_text(strip=True)
            data["goals"] = int(txt) if txt.isdigit() else 0
        if len(tds) > 2:
            txt = tds[2].get_text(strip=True)
            data["assists"] = int(txt) if txt.isdigit() else 0

    # base_season="2023/24" 을 기준으로 이적 여부(0/1) 추가
    data["transfer"] = transfer_label_from_joined(data["joined"], base_season="2023/24")
    return data

BUCKET          = "kickon-ml-data-bucket"
PROGRESS_KEY    = "transfer_progress/progress.json"
RESULTS_PREFIX  = "EPL/Crawl_Data/"

s3        = boto3.client("s3")

TEAM_RANKINGS = {
    "Real Madrid":              1,
    "FC Barcelona":             2,
    "Girona FC":                3,
    "Atlético de Madrid":       4,
    "Athletic Bilbao":          5,
    "Real Sociedad":            6,
    "Real Betis Balompié":      7,
    "Villarreal CF":            8,
    "Valencia CF":              9,
    "Deportivo Alavés":        10,
    "CA Osasuna":              11,
    "Getafe CF":               12,
    "Celta de Vigo":           13,
    "Sevilla FC":              14,
    "RCD Mallorca":            15,
    "UD Las Palmas":           16,
    "Rayo Vallecano":          17,
    "Cadiz CF":                18,
    "UD Almería":              19,
    "Granada CF":              20,
}

POSITION_MAPPING = {
    "Goalkeeper": 0,
    "Defender":   1,
    "Midfield":   2,
    "Attack":     3,
}

def map_position(pos_str: str) -> int:
    ps = pos_str.lower()
    for key, val in POSITION_MAPPING.items():
        if key.lower() in ps:
            return val
    return -1
    
def load_progress():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=PROGRESS_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {"team_idx": 0, "player_idx": 0}

def save_progress(prog: dict):
    s3.put_object(
        Bucket=BUCKET,
        Key=PROGRESS_KEY,
        Body=json.dumps(prog).encode("utf-8"),
        ContentType="application/json"
    )

def save_team_results_csv(team_idx: int, team_name: str, data: list[dict]):
    """
    team_{team_idx}_{team_name}.csv
    컬럼(순서):
      transfer, market_value, position, joined_ts, expires_ts,
      appearances, goals, assists, team_rank
    """
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "transfer",
        "name",
        "age",
        "market_value",
        "position",
        "joined_ts",
        "expires_ts",
        "appearances",
        "goals",
        "assists",
        "team_rank",
    ])

    team_rank = TEAM_RANKINGS.get(team_name, 0)

    for rec in data:
        joined_ts  = to_ts_safe(rec.get("joined", ""))
        expires_ts = to_ts_safe(rec.get("contract_expires", ""))
        writer.writerow([
            rec["transfer"],
            rec.get("name", ""),
            rec.get("age", 0),
            rec["market_value"],
            map_position(rec["position"]),
            joined_ts,
            expires_ts,
            rec["appearances"],
            rec["goals"],
            rec["assists"],
            team_rank,
        ])

    key = f"{RESULTS_PREFIX}team_{team_idx:04d}_{team_name}.csv"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

def append_player_to_csv(team_idx: int, team_name: str, rec: dict):
    key = f"{RESULTS_PREFIX}team_{team_idx:04d}_{team_name}.csv"
    # 1) 기존 파일 읽어오기 (헤더+이미 있는 데이터)
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        text = obj["Body"].read().decode("utf-8")
        buf = StringIO(text)
        rows = list(csv.reader(buf))
        buf = StringIO()
        writer = csv.writer(buf)
        for r in rows:
            writer.writerow(r)
    except s3.exceptions.NoSuchKey:
        # 파일이 없으면 헤더부터 새로 생성
        buf = StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "transfer","name","age","market_value","position",
            "joined_ts","expires_ts",
            "appearances","goals","assists","team_rank"
        ])
    # 2) 새 플레이어 한 줄 추가
    joined_ts  = to_ts_safe(rec.get("joined", ""))
    expires_ts = to_ts_safe(rec.get("contract_expires", ""))
    team_rank  = TEAM_RANKINGS.get(team_name, 0)

    writer.writerow([
        rec["transfer"],
        rec.get("name", ""),
        rec.get("age", 0),
        rec["market_value"],
        map_position(rec["position"]),
        joined_ts,
        expires_ts,
        rec["appearances"],
        rec["goals"],
        rec["assists"],
        team_rank,
    ])

    # 3) S3에 다시 쓰기
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

def lambda_handler(event, context):
    if event.get("reset"):
        save_progress({"team_idx":0, "player_idx":0})
        return {"statusCode":200, "body":"✅ progress reset"}
    
    # 진행 상태 로드
    prog   = load_progress()  # {"team_idx": X, "player_idx": Y}
    ti, pi = prog["team_idx"], prog["player_idx"]
    teams  = fetch_team_links()
    if ti >= len(teams):
        return {"statusCode":200,"body":"✅ all teams done"}

    team      = teams[ti]
    team_name = team["team"]
    players   = fetch_player_links(team["url"])

    # 플레이어 하나씩
    for idx, url in enumerate(players):
        if idx < pi:
            continue

        try:
            info = fetch_player_info(url)
            info["team"] = team_name

            # 바로 CSV에 append
            append_player_to_csv(ti, team_name, info)
            print(f"[INFO] Team#{ti}({team_name}) ▶ Player#{idx} '{info['name']}' 저장완료")

            # 진행 상태 업데이트 (다음 플레이어부터)
            save_progress({"team_idx": ti, "player_idx": idx+1})

        except Exception as e:
            print(f"[ERROR] Team#{ti}({team_name}) ▶ Player#{idx} 실패: {e}")
            # 실패해도 인덱스만 기록하고 함수 종료
            save_progress({"team_idx": ti, "player_idx": idx})
            return {"statusCode":500, "body":f"Error at player #{idx}"}

    # 한 팀 끝나면 다음 팀으로
    save_progress({"team_idx": ti+1, "player_idx": 0})
    return {
        "statusCode":200,
        "body":f"✅ processed team #{ti}: {team_name}"
    }

