import os
import re
import json
import openai
import requests

# ─────────────────────────────────────
# 환경 변수 및 상수 설정
openai.api_key = os.getenv("OPENAI_API_KEY")  # Lambda 환경변수로 설정하세요
STATE_FILE = "/tmp/used_titles.json"

API_BASE  = "https://api-dev.kickon.net/api"
BOARD_URL = f"{API_BASE}/board"
TOKEN     = os.getenv("BACKEND_JWT")          # Lambda 환경변수로 설정하세요

SYSTEM_PROMPT = """
당신은 축구 커뮤니티용 AI 가상 사용자입니다.
다음 조건을 반드시 준수하여 축구 상식과 토론 주제 5개를 JSON 배열로만 반환하세요:

[
  { "team": 37, "title": "…", "contents": "…"},
  { "team": 37, "title": "…", "contents": "…"},
  { "team": 37, "title": "…", "contents": "…"},
  { "team": 37, "title": "…", "contents": "…"},
  { "team": 37, "title": "…", "contents": "…"}
]

1. 처음 세 개는 축구 상식, 마지막 두 개는 토론형 질문.
2. title 20자 이내, contents 2문장 이내, 모두 한국어.
3. team 값은 모두 37 고정.
4. JSON 형식 엄격 준수, 마지막 쉼표 금지.
5. 이전에 생성한 제목은 다시 사용하지 마세요.
""".strip()
# ─────────────────────────────────────

def _safe_parse_array(text: str):
    start = text.find('[')
    end   = text.rfind(']')
    if start == -1 or end == -1:
        raise ValueError("JSON 배열 구분자 '[' 또는 ']'가 없습니다")
    snippet = text[start:end+1]
    return json.loads(re.sub(r',\s*(\])', r'\1', snippet))

def load_used_titles():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return []

def save_used_titles(titles):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(titles, f, ensure_ascii=False, indent=2)

def clear_used_titles():
    save_used_titles([])
    return {"cleared": True}

def generate_posts_excluding_previous():
    used = load_used_titles()
    ban_list = ""
    if used:
        ban_list = "\n금지할 이전 제목:\n- " + "\n- ".join(used)
    prompt = SYSTEM_PROMPT + ban_list + "\n\n위 제목들은 절대 다시 사용하지 마세요.\n"

    resp = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        temperature=0.8,
        max_tokens=500,
        messages=[{"role":"system","content":prompt}],
    )
    raw = resp.choices[0].message.content
    posts = _safe_parse_array(raw)

    save_used_titles(used + [p["title"] for p in posts])
    return posts

def post_to_board(post: dict):
    orig = post["contents"].rstrip()
    final_contents = orig + "\n\n\n\n\n이 글은 AI 가상 사용자에 의해 작성되었습니다."
    body = {
        "team":     post["team"],
        "title":    post["title"],
        "contents": final_contents
    }
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type":  "application/json"
    }
    return requests.post(BOARD_URL, headers=headers, json=body)

def lambda_handler(event, context):
    """
    event = { "action": "generate" } → 글 생성 & 업로드
    event = { "action": "clear" }    → used_titles.json 초기화
    """
    action = event.get("action", "generate")
    if action == "clear":
        result = clear_used_titles()
        return {
            "statusCode": 200,
            "body": json.dumps(result, ensure_ascii=False)
        }

    try:
        posts = generate_posts_excluding_previous()
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"AI 생성 오류: {e}"
        }

    results = []
    for p in posts:
        resp = post_to_board(p)
        results.append({
            "title":    p["title"],
            "status":   resp.status_code,
            "response": resp.text
        })

    return {
        "statusCode": 200,
        "body": json.dumps(results, ensure_ascii=False)
    }
