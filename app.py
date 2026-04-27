import os
import re
import json
import time
import sqlite3
import hashlib
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
from requests_oauthlib import OAuth1Session

JST = timezone(timedelta(hours=9))
DB_PATH = "bot.db"

JPX_DISCLOSURE_URL = "https://www.release.tdnet.info/inbs/I_list_001_{}.html"
EDINET_LIST_URL = "https://disclosure.edinet-fsa.go.jp/api/v2/documents.json"
X_POST_URL = "https://api.x.com/2/tweets"

KEYWORDS = {
    "up_revision": ["業績予想の修正", "上方修正", "通期業績予想の修正", "連結業績予想の修正"],
    "down_revision": ["下方修正", "業績予想の修正"],
    "buyback": ["自己株式取得", "自己株式の取得", "自社株買い"],
    "dividend_up": ["増配", "配当予想の修正"],
    "dividend_down": ["減配", "配当予想の修正"],
    "split": ["株式分割"],
    "ma": ["株式取得", "子会社化", "合併", "会社分割", "事業譲受", "公開買付"],
}

POSITIVE_HINTS = ["上方修正", "増配", "自己株式取得", "自社株買い", "株式分割"]
NEGATIVE_HINTS = ["下方修正", "減配", "赤字", "特別損失", "監理", "継続企業の前提"]

EDINET_LARGE_HOLDER_FORMS = {"大量保有報告書", "変更報告書", "訂正大量保有報告書", "訂正変更報告書"}


def get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS posted_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            external_id TEXT NOT NULL,
            posted_at TEXT NOT NULL,
            UNIQUE(source, external_id)
        )
        """
    )
    conn.commit()
    conn.close()


def already_posted(source: str, external_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM posted_items WHERE source = ? AND external_id = ? LIMIT 1",
        (source, external_id),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_posted(source: str, external_id: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO posted_items(source, external_id, posted_at) VALUES (?, ?, ?)",
        (source, external_id, datetime.now(JST).isoformat()),
    )
    conn.commit()
    conn.close()


def normalize_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text

def extract_numbers(text: str) -> str:
    if not text:
        return ""

    patterns = [
        r"\d+\.?\d*％",
        r"\d+\.?\d*%",
        r"\d+[億万千円]+",
        r"\d+\.?\d*倍",
    ]

    found = []
    for p in patterns:
        matches = re.findall(p, text)
        found.extend(matches)

    found = list(dict.fromkeys(found))

    if not found:
        return ""

    return " / ".join(found[:3])


def infer_label(title: str) -> str:
    title = title or ""
    if any(k in title for k in POSITIVE_HINTS):
        return "ポジ"
    if any(k in title for k in NEGATIVE_HINTS):
        return "ネガ"
    return "中立"


def infer_event_type(title: str) -> Optional[str]:
    for event_type, words in KEYWORDS.items():
        if any(w in title for w in words):
            if event_type == "dividend_up" and "減配" in title:
                continue
            if event_type == "dividend_down" and "増配" in title:
                continue
            return event_type
    return None


def short_event_text(event_type: str, title: str) -> str:
    mapping = {
        "up_revision": "業績見通しを引き上げ。",
        "down_revision": "業績見通しを引き下げ。",
        "buyback": "自社株買いを発表。",
        "dividend_up": "配当方針の引き上げを発表。",
        "dividend_down": "配当方針の引き下げを発表。",
        "split": "株式分割を発表。",
        "ma": "M&A関連の開示を発表。",
        "large_holder": "大量保有報告書関連の提出を確認。",
    }
    return mapping.get(event_type, title)


def implication_text(label: str, event_type: str) -> str:
    if event_type == "buyback":
        return "株主還元強化として短期需給はポジ寄り。"
    if event_type == "up_revision":
        return "業績期待の見直しが入りやすく、短期は注目。"
    if event_type == "down_revision":
        return "期待剥落で短期はネガティブ反応に注意。"
    if event_type == "dividend_up":
        return "還元強化として評価されやすい内容。"
    if event_type == "dividend_down":
        return "還元後退として嫌気されやすい内容。"
    if event_type == "split":
        return "流動性改善期待で個人投資家の注目を集めやすい。"
    if event_type == "ma":
        return "案件の規模と条件次第で評価が分かれやすい。"
    if event_type == "large_holder":
        return "需給や思惑に影響しやすく、継続監視向き。"
    return "追加開示や市場の初期反応を確認したい内容。"


def build_post_text(code: str, company: str, title: str, event_type: str, source_url: str) -> str:
    label = infer_label(title)

    event_names = {
        "up_revision": "上方修正",
        "down_revision": "下方修正",
        "buyback": "自社株買い",
        "dividend_up": "増配",
        "dividend_down": "減配",
        "split": "株式分割",
        "ma": "M&A",
        "large_holder": "大量保有",
    }

    event_name = event_names.get(event_type, "IR")

    point = {
        "up_revision": "通期業績予想の引き上げを発表。",
        "down_revision": "通期業績予想の引き下げを発表。",
        "buyback": "自己株式取得を発表。",
        "dividend_up": "配当予想の引き上げを発表。",
        "dividend_down": "配当予想の引き下げを発表。",
        "split": "株式分割を発表。",
        "ma": "M&A関連の開示を発表。",
        "large_holder": "大量保有報告書関連の提出を確認。",
    }.get(event_type, normalize_text(title))

    impact = implication_text(label, event_type)
    numbers = extract_numbers(title)
    body = (
        f"【{label}/{event_name}】{code} {company}\n"
        f"・内容：{point}{f'（{numbers}）' if numbers else ''}\n"
        f"・注目点：{impact}\n"
        f"・開示：{normalize_text(title)}\n"
        f"{source_url}\n"
        f"#日本株 #{code}"
    )

    if len(body) > 280:
        body = (
            f"【{label}/{event_name}】{code} {company}\n"
            f"・内容：{point}\n"
            f"・注目点：{impact}\n"
            f"#日本株 #{code}"
        )

    return body[:280]


def fetch_jpx_html(date_jst: datetime) -> str:
    date_str = date_jst.strftime("%Y%m%d")
    url = JPX_DISCLOSURE_URL.format(date_str)
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.text


def parse_jpx_items(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    items = []

    for tr in soup.select("table tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        time_text = normalize_text(tds[0].get_text(" ", strip=True))
        code_text = normalize_text(tds[1].get_text(" ", strip=True))
        company_text = normalize_text(tds[2].get_text(" ", strip=True))
        title_text = normalize_text(tds[3].get_text(" ", strip=True))

        a = tds[3].find("a")
        href = a.get("href") if a else None
        if href and href.startswith("/"):
            href = f"https://www.release.tdnet.info{href}"

        if not code_text or not title_text:
            continue

        event_type = infer_event_type(title_text)
        if not event_type:
            continue

        external_id = hashlib.sha256(f"JPX|{code_text}|{title_text}|{href}".encode()).hexdigest()
        items.append(
            {
                "source": "JPX",
                "external_id": external_id,
                "time": time_text,
                "code": code_text,
                "company": company_text,
                "title": title_text,
                "url": href or "https://www.release.tdnet.info/",
                "event_type": event_type,
            }
        )
    return items


def fetch_edinet_documents(target_date: datetime, api_key: str) -> List[Dict]:
    params = {
        "date": target_date.strftime("%Y-%m-%d"),
        "type": 2,
        "Subscription-Key": api_key,
    }
    headers = {"Accept": "application/json"}
    resp = requests.get(EDINET_LIST_URL, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def parse_edinet_items(docs: List[Dict]) -> List[Dict]:
    items = []
    for d in docs:
        doc_type = normalize_text(d.get("docDescription", ""))
        if not any(name in doc_type for name in EDINET_LARGE_HOLDER_FORMS):
            continue

        filer = normalize_text(d.get("filerName", ""))
        sec_code = normalize_text(d.get("secCode", ""))
        doc_id = d.get("docID")
        if not filer or not sec_code or not doc_id:
            continue

        title = doc_type
        url = f"https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp?uji.verb=W1E63071EdinetCode&uji.bean=ee.bean.W1E63071Bean&TID=W1E63071&PID=W0ZE0101&lgKbn=2&dflg=0&iflg=0&edinetCode={d.get('edinetCode','')}"
        items.append(
            {
                "source": "EDINET",
                "external_id": doc_id,
                "time": normalize_text(d.get("submitDateTime", "")),
                "code": sec_code,
                "company": filer,
                "title": title,
                "url": url,
                "event_type": "large_holder",
            }
        )
    return items


def post_to_x(text: str) -> Dict:
    ...
    return resp.json()


# 👇ここに貼る
def force_test_post():
    api_key = os.getenv("X_API_KEY")
    api_secret = os.getenv("X_API_SECRET")
    access_token = os.getenv("X_ACCESS_TOKEN")
    access_token_secret = os.getenv("X_ACCESS_TOKEN_SECRET")

    oauth = OAuth1Session(
        client_key=api_key,
        client_secret=api_secret,
        resource_owner_key=access_token,
        resource_owner_secret=access_token_secret,
    )

    text = "【テスト投稿】IRボット接続確認"

    resp = oauth.post(
        "https://api.x.com/2/tweets",
        json={"text": text},
        timeout=30
    )

    print("=== TEST POST RESULT ===")
    print(resp.status_code, resp.text)
    print("========================")

    oauth = OAuth1Session(
        client_key=api_key,
        client_secret=api_secret,
        resource_owner_key=access_token,
        resource_owner_secret=access_token_secret,
    )

    resp = oauth.post(X_POST_URL, json={"text": text}, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(f"X post failed: {resp.status_code} {resp.text}")
    return resp.json()


def should_post_now(item: Dict) -> bool:
    title = item["title"]

    skip_words = ["訂正", "数値データ訂正", "補足資料", "説明資料"]
    if any(w in title for w in skip_words):
        return False

    return True


def collect_items() -> List[Dict]:
    items = []
    now = datetime.now(JST)

    for dt in [now, now - timedelta(days=1)]:
        try:
            html = fetch_jpx_html(dt)
            items.extend(parse_jpx_items(html))
        except Exception as e:
            print(f"[WARN] JPX fetch failed for {dt.date()}: {e}")

    try:
        api_key = get_env("EDINET_API_KEY")
        docs = fetch_edinet_documents(now, api_key)
        items.extend(parse_edinet_items(docs))
    except Exception as e:
        print(f"[WARN] EDINET fetch failed: {e}")

    unique = {}
    for item in items:
        unique[(item["source"], item["external_id"])] = item
    return list(unique.values())


def main() -> None:
    init_db()
    items = collect_items()

    print("=== DEBUG ===")
    print(f"collected_items: {len(items)}")

    if items:
        print("sample_items:")
        for item in items[:5]:
            print(json.dumps(item, ensure_ascii=False))
    else:
        print("no items collected")
    print("=============")

    items = sorted(items, key=lambda x: (x.get("time", ""), x["code"], x["title"]))

    posted_count = 0
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
    max_posts = int(os.getenv("MAX_POSTS_PER_RUN", "3"))

    print(f"dry_run: {dry_run}")
    print(f"max_posts: {max_posts}")

    for item in items:
        print(f"checking: {item['code']} {item['company']} {item['title']} / event_type={item['event_type']}")

        if posted_count >= max_posts:
            print("skip: max_posts reached")
            break

        if already_posted(item["source"], item["external_id"]):
            print("skip: already posted")
            continue

        if not should_post_now(item):
            print("skip: should_post_now false")
            continue

        text = build_post_text(
            code=item["code"],
            company=item["company"],
            title=item["title"],
            event_type=item["event_type"],
            source_url=item["url"],
        )

        print("=" * 80)
        print(text)
        print("=" * 80)

        if dry_run:
            mark_posted(item["source"], item["external_id"])
            posted_count += 1
            continue

        try:
            result = post_to_x(text)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            mark_posted(item["source"], item["external_id"])
            posted_count += 1
            time.sleep(2)
        except Exception as e:
            print(f"[ERROR] failed posting: {e}")

    items = sorted(items, key=lambda x: (x.get("time", ""), x["code"], x["title"]))

    posted_count = 0
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
    max_posts = int(os.getenv("MAX_POSTS_PER_RUN", "3"))

    for item in items:
        if posted_count >= max_posts:
            break

        if already_posted(item["source"], item["external_id"]):
            continue

        if not should_post_now(item):
            continue

        text = build_post_text(
            code=item["code"],
            company=item["company"],
            title=item["title"],
            event_type=item["event_type"],
            source_url=item["url"],
        )

        print("=" * 80)
        print(text)
        print("=" * 80)

        if dry_run:
            mark_posted(item["source"], item["external_id"])
            posted_count += 1
            continue

        try:
            result = post_to_x(text)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            mark_posted(item["source"], item["external_id"])
            posted_count += 1
            time.sleep(2)
        except Exception as e:
            print(f"[ERROR] failed posting: {e}")


if __name__ == "__main__":
    main()