"""
Nepal Election Candidate News Scraper
======================================
- Select constituency → scrape ALL candidates fresh (always overwrites)
- 11 sources fired in parallel per candidate
- Article texts fetched in parallel
- Queries tuned for recency: 2079/2080 BS, recent Nepali news sites
- No resume / skip logic — selected constituencies always re-scraped clean

Install:
    pip install requests beautifulsoup4 pandas lxml

Run:
    python election_scraper.py
"""

import re
import time
import random
import urllib.parse
from math import log
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE = "candidates_list.csv"
OUTPUT_DIR = "data"
MAX_RESULTS = 32
SOURCE_WORKERS = 8
ARTICLE_WORKERS = 16
REQUEST_TIMEOUT = 12
INTER_CANDIDATE_DELAY = (1, 2)
# ─────────────────────────────────────────────────────────────────────────────

HEADERS_POOL = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "ne-NP,ne;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://duckduckgo.com/",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
        "Accept-Language": "en-GB,en;q=0.9,ne;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.bing.com/",
    },
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Accept-Language": "ne,en-US;q=0.7,en;q=0.3",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    },
]


def _new_session():
    s = requests.Session()
    s.headers.update(random.choice(HEADERS_POOL))
    return s


def safe_get(url, session=None, timeout=REQUEST_TIMEOUT):
    sess = session or _new_session()
    try:
        resp = sess.get(url, headers=random.choice(HEADERS_POOL), timeout=timeout)
        resp.raise_for_status()
        return resp
    except Exception as e:
        msg = str(e)
        if (
            "404" not in msg
            and "timed out" not in msg.lower()
            and "ConnectTimeout" not in msg
        ):
            print(f"      ⚠ {url[:65]} — {msg[:70]}")
        return None


def abs_url(href, base):
    """Always return a valid absolute URL. Handles relative, protocol-relative, and absolute."""
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    return base.rstrip("/") + "/" + href.lstrip("/")


# ── Relevance scoring ─────────────────────────────────────────────────────────
ELECTION_TERMS = {
    # Weight 4 — very specific to this election context
    "निर्वाचन २०७९": 4,
    "निर्वाचन २०८०": 4,
    "election 2079": 4,
    "election 2022": 4,
    "प्रतिनिधिसभा": 4,
    "house of representatives": 4,
    # Weight 3 — strong election signals
    "निर्वाचन": 3,
    "उम्मेदवार": 3,
    "विजयी": 3,
    "नतिजा": 3,
    "election": 3,
    "candidate": 3,
    "winner": 3,
    "result": 3,
    "elected": 3,
    # Weight 2 — supporting signals
    "मत": 2,
    "मतदान": 2,
    "vote": 2,
    "polling": 2,
    "constituency": 2,
    "क्षेत्र नं": 2,
    "parliament": 2,
    "संसद": 2,
    "campaign": 2,
    "चुनाव": 2,
    "प्रतिनिधि": 2,
    "manifesto": 2,
    # Weight 1 — weak signals
    "party": 1,
    "दल": 1,
    "seat": 1,
    "MP": 1,
    "defeated": 1,
    "सभा": 1,
}


def relevance_score(item, name_en, name_ne, const):
    text = " ".join(
        [
            item.get("title", ""),
            item.get("snippet", ""),
            item.get("content", ""),
        ]
    )
    text_lower = text.lower()

    score = 0.0
    for term, weight in ELECTION_TERMS.items():
        if term.lower() in text_lower:
            score += weight

    # Name frequency boost (log-dampened to avoid runaway scores)
    for name, boost in [(name_en.lower(), 5), (name_ne, 5)]:
        hits = text_lower.count(name.lower())
        if hits:
            score += boost * (1 + log(hits))

    # Constituency number present
    if str(const) in text:
        score += 3

    # Strong boost if name is in the title
    title_lower = item.get("title", "").lower()
    if name_en.lower() in title_lower or name_ne in item.get("title", ""):
        score += 5
    else:
        # Penalise off-topic results hard
        score *= 0.3

    return round(score, 2)


# ── Sources ───────────────────────────────────────────────────────────────────


def ddg_search(query, source_label, max_results=6):
    """DuckDuckGo HTML — most reliable, no bot detection."""
    sess = _new_session()
    encoded = urllib.parse.quote_plus(query)
    resp = safe_get(f"https://html.duckduckgo.com/html/?q={encoded}", sess)
    if not resp:
        return []
    soup, results = BeautifulSoup(resp.text, "lxml"), []
    for r in soup.select(".result__body")[:max_results]:
        title_el = r.select_one(".result__title a")
        snippet_el = r.select_one(".result__snippet")
        if not title_el:
            continue
        href = title_el.get("href", "")
        if "uddg=" in href:
            m = re.search(r"uddg=([^&]+)", href)
            href = urllib.parse.unquote(m.group(1)) if m else href
        title = title_el.get_text(strip=True)
        if title and href:
            results.append(
                {
                    "title": title,
                    "url": href,
                    "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
                    "source": source_label,
                }
            )
    return results


def nepali_times(name_en, district_en):
    base = "https://www.nepalitimes.com"
    sess = _new_session()
    q = f"{name_en} {district_en} election constituency"
    resp = safe_get(f"{base}/?s={urllib.parse.quote_plus(q)}", sess)
    if not resp:
        return []
    soup, results = BeautifulSoup(resp.text, "lxml"), []
    for a in soup.select("article h2 a, article h3 a, .entry-title a")[:4]:
        title = a.get_text(strip=True)
        url = abs_url(a.get("href", ""), base)
        if title and url:
            results.append(
                {"title": title, "url": url, "snippet": "", "source": "Nepali Times"}
            )
    return results


def onlinekhabar_en(name_en):
    base = "https://english.onlinekhabar.com"
    sess = _new_session()
    resp = safe_get(f"{base}/?s={urllib.parse.quote_plus(name_en)}", sess)
    if not resp:
        return []
    soup, results = BeautifulSoup(resp.text, "lxml"), []
    for a in soup.select(".ok-news-post h2 a, article h2 a, .entry-title a")[:4]:
        title = a.get_text(strip=True)
        url = abs_url(a.get("href", ""), base)
        if title and url:
            results.append(
                {"title": title, "url": url, "snippet": "", "source": "OnlineKhabar EN"}
            )
    return results


def ratopati(name_ne, name_en):
    base = "https://www.ratopati.com"
    sess = _new_session()
    for q in [name_ne, name_en]:
        resp = safe_get(f"{base}/search?q={urllib.parse.quote_plus(q)}", sess)
        if not resp:
            continue
        soup, results = BeautifulSoup(resp.text, "lxml"), []
        for a in soup.select("article h2 a, .news-item a, a[href*='/story/']")[:4]:
            title = a.get_text(strip=True)
            url = abs_url(a.get("href", ""), base)
            if title and url:
                results.append(
                    {"title": title, "url": url, "snippet": "", "source": "Ratopati"}
                )
        if results:
            return results
    return []


# ── Source: Reddit JSON (no API key needed) ───────────────────────────────────
REDDIT_HEADERS = {
    # Reddit requires a descriptive User-Agent — generic browser strings get 429
    "User-Agent": "script:nepal-election-research:v1.0 (by /u/researcher)",
    "Accept": "application/json",
}
# Search all Nepal subs + unrestricted global search as fallback
NEPAL_SUBREDDITS = ["Nepal", "nepalipolitics", "NepalSocial"]


def _reddit_fetch(url, sess):
    """GET Reddit JSON with correct headers. Returns parsed dict or None."""
    try:
        resp = sess.get(url, headers=REDDIT_HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 429:
            print(f"      ⚠ Reddit rate-limited — waiting 5s...")
            time.sleep(5)
            resp = sess.get(url, headers=REDDIT_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"      ⚠ Reddit {url[:60]} — {e}")
        return None


def _reddit_top_comments(sess, permalink, max_comments=8):
    """Fetch top-sorted comments from a Reddit post."""
    url = f"https://www.reddit.com{permalink}.json?limit={max_comments}&sort=top"
    data = _reddit_fetch(url, sess)
    if not data or len(data) < 2:
        return ""
    texts = []
    for c in data[1].get("data", {}).get("children", []):
        body = c.get("data", {}).get("body", "").strip()
        if body and body not in ("[deleted]", "[removed]", ""):
            texts.append(body)
    return "\n---\n".join(texts[:max_comments])


def reddit_search(name_en, name_ne, dist_en, const):
    """
    Search Reddit JSON API — no auth needed.
    Strategy:
      1. Search each Nepal subreddit with English name + election
      2. Also search globally (no restrict_sr) for Nepali name
      3. Fetch top comments for each hit (good sentiment signal)
    """
    sess = requests.Session()  # dedicated session with no extra headers
    results = []
    seen = set()

    # Query variants — most specific first
    sub_queries = [
        f"{name_en} Nepal election",
        f"{name_en} candidate",
        f"{name_ne}",
    ]

    # Pass 1: subreddit-restricted searches
    for subreddit in NEPAL_SUBREDDITS:
        for q in sub_queries:
            url = (
                f"https://www.reddit.com/r/{subreddit}/search.json"
                f"?q={urllib.parse.quote_plus(q)}"
                f"&restrict_sr=1&sort=relevance&limit=5&t=all"
            )
            data = _reddit_fetch(url, sess)
            if not data:
                continue

            posts = data.get("data", {}).get("children", [])
            for post in posts:
                p = post.get("data", {})
                title = p.get("title", "").strip()
                permalink = p.get("permalink", "")
                post_url = f"https://www.reddit.com{permalink}"
                score = p.get("score", 0)
                n_comments = p.get("num_comments", 0)
                selftext = p.get("selftext", "").strip()

                if not title or post_url in seen:
                    continue
                seen.add(post_url)

                # Only grab comments if there are any
                comments = (
                    _reddit_top_comments(sess, permalink) if n_comments > 0 else ""
                )
                content = "\n\n".join(filter(None, [selftext, comments]))

                results.append(
                    {
                        "title": f"[r/{subreddit}] {title}",
                        "url": post_url,
                        "snippet": f"↑{score}  💬{n_comments}",
                        "content": content[:5000],
                        "source": f"Reddit/r/{subreddit}",
                    }
                )

            if results:
                break  # got hits from this sub, move to next sub
        time.sleep(0.6)  # be polite between subreddit requests

    # Pass 2: global Reddit search for Nepali name (catches posts outside Nepal subs)
    if len(results) < 3:
        for q in [f"{name_ne} निर्वाचन", f"{name_en} Nepal election candidate"]:
            url = (
                f"https://www.reddit.com/search.json"
                f"?q={urllib.parse.quote_plus(q)}"
                f"&sort=relevance&limit=5&t=all"
            )
            data = _reddit_fetch(url, sess)
            if not data:
                continue
            posts = data.get("data", {}).get("children", [])
            for post in posts:
                p = post.get("data", {})
                title = p.get("title", "").strip()
                permalink = p.get("permalink", "")
                post_url = f"https://www.reddit.com{permalink}"
                score = p.get("score", 0)
                n_comments = p.get("num_comments", 0)
                selftext = p.get("selftext", "").strip()
                subreddit = p.get("subreddit", "")

                if not title or post_url in seen:
                    continue
                seen.add(post_url)

                comments = (
                    _reddit_top_comments(sess, permalink) if n_comments > 0 else ""
                )
                content = "\n\n".join(filter(None, [selftext, comments]))
                results.append(
                    {
                        "title": f"[r/{subreddit}] {title}",
                        "url": post_url,
                        "snippet": f"↑{score}  💬{n_comments}",
                        "content": content[:5000],
                        "source": f"Reddit/r/{subreddit}",
                    }
                )
            time.sleep(0.6)

    return results[:6]


# ── Article full-text ─────────────────────────────────────────────────────────
CONTENT_SELECTORS = [
    "article",
    ".article-content",
    ".entry-content",
    ".post-content",
    ".story-content",
    ".news-content",
    ".content-body",
    "main",
]


def fetch_article(item):
    url = item.get("url", "")
    if not url or item.get("content"):
        return item
    sess = _new_session()
    resp = safe_get(url, sess, timeout=15)
    if not resp:
        item["content"] = ""
        return item
    soup = BeautifulSoup(resp.text, "lxml")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form"]):
        tag.decompose()
    for sel in CONTENT_SELECTORS:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(" ", strip=True)
            if len(text) > 150:
                item["content"] = text[:5000]
                return item
    paras = [
        p.get_text(strip=True)
        for p in soup.find_all("p")
        if len(p.get_text(strip=True)) > 40
    ]
    item["content"] = " ".join(paras)[:5000]
    return item


# ── Per-candidate parallel search ─────────────────────────────────────────────
def search_candidate(row):
    name_en = row["EnglishCandidateName"]
    name_ne = row["CandidateName"]
    dist_en = row["EnglishDistrictName"]
    dist_ne = row["DistrictName"]
    const = str(row["ConstName"])

    # Election date context: 21 Falgun 2082 BS (March 5 2026)
    # Queries focused on what working sources actually return results for
    q_ne_news = f"{name_ne} उम्मेदवार निर्वाचन {dist_ne}"
    q_en_news = f"{name_en} {dist_en} Nepal election candidate"
    q_ne_cand = f"{name_ne} {dist_ne} {const} निर्वाचन २०८२"
    q_en_cand = f"{name_en} Nepal 2082 election {dist_en} constituency {const}"

    tasks = [
        # DDG variants — only ne-news and en-news consistently return results
        ("DDG ne-news", ddg_search, q_ne_news, "DDG (ne1)", 8),
        ("DDG en-news", ddg_search, q_en_news, "DDG (en1)", 8),
        ("DDG ne-cand", ddg_search, q_ne_cand, "DDG (ne2)", 6),
        ("DDG en-cand", ddg_search, q_en_cand, "DDG (en2)", 6),
        # Nepali news sites that consistently return results
        ("Nepali Times", nepali_times, name_en, dist_en),
        ("OKhabar EN", onlinekhabar_en, name_en),
        ("Ratopati", ratopati, name_ne, name_en),
        # Reddit — good for sentiment + discussion
        ("Reddit", reddit_search, name_en, name_ne, dist_en, const),
    ]

    # Round 1: all sources simultaneously
    all_raw = []
    with ThreadPoolExecutor(max_workers=SOURCE_WORKERS) as pool:
        futures = {pool.submit(fn, *args): label for label, fn, *args in tasks}
        for future in as_completed(futures):
            label = futures[future]
            try:
                res = future.result()
                icon = "✓" if res else "·"
                print(f"      {icon} {label:<18} → {len(res)}")
                all_raw.extend(res)
            except Exception as e:
                print(f"      ✗ {label:<18} → {e}")

    # Deduplicate by normalised URL
    seen, unique = set(), []
    for item in all_raw:
        key = re.sub(r"[?#].*$", "", item.get("url", "")).rstrip("/")
        if key and key not in seen:
            seen.add(key)
            unique.append(item)

    # Round 2: fetch full article texts simultaneously
    need_fetch = [i for i in unique if not i.get("snippet") and not i.get("content")]
    have_text = [i for i in unique if i.get("snippet") or i.get("content")]

    if need_fetch:
        print(f"   ⚡ Round 2 — fetching {len(need_fetch)} article(s)...")
        with ThreadPoolExecutor(max_workers=ARTICLE_WORKERS) as pool:
            fetched = list(pool.map(fetch_article, need_fetch))
    else:
        fetched = []

    all_items = have_text + fetched

    # Score, rank, trim
    for item in all_items:
        item["_score"] = relevance_score(item, name_en, name_ne, const)
    all_items.sort(key=lambda x: x["_score"], reverse=True)

    rows = []
    for item in all_items[:MAX_RESULTS]:
        snippet = item.get("snippet", "")
        content = item.get("content", snippet)
        rows.append(
            {
                "Constituency": const,
                "DistrictEnglish": dist_en,
                "DistrictNepali": dist_ne,
                "CandidateEnglish": name_en,
                "CandidateNepali": name_ne,
                "Source": item.get("source", ""),
                "Title": item.get("title", ""),
                "URL": item.get("url", ""),
                "Snippet": snippet[:500],
                "Content": content[:5000],
                "RelevanceScore": item["_score"],
                "ScrapedAt": datetime.now().isoformat(),
            }
        )
    return rows


# ── File helpers ──────────────────────────────────────────────────────────────
def slugify(text):
    text = str(text).lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s\-]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def candidate_path(dist_en, const, name_en):
    folder = slugify(f"{dist_en}_{const}")
    return Path(OUTPUT_DIR) / folder / (slugify(name_en) + ".csv")


def constituency_done_count(df, dist_en, const):
    mask = (df["EnglishDistrictName"] == dist_en) & (df["ConstName"] == const)
    cands = df[mask]
    done = sum(
        candidate_path(dist_en, const, r["EnglishCandidateName"]).exists()
        for _, r in cands.iterrows()
    )
    return done, len(cands)


# ── Constituency picker ───────────────────────────────────────────────────────
def pick_candidates(df):
    combos = (
        df[["EnglishDistrictName", "DistrictName", "ConstName"]]
        .drop_duplicates()
        .sort_values(["EnglishDistrictName", "ConstName"])
        .reset_index(drop=True)
    )
    total = len(combos)

    print("\n┌────┬──────────────────────────────┬──────┬──────────────────────┐")
    print("│  # │ Constituency                 │ Cand │ Prev scrape          │")
    print("├────┼──────────────────────────────┼──────┼──────────────────────┤")

    for i, row in combos.iterrows():
        dist_en = row["EnglishDistrictName"]
        const = row["ConstName"]
        done, n = constituency_done_count(df, dist_en, const)
        pct = int(done / n * 10) if n else 0
        bar = "█" * pct + "░" * (10 - pct)
        label = f"{dist_en.title()} - {const}"
        note = f"[{bar}] {done}/{n}"
        print(f"│{i + 1:>3} │ {label:<28} │ {n:>4} │ {note:<20} │")

    print("└────┴──────────────────────────────┴──────┴──────────────────────┘")
    print(
        f"\n  {total} constituencies  ·  selected constituencies will be FULLY RE-SCRAPED"
    )
    print("  Select:  1,3,5  |  2-10  |  1,5-8,12  |  all\n")

    while True:
        raw = input("  → ").strip().lower()
        if not raw:
            continue
        if raw == "all":
            selected = list(range(total))
            break
        try:
            selected = []
            for part in raw.split(","):
                part = part.strip()
                if "-" in part:
                    a, b = part.split("-", 1)
                    selected.extend(range(int(a) - 1, int(b)))
                else:
                    selected.append(int(part) - 1)
            oob = [i for i in selected if i < 0 or i >= total]
            if oob:
                print(f"  ⚠ Out of range: {[i + 1 for i in oob]}. Max is {total}.")
                continue
            selected = sorted(set(selected))
            break
        except ValueError:
            print("  ⚠ Invalid. Try:  1,3,5  or  2-10  or  all")

    chosen_combos = combos.iloc[selected]
    print(
        f"\n  ✔ {len(chosen_combos)} constituency/ies — will overwrite existing data:"
    )
    for _, row in chosen_combos.iterrows():
        dist_en = row["EnglishDistrictName"]
        const = row["ConstName"]
        n = len(df[(df["EnglishDistrictName"] == dist_en) & (df["ConstName"] == const)])
        print(f"    • {dist_en.title()} - {const}  ({n} candidates)")
    print()

    mask = df.apply(
        lambda r: any(
            r["EnglishDistrictName"] == sc["EnglishDistrictName"]
            and r["ConstName"] == sc["ConstName"]
            for _, sc in chosen_combos.iterrows()
        ),
        axis=1,
    )
    return df[mask].reset_index(drop=True)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    df = pd.read_csv(INPUT_FILE)
    print(f"\n📋 {len(df)} candidates loaded from {INPUT_FILE}")

    subset = pick_candidates(df)
    total = len(subset)
    done = 0

    print(f"🚀 Scraping {total} candidate(s)...\n")

    for i, (_, row) in enumerate(subset.iterrows()):
        name_en = row["EnglishCandidateName"]
        dist_en = row["EnglishDistrictName"]
        const = row["ConstName"]
        out_path = candidate_path(dist_en, const, name_en)

        print(f"\n[{i + 1}/{total}] 🔎 {name_en} | {dist_en}-{const}")
        print(f"   ⚡ Round 1 — 11 sources in parallel...")

        try:
            rows = search_candidate(row)
        except Exception as e:
            print(f"   ❌ {e}")
            rows = []

        if not rows:
            print("   ⚠ No results found.")
            rows = [
                {
                    "Constituency": const,
                    "DistrictEnglish": dist_en,
                    "DistrictNepali": row["DistrictName"],
                    "CandidateEnglish": name_en,
                    "CandidateNepali": row["CandidateName"],
                    "Source": "",
                    "Title": "",
                    "URL": "",
                    "Snippet": "",
                    "Content": "",
                    "RelevanceScore": 0,
                    "ScrapedAt": datetime.now().isoformat(),
                }
            ]
        else:
            with_text = sum(1 for r in rows if r.get("Content", "").strip())
            print(f"   ✅ {len(rows)} results  ({with_text} with full text)")

        # Always overwrite — no skip, no resume
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(out_path, index=False)
        done += 1

        time.sleep(random.uniform(*INTER_CANDIDATE_DELAY))

    print(f"\n{'─' * 60}")
    print(f"✅  Done — {done}/{total} candidates scraped fresh")

    data_path = Path(OUTPUT_DIR)
    if data_path.exists():
        print("\n── Coverage ──")
        for folder in sorted(data_path.iterdir()):
            if folder.is_dir():
                files = list(folder.glob("*.csv"))
                n = len(files)
                pct = int(n / n * 24) if n else 0
                bar = "█" * 24
                print(f"  {folder.name:<26} [{bar}] {n}/{n}")


if __name__ == "__main__":
    main()
