from __future__ import annotations

import asyncio
import csv
import logging
import random
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from playwright.async_api import BrowserContext, Page, async_playwright

INPUT_CSV = "candidates.csv"
OUTPUT_DIR = Path("election_data")
LOG_DIR = Path("logs")
POSTS_PER_CANDIDATE = 30
CONCURRENCY = 10
HEADLESS = True
USER_DATA_DIR = "./sessions"

USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

SEL_TWEET = 'article[data-testid="tweet"]'
SEL_TEXT = 'div[data-testid="tweetText"]'
SEL_USERNAME = 'div[data-testid="User-Name"]'

WALL_KEYWORDS = ("log in", "sign in", "rate limit", "something went wrong")

OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

_log_file = LOG_DIR / f"scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

semaphore = asyncio.Semaphore(CONCURRENCY)
_shutdown = asyncio.Event()


def load_done_candidates() -> set[tuple[str, str]]:
    done: set[tuple[str, str]] = set()
    for csv_path in OUTPUT_DIR.glob("*.csv"):
        try:
            existing = pd.read_csv(
                csv_path, encoding="utf-8-sig", usecols=["EnglishName", "Constituency"]
            )
            for _, r in existing.iterrows():
                done.add((csv_path.name, str(r["EnglishName"])))
        except Exception:
            pass
    return done


def prompt_district_selection(df: pd.DataFrame) -> pd.DataFrame:
    district_series: pd.Series = df["EnglishDistrictName"].astype(str)
    districts: list[str] = sorted(district_series.unique().tolist())

    print("\n" + "=" * 62)
    print("  NEPAL ELECTION TWITTER SCRAPER")
    print("=" * 62)
    print(f"\n  Available districts ({len(districts)} total):\n")

    for i, d in enumerate(districts, 1):
        mask = district_series == d
        consts = int(df.loc[mask, "FileSaveName"].nunique())
        cands = int(mask.sum())
        print(f"  {i:>3}. {d:<26} {consts} constituency/ies, {cands} candidates")

    all_opt = len(districts) + 1
    print(f"\n  {all_opt:>3}. ALL DISTRICTS (full run)\n")
    print("-" * 62)
    print("  Enter number(s) separated by commas, or a range (e.g. 3-7).")
    print(f"  Examples:  5  |  1,4,12  |  3-10  |  {all_opt} (everything)")
    print("-" * 62)

    while True:
        raw = input("\n  Your selection: ").strip()
        if not raw:
            print("  Please enter a valid selection.")
            continue

        selected: set[int] = set()
        try:
            for part in raw.split(","):
                part = part.strip()
                if "-" in part:
                    a, b = part.split("-", 1)
                    selected.update(range(int(a), int(b) + 1))
                else:
                    selected.add(int(part))
        except ValueError:
            print("  Invalid input. Use numbers, commas, and hyphens only.")
            continue

        if all_opt in selected:
            log.info("Selected ALL %d districts", len(districts))
            return df.copy()

        invalid = [n for n in selected if n < 1 or n > len(districts)]
        if invalid:
            print(f"  Out-of-range: {invalid}. Valid range: 1-{len(districts)}.")
            continue

        chosen: list[str] = [districts[n - 1] for n in sorted(selected)]
        filtered: pd.DataFrame = df[district_series.isin(chosen)].copy()
        consts_n = int(filtered["FileSaveName"].nunique())

        print(f"\n  Selected: {', '.join(chosen)}")
        print(f"     -> {consts_n} constituencies, {len(filtered)} candidates\n")
        log.info("Selected districts: %s  (%d candidates)", chosen, len(filtered))
        return filtered


def build_search_url(row: Any) -> str:
    query = str(row.PowerSearchQuery)
    return f"https://x.com/search?q={urllib.parse.quote(query)}&f=live&src=typed_query"


def jitter(lo: float = 2.5, hi: float = 6.0) -> float:
    return random.uniform(lo, hi)


async def human_scroll(page: Page, steps: int = 3) -> None:
    for _ in range(steps):
        await page.mouse.wheel(0, random.randint(350, 850))
        await asyncio.sleep(random.uniform(0.3, 0.9))


async def wait_for_tweets(page: Page, timeout_s: float = 8.0) -> None:
    try:
        await page.wait_for_selector(SEL_TWEET, timeout=int(timeout_s * 1000))
    except Exception:
        pass


async def is_walled(page: Page) -> bool:
    try:
        text = (await page.inner_text("body")).lower()
        return any(kw in text for kw in WALL_KEYWORDS)
    except Exception:
        return False


def append_rows(save_path: Path, rows: list[dict[str, str]]) -> None:
    file_exists = save_path.exists()
    with save_path.open("a", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["Candidate", "EnglishName", "Constituency", "Author", "Text"],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


async def scrape_candidate(
    row: Any,
    context: BrowserContext,
    done: set[tuple[str, str]],
) -> None:
    name = str(row.CandidateName)
    english = str(row.EnglishCandidateName)
    file_name = str(row.FileSaveName)
    label = file_name.replace(".csv", "")
    save_path = OUTPUT_DIR / file_name

    if (file_name, english) in done:
        log.info("SKIP  [%s] %s  (already scraped)", label, english)
        return

    async with semaphore:
        if _shutdown.is_set():
            return

        await asyncio.sleep(random.uniform(0.5, 3.5))

        page: Page = await context.new_page()
        await page.set_extra_http_headers(
            {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "en-US,en;q=0.9,ne;q=0.8",
            }
        )

        results: list[dict[str, str]] = []
        seen_texts: set[str] = set()

        try:
            url = build_search_url(row)
            log.info("FETCH [%s] %s", label, english)
            await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
            await wait_for_tweets(page, timeout_s=8.0)
            await asyncio.sleep(jitter(3.0, 5.5))

            if await is_walled(page):
                log.warning("WALL  [%s] %s - pausing 45s and retrying", label, english)
                await asyncio.sleep(45)
                await page.reload(wait_until="domcontentloaded", timeout=90_000)
                await wait_for_tweets(page, timeout_s=8.0)
                await asyncio.sleep(jitter(4.0, 7.0))

            stagnant = 0
            last_count = 0

            for scroll_n in range(14):
                if _shutdown.is_set():
                    break

                articles = await page.locator(SEL_TWEET).all()
                for art in articles:
                    try:
                        content = (
                            (await art.locator(SEL_TEXT).inner_text(timeout=3_000))
                            .replace("\n", " ")
                            .strip()
                        )
                        author_raw = await art.locator(SEL_USERNAME).inner_text(
                            timeout=3_000
                        )
                        if not content or content in seen_texts:
                            continue
                        seen_texts.add(content)
                        results.append(
                            {
                                "Candidate": name,
                                "EnglishName": english,
                                "Constituency": label,
                                "Author": author_raw.split("\n")[0].strip(),
                                "Text": content,
                            }
                        )
                    except Exception:
                        continue

                cur = len(results)
                if cur >= POSTS_PER_CANDIDATE:
                    results = results[:POSTS_PER_CANDIDATE]
                    break

                stagnant = stagnant + 1 if cur == last_count else 0
                last_count = cur

                if stagnant >= 3:
                    log.debug(
                        "STAG  [%s] %s after %d scrolls", label, english, scroll_n + 1
                    )
                    break

                await human_scroll(page, steps=random.randint(2, 4))
                pause = jitter(5.0, 9.0) if scroll_n % 3 == 2 else jitter(2.5, 4.5)
                await asyncio.sleep(pause)

            if results:
                append_rows(save_path, results)
                done.add((file_name, english))
                log.info("SAVE  [%s] %s -> %d tweets", label, english, len(results))
            else:
                log.warning("NONE  [%s] %s - no tweets found", label, english)

        except asyncio.CancelledError:
            log.warning("CANCEL [%s] %s", label, english)
        except Exception as exc:
            log.error("ERROR [%s] %s - %s", label, english, str(exc)[:120])
        finally:
            try:
                await page.close()
            except Exception:
                pass
            await asyncio.sleep(jitter(2.5, 5.0))


async def main() -> None:
    df_raw = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")

    for col in (
        "CandidateName",
        "EnglishCandidateName",
        "EnglishDistrictName",
        "ConstName",
        "PowerSearchQuery",
    ):
        df_raw[col] = df_raw[col].astype(str).str.strip()

    df_raw["FileSaveName"] = (
        df_raw["EnglishDistrictName"].str.lower().str.strip()
        + "_"
        + df_raw["ConstName"].astype(str).str.strip()
        + ".csv"
    )

    selected_df: pd.DataFrame = prompt_district_selection(df_raw)

    log.info("Parallelism : %d candidates at a time", CONCURRENCY)
    log.info("Output dir  : %s", OUTPUT_DIR.resolve())
    log.info("Log file    : %s", _log_file.resolve())

    done: set[tuple[str, str]] = load_done_candidates()
    log.info("Resume      : %d candidates already in output", len(done))

    loop = asyncio.get_running_loop()
    try:
        import signal

        loop.add_signal_handler(signal.SIGINT, lambda: _shutdown.set())
        loop.add_signal_handler(signal.SIGTERM, lambda: _shutdown.set())
    except NotImplementedError:
        pass

    async with async_playwright() as pw:
        context: BrowserContext = await pw.chromium.launch_persistent_context(
            USER_DATA_DIR,
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-web-security",
            ],
            viewport={
                "width": random.randint(1280, 1440),
                "height": random.randint(780, 920),
            },
            locale="en-US",
            timezone_id="Asia/Kathmandu",
        )

        constituencies: list[tuple[str, pd.DataFrame]] = [
            (str(fname), grp.reset_index(drop=True))
            for fname, grp in selected_df.groupby("FileSaveName", sort=True)
        ]
        total_consts = len(constituencies)

        for c_idx, (file_name, group) in enumerate(constituencies, 1):
            if _shutdown.is_set():
                break

            rows_list = list(group.itertuples(index=False))
            n_cands = len(rows_list)
            batches = [
                rows_list[i : i + CONCURRENCY] for i in range(0, n_cands, CONCURRENCY)
            ]
            label = file_name.replace(".csv", "")

            log.info("-" * 58)
            log.info(
                "CONST [%d/%d] %s  (%d candidates, %d batches)",
                c_idx,
                total_consts,
                label,
                n_cands,
                len(batches),
            )

            for b_idx, batch in enumerate(batches, 1):
                if _shutdown.is_set():
                    break

                log.info("  Batch %d/%d", b_idx, len(batches))
                tasks = [scrape_candidate(r, context, done) for r in batch]
                await asyncio.gather(*tasks, return_exceptions=True)

                if b_idx < len(batches) and not _shutdown.is_set():
                    wait = random.uniform(8, 16)
                    log.info("  Inter-batch cooldown %.0fs", wait)
                    await asyncio.sleep(wait)

            if not _shutdown.is_set():
                cooldown = random.uniform(12, 22)
                log.info("  Inter-constituency cooldown %.0fs", cooldown)
                await asyncio.sleep(cooldown)

        await context.close()

    log.info("=" * 58)
    log.info("Run complete. Tweet counts per file:")
    total_tweets = 0
    for csv_path in sorted(OUTPUT_DIR.glob("*.csv")):
        try:
            n = len(pd.read_csv(csv_path, encoding="utf-8-sig"))
            total_tweets += n
            log.info("  %-38s  %4d tweets", csv_path.name, n)
        except Exception:
            log.warning("  %-38s  (unreadable)", csv_path.name)
    log.info("Total tweets collected: %d", total_tweets)
    log.info("=" * 58)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted - progress saved.")
