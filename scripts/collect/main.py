import os
import time
from datetime import datetime
from playwright.sync_api import sync_playwright

PAGE_IDS = [
    "officialroutineofnepalbanda",
]
USER_DATA_DIR = "./sessions"
OUTPUT_DIR = "./harvested"
TARGET_POSTS = 15
MAX_SCROLLS = 50
SCROLL_PAUSE = 3.0


def capture_posts(page, page_id, seen_ids: set) -> list:
    nodes = page.query_selector_all('div[data-focus="feed_story"]')
    saved = []

    for node in nodes:
        try:
            # 1. Target the specific timestamp link in the header
            link_el = node.query_selector(
                'h2 + div a[role="link"], span[id] a[role="link"]'
            )
            if not link_el:
                link_el = node.query_selector(
                    'a[href*="/posts/"], a[href*="/permalink/"]'
                )

            if link_el:
                raw_href = link_el.get_attribute("href") or ""
                post_id = raw_href.split("?")[0]
            else:
                continue

            if post_id in seen_ids:
                continue

            # --- ACCURATE TIMESTAMP EXTRACTION ---
            full_timestamp = "Unknown"

            # Step A: Check aria-label first (Instant & Accurate)
            label = link_el.get_attribute("aria-label")
            if label and len(label) > 12:  # Usually contains "day, month date, year..."
                full_timestamp = label
            else:
                # Step B: Hover Fallback with "Freshness" Check
                link_el.scroll_into_view_if_needed()

                # Move mouse to a neutral spot first to close any old tooltips
                page.mouse.move(0, 0)
                time.sleep(0.2)

                # Perform the hover
                link_el.hover()

                # Wait specifically for the tooltip to appear and be stable
                try:
                    # Target common FB tooltip selectors
                    tooltip_selector = '[role="tooltip"], .uiContextualLayer'
                    page.wait_for_selector(tooltip_selector, timeout=2000)

                    # Capture text
                    tooltip = page.query_selector(tooltip_selector)
                    if tooltip:
                        full_timestamp = tooltip.inner_text().strip()
                except Exception:
                    full_timestamp = "Hover Timeout"

            # Final Cleanup: Move mouse away so the next post starts clean
            page.mouse.move(0, 0)

            seen_ids.add(post_id)
            index = len(seen_ids) - 1

            # --- SAVE DATA ---
            outer_html = node.evaluate("el => el.outerHTML")
            timestamp_marker = f'<span class="custom-timestamp" style="display:none">{full_timestamp}</span>'
            save_content = f"{timestamp_marker}\n{outer_html}"

            PAGE_DIR = os.path.join(OUTPUT_DIR, page_id)
            os.makedirs(PAGE_DIR, exist_ok=True)
            filename = os.path.join(PAGE_DIR, f"[{index}].html")

            with open(filename, "w", encoding="utf-8") as f:
                f.write(save_content)

            saved.append(filename)
            print(f"    ✓ [{index}] {full_timestamp}")

        except Exception as e:
            print(f"    ⚠ Node error: {e}")
            continue

    return saved


def harvest_page(page, page_id):
    print(f"\n{'=' * 40}")
    print(f"Loading {page_id}...")

    page.goto(
        f"https://www.facebook.com/{page_id}",
        wait_until="domcontentloaded",
        timeout=30_000,
    )

    # Wait for at least one post to appear
    try:
        page.wait_for_selector('div[data-focus="feed_story"]', timeout=15_000)
    except Exception:
        print(f"  ✗ No feed stories found for {page_id}")
        return None

    # Let the page settle fully
    time.sleep(SCROLL_PAUSE)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    seen_hashes: set = set()
    all_saved: list = []
    stale_scrolls = 0

    for scroll_num in range(MAX_SCROLLS):
        # Capture whatever is visible RIGHT NOW before scrolling away
        newly_saved = capture_posts(page, page_id, seen_hashes)
        all_saved.extend(newly_saved)

        if newly_saved:
            stale_scrolls = 0
            print(
                f"  → Scroll {scroll_num}: +{len(newly_saved)} new | total={len(all_saved)}"
            )
        else:
            stale_scrolls += 1
            print(
                f"  → Scroll {scroll_num}: +0 new | total={len(all_saved)} (stale {stale_scrolls}/6)"
            )

        if len(all_saved) >= TARGET_POSTS:
            print(f"  ✓ Reached target of {TARGET_POSTS} posts!")
            break

        if stale_scrolls >= 6:
            print("  ✗ Feed exhausted")
            break

        # Scroll down and wait for new content to load
        page.evaluate("window.scrollBy(0, window.innerHeight * 0.5)")
        time.sleep(SCROLL_PAUSE)

        # Wait for DOM to update with new posts
        try:
            page.wait_for_function(
                """() => document.querySelectorAll('div[data-focus="feed_story"]').length > 0""",
                timeout=5_000,
            )
        except Exception:
            pass  # continue anyway

    print(f"  → Final: {len(all_saved)} posts saved for {page_id}")
    return all_saved if all_saved else None


def harvest_all():
    captured_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"Starting bulk harvest at {captured_at}")
    print(f"Pages to scrape: {len(PAGE_IDS)}")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            USER_DATA_DIR,
            headless=False,
            viewport={"width": 1280, "height": 800},
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        results = {"success": [], "failed": []}

        for i, page_id in enumerate(PAGE_IDS, 1):
            print(f"\n[{i}/{len(PAGE_IDS)}] {page_id}")
            try:
                saved = harvest_page(page, page_id)
                if saved:
                    results["success"].append(page_id)
                else:
                    results["failed"].append(page_id)
            except Exception as e:
                print(f"  ✗ Error: {e}")
                results["failed"].append(page_id)

        context.close()

    print(f"\n{'=' * 40}")
    print(
        f"Done! ✓ {len(results['success'])} succeeded, ✗ {len(results['failed'])} failed"
    )
    if results["failed"]:
        print(f"Failed: {results['failed']}")


if __name__ == "__main__":
    harvest_all()
