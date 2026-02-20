import pandas as pd
from bs4 import BeautifulSoup
import os
import re
from natsort import natsorted

OUTPUT_DIR = "../harvested"


def parse_val(text):
    if not text:
        return 0
    text = text.upper().replace(",", "").strip()
    match = re.search(r"(\d+\.?\d*)([KMB]?)", text)
    if not match:
        return 0
    number = float(match.group(1))
    multiplier = match.group(2)
    if multiplier == "K":
        number *= 1000
    elif multiplier == "M":
        number *= 1000000
    elif multiplier == "B":
        number *= 1000000000
    return int(number)


def parse_html_to_data(file_path, page_id):
    with open(file_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")
    story = soup.select_one('div[data-focus="feed_story"]')
    if not story:
        return None

    # --- Content ---
    content_node = (
        story.find("span", {"data-ad-rendering-role": "description"})
        or story.find("div", {"data-ad-comet-preview": "message"})
        or story.find("div", {"data-ad-preview": "message"})
    )
    content = content_node.get_text(strip=True) if content_node else "N/A"

    # --- Timestamp (New) ---
    # Look for the hidden span we injected in the scraper
    ts_node = soup.find("span", class_="custom-timestamp")
    timestamp = ts_node.get_text(strip=True) if ts_node else "Unknown"

    # --- Reactions ---
    reaction_count = 0
    # Search for the reaction string (e.g., "18K", "2.5K")
    reaction_node = story.find("span", {"class": "xt0b8zv"})
    if reaction_node:
        reaction_count = parse_val(reaction_node.get_text())

    # --- Comments ---
    comment_node = story.find("span", string=lambda x: x and "comment" in x.lower())
    comments_count = parse_val(comment_node.get_text()) if comment_node else 0

    # --- Shares ---
    share_node = story.find("span", string=lambda x: x and "share" in x.lower())
    shares_count = parse_val(share_node.get_text()) if share_node else 0

    return {
        "Page": page_id,
        "Timestamp": timestamp,
        "Content": content,
        "Reactions": reaction_count,
        "Comments": comments_count,
        "Shares": shares_count,
    }


def convert_all_to_csv():
    all_data = []

    # Iterate through folders (each folder is a Page ID)
    if not os.path.exists(OUTPUT_DIR):
        print("No harvested data found.")
        return

    for page_id in os.listdir(OUTPUT_DIR):
        page_path = os.path.join(OUTPUT_DIR, page_id)
        if not os.path.isdir(page_path):
            continue

        print(f"Parsing {page_id}...")
        # Sort files numerically [0], [1], etc.
        files = natsorted([f for f in os.listdir(page_path) if f.endswith(".html")])

        for file in files:
            file_path = os.path.join(page_path, file)
            data = parse_html_to_data(file_path, page_id)
            if data:
                all_data.append(data)

    # Save to CSV
    df = pd.DataFrame(all_data)
    df.to_csv("facebook_harvested_data.csv", index=False, encoding="utf-8-sig")
    print(f"\nSuccess! Saved {len(all_data)} posts to facebook_harvested_data.csv")


if __name__ == "__main__":
    convert_all_to_csv()
