import pandas as pd
import os
import glob
import numpy as np  # Added for exponential calculations
from textblob import TextBlob
import matplotlib.pyplot as plt

# Adjustable parameter: Lower value (e.g., 0.1) makes the winner more distinct
# Higher value (e.g., 1.0) keeps it closer to the original sentiment
SOFTMAX_TEMPERATURE = 0.1

def combiner(input_dir, results_dir):
    """
    Finds all CSV files in a specific constituency folder,
    merges them, and saves as combined.csv in the results subfolder.
    """
    search_path = os.path.join(input_dir, "*.csv")
    all_files = glob.glob(search_path)

    # Exclude combined.csv if it somehow exists in the source
    input_files = [f for f in all_files if "combined.csv" not in f]

    if not input_files:
        return False

    df_list = []
    for filename in input_files:
        try:
            temp_df = pd.read_csv(filename)
            df_list.append(temp_df)
        except Exception as e:
            print(f"  [!] Error reading {filename}: {e}")

    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_path = os.path.join(results_dir, "combined.csv")
        combined_df.to_csv(combined_path, index=False)
        return True
    return False

def analyze_constituency(const_folder):
    """
    Performs sentiment analysis and probability calculation for a single folder.
    """
    input_dir = const_folder
    results_dir = os.path.join("results", const_folder)

    # Create the specific results folder
    os.makedirs(results_dir, exist_ok=True)

    if combiner(input_dir, results_dir):
        file_path = os.path.join(results_dir, "combined.csv")
        df = pd.read_csv(file_path)

        if df.empty:
            print(f"  [!] Skipping {const_folder}: Combined file is empty.")
            return

        # 1. Metadata & Sentiment
        district = df["DistrictEnglish"].iloc[0]
        const_num = df["Constituency"].iloc[0]
        const_label = f"{district}_{const_num}"

        df["Sentiment"] = df["Content"].apply(lambda x: TextBlob(str(x)).sentiment.polarity)

        # 2. Grouping & Statistics
        cand_stats = (
            df.groupby("CandidateEnglish")
            .agg(Mentions=("Content", "count"), Avg_Sentiment=("Sentiment", "mean"))
            .reset_index()
        )

        # Normalize sentiment to [0, 1] range
        cand_stats["Sentiment_Norm"] = (cand_stats["Avg_Sentiment"] + 1) / 2
        cand_stats["Share_of_Voice"] = cand_stats["Mentions"] / cand_stats["Mentions"].sum()

        # Calculate Raw Score (70% Sentiment + 30% Voice)
        cand_stats["Raw_Score"] = (cand_stats["Sentiment_Norm"] * 0.70) + (cand_stats["Share_of_Voice"] * 0.30)

        # --- 3. SOFTMAX NORMALIZATION ---
        # Instead of linear normalization, we use the exponential to amplify differences
        # We divide by temperature to control the 'sharpness'
        exp_scores = np.exp(cand_stats["Raw_Score"] / SOFTMAX_TEMPERATURE)
        cand_stats["Probability"] = exp_scores / exp_scores.sum()

        # 4. Save Probability CSV
        output_csv = os.path.join(results_dir, f"{const_label}_probabilities.csv")
        cand_stats[["CandidateEnglish", "Probability", "Mentions", "Avg_Sentiment", "Raw_Score"]].to_csv(output_csv, index=False)

        # 5. Save Pie Chart
        plt.figure(figsize=(10, 8))
        # Use Probability for the chart
        plt.pie(
            cand_stats["Probability"],
            labels=cand_stats["CandidateEnglish"],
            autopct=lambda p: "{:.3f}".format(p * 0.01),
            startangle=140,
            colors=plt.cm.Paired.colors,
        )
        plt.title(f"Win Probability (Softmax T={SOFTMAX_TEMPERATURE}) - {district.title()} {const_num}")

        chart_path = os.path.join(results_dir, f"{const_label}_pie_chart.png")
        plt.savefig(chart_path)
        plt.close()

        print(f"  [✓] Success: Softmax results for results/{const_folder}/")
    else:
        print(f"  [!] Skipping {const_folder}: No CSV files found.")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("Starting automated Softmax analysis...")

    # Identify all folders in the current directory (excluding 'results' and script itself)
    all_items = os.listdir('.')
    constituency_folders = [
        item for item in all_items
        if os.path.isdir(item) and item != "results" and "_" in item and not item.startswith('.')
    ]

    if not constituency_folders:
        print("No constituency folders found. Ensure folders are named like 'district_number'.")
    else:
        print(f"Found {len(constituency_folders)} constituencies: {constituency_folders}")
        for folder in constituency_folders:
            print(f"\nProcessing: {folder}...")
            analyze_constituency(folder)

    print("\nAll tasks completed.")