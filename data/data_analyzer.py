#################################################
#                  HOW TO USE?                  #
#       Move this file to the constituency      #
# whose result you want to generate and run it. #
#################################################

import pandas as pd
import os
import glob
from textblob import TextBlob
import matplotlib.pyplot as plt

def combiner():
    """
    Finds all CSV files in the current directory (except combined.csv),
    merges them, and creates a new combined.csv file.
    """
    # Find all CSV files in the current working directory
    all_files = glob.glob("*.csv")
    # Filter out 'combined.csv' if it already exists
    input_files = [f for f in all_files if f != "combined.csv"]

    if not input_files:
        print("No source CSV files found to combine.")
        return False

    print(f"Combining {len(input_files)} files: {input_files}")

    # Read and concatenate all dataframes
    df_list = []
    for filename in input_files:
        try:
            temp_df = pd.read_csv(filename)
            df_list.append(temp_df)
        except Exception as e:
            print(f"Error reading {filename}: {e}")

    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_df.to_csv("results/combined.csv", index=False)
        print("Successfully created combined.csv")
        return True
    return False

# 1. Setup Environment
if not os.path.exists("results"):
    os.mkdir("results")

# 2. Run the combiner function first
combiner_success = combiner()

# 3. Load the combined dataset
file_path = "results/combined.csv"
if os.path.exists(file_path):
    df = pd.read_csv(file_path)

    # Check if the dataframe is empty after combining
    if df.empty:
        print("Error: combined.csv is empty.")
    else:
        # Get the constituency details for naming the outputs
        # (Assuming all rows in the combined file belong to the same constituency)
        district = df["DistrictEnglish"].iloc[0]
        const_num = df["Constituency"].iloc[0]
        const_label = f"{district}_{const_num}"

        # 4. Calculate sentiment polarity on the 'Content' column
        df["Sentiment"] = df["Content"].apply(
            lambda x: TextBlob(str(x)).sentiment.polarity
        )

        # 5. Group data by Candidate
        cand_stats = (
            df.groupby("CandidateEnglish")
            .agg(Mentions=("Content", "count"), Avg_Sentiment=("Sentiment", "mean"))
            .reset_index()
        )

        # 6. Calculation Logic:
        # A. Normalize sentiment score from [-1, 1] range to [0, 1]
        cand_stats["Sentiment_Norm"] = (cand_stats["Avg_Sentiment"] + 1) / 2

        # B. Calculate Share of Voice (Candidate mentions / Total mentions)
        cand_stats["Share_of_Voice"] = cand_stats["Mentions"] / cand_stats["Mentions"].sum()

        # C. Calculate Raw Score (70% Sentiment + 30% Share of Voice)
        cand_stats["Raw_Score"] = (cand_stats["Sentiment_Norm"] * 0.70) + (
            cand_stats["Share_of_Voice"] * 0.30
        )

        # D. Final Probability (Normalize so the sum for the constituency is exactly 1.0)
        cand_stats["Probability"] = cand_stats["Raw_Score"] / cand_stats["Raw_Score"].sum()

        # 7. Save the probabilities to a single CSV file
        output_csv = f"results/{const_label}_probabilities.csv"
        cand_stats[["CandidateEnglish", "Probability", "Mentions", "Avg_Sentiment"]].to_csv(
            output_csv, index=False
        )

        # 8. Generate and save the single Pie Chart
        plt.figure(figsize=(10, 8))
        plt.pie(
            cand_stats["Probability"],
            labels=cand_stats["CandidateEnglish"],
            autopct=lambda p: "{:.3f}".format(p * 0.01),  # Display as a probability decimal
            startangle=140,
            colors=plt.cm.Paired.colors,
        )
        plt.title(f"Win Probability - {district.title()} {const_num}")

        chart_path = f"results/{const_label}_pie_chart.png"
        plt.savefig(chart_path)
        plt.close()

        print(f"Analysis Complete for {const_label}")
        print(f"Results saved to {output_csv} and {chart_path}")
else:
    print("Error: combined.csv was not found or could not be created.")