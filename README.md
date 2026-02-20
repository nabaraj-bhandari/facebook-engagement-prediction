<!--toc:start-->

- [Facebook Engagement Prediction](#facebook-engagement-prediction)
- [Project Overview](#project-overview)
- [What is Engagement](#what-is-engagement)
- [Project Workflow](#project-workflow)
- [Directory Structure](#directory-structure)
- [Data Collection Approach](#data-collection-approach)
- [Features Used](#features-used)
- [Tech Stack](#tech-stack)
- [Ethical Considerations](#ethical-considerations)

<!--toc:end-->

# Facebook Engagement Prediction

## Project Overview

This project is developed as part of the **Foundation of Data Science** subject's practical.
The objective is to **predict the engagement of Facebook page posts** using historical data and data science techniques.

The project focuses on understanding how different post characteristics affect engagement and building a predictive model based on those factors.

---

## What is Engagement

In this project, **engagement** is defined as:

```
Engagement = Number of Reactions + Number of Comments + Number of Shares
```

Engagement is measured after a post is published and used as the target variable for prediction.

---

## Project Workflow

1. Select popular public Facebook pages
2. Collect post-level data manually using controlled browser automation
3. Store collected data as CSV files (one file per page)
4. Clean and preprocess the data
5. Perform exploratory data analysis (EDA)
6. Apply statistical analysis and hypothesis testing
7. Build regression-based prediction models
8. Evaluate model performance

---

## Directory Structure

```
facebook-engagement-prediction/
│
├── data/
│   ├── raw/
│   │   └── pages/       # Collected CSV files (pageID.csv)
│   └── processed/       # Cleaned and processed data
│
├── scripts/
│   ├── collect/         # TypeScript data collection scripts
│   └── analysis/        # Python notebooks/scripts for EDA & modeling
│
├── README.md
└── package.json
```

---

## Data Collection Approach

- Data is collected **only from publicly accessible Facebook pages**
- Pages are processed **one at a time** to avoid automated mass scraping
- A real browser session is used where the user logs in manually
- The latest _N_ posts (e.g., 50 or 100) are collected per page
- Each page’s data is saved as a separate CSV file

This semi-manual approach ensures ethical data usage and avoids platform restrictions.

---

## Features Used

### Page-level Features

- Page name
- Page follower count

### Post-level Features

- Post time and day
- Post type (text / image / video)
- Caption length (number of characters)
- Number of hashtags
- Presence of link
- Presence of image or video

### Target Variables

- Number of reactions
- Number of comments
- Number of shares
- Total engagement (computed)

---

## Tech Stack

### Data Collection

- TypeScript
- Playwright
- CSV Writer

### Data Analysis & Modeling

- Python
- Pandas
- NumPy
- Matplotlib
- Scikit-learn

---

## How to Get Started

1. Clone the repository
2. Install Node.js dependencies
3. Configure the list of Facebook pages to collect data from
4. Run the data collection script and log in manually
5. Verify generated CSV files in the `data/raw/pages` directory
6. Proceed to data analysis and model building using Python

---

## Ethical Considerations

- Only publicly visible data is collected
- No private profiles or restricted content are accessed
- No personal user information is stored
- Data is used strictly for academic purposes
- The project does not attempt to bypass platform security mechanisms
