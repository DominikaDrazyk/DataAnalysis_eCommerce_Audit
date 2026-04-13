# Olist E-Commerce Strategic Audit (2016–2018)

## :large_orange_diamond: About Me

I am a Doctor of Neuroscience with strong experience in data analysis, statistical modelling and research design. I focus on translating complex data into actionable insights for business and policy. I enjoy data wrangling, visualization, and project management.

**Skills & tools:** 
- advanced **R**, advanced **Python** (*pandas*, *NumPy*, *matplotlib*, *seaborn*, *scipy*) - see my [Python portfolio project](https://github.com/DominikaDrazyk/DataAnalysis_Efficiency_and_Diversity), 
- developing my skills in **Power BI** and **Power Apps** - see my [PowerBI portfolio project](https://github.com/DominikaDrazyk/DataAnalysis_Consultant_Dashboard),
- developing my skills in **SQL** (**ETL**, **PostgreSQL**, **pgAdmin4**, **DBeaver**) - read along for more information,
- comfortable managing **AI-augmented workflow**, leveraging *Cursor IDE* and *Claude* while ensuring code integrity through manual review - see my [Python/CSS portfolio project](https://github.com/DominikaDrazyk/DataAnalysis_euPOWERED_Navigator),
- technical documentation in **Jupyter Notebook** (*Markdown* syntax), version control in **Git**.

&emsp; **Contact**: dominika.a.drazyk@gmail.com <br> 
&emsp; **LinkedIn**: [in/dominika-drazyk-otw95](https://www.linkedin.com/in/dominika-drazyk-otw95/)

## :large_orange_diamond: Project Navigation
Select the path that best matches your interest:

**1. Executive & Business Insight** <br>
*For reviewers focused on storytelling, strategy, and end-results.*

- [PDF Presentation](./reports/eCommerce_presentation.pdf): a step-by-step walkthrough of the project’s assumptions, technical execution highlights, and business insights;

- [Figures](./figures/): a repository of all programmatically generated visualizations used to drive the data narrative.

**2. Technical Deep-Dive & Audit** <br>
*For reviewers interested in the full analytical process and data interpretation.*

- [Full HTML Report](./reports/): a comprehensive, rendered version of the analysis, including all code, statistical interpretations, and granular findings;

- [Codes](./codes/): production-ready pre-staging, ETL and analysis scripts:
    - [Pre-Staging code](./codes/ecommerce_converter.py): python script for Unicode normalization & encoding correction;
    - [ETL Pipeline](./codes/ecommerce_ETL.sql): complete PostgreSQL ETL pipeline for Olist e-commerce dataset;
    - [Interactive Audit Notebook](./codes/ecommerce_analysis.ipynb): the original Jupyter environment used for iterative development and data exploration.

:eight_spoked_asterisk: **Dependency Management** <br>
This project uses Poetry to ensure a deterministic environment (locked versions) and 100% reproducibility. For basic users, a standard `requirements.txt` is also maintained.

- **Option 1: Modern Workflow (Recommended)**
Use this if you have Poetry installed. This will automatically create a virtual environment and install the exact versions from `poetry.lock`.

```
# Bash
# Install dependencies and create virtual environment
poetry install
# Activate the environment
poetry shell
```

- **Option 2: Standard Workflow (Pip)**
Use this for a traditional setup using the provided `requirements.txt`.

1. Initialize the Virtual Environment
```
# Linux / macOS
python3 -m venv .venv && source .venv/bin/activate
# Windows 
python -m venv .venv && .venv\Scripts\activate
```

2. Install Dependencies
```
# Bash
pip install --upgrade pip
pip install -r requirements.txt
```

## :large_orange_diamond: Overview

This project audits the logistical performance and market dynamics of the Olist e-commerce landscape (Brazilian marketplace). I inspect the dataset to assess whether high transaction volume and market leadership correlate with operational excellence, and to quantify the financial exposure of regional logistical bottlenecks.

:part_alternation_mark: *Practical business question*: Does rapid marketplace scaling outpace logistical infrastructure, and what is the systemic financial risk of relying on a highly concentrated pool of top-tier sellers and regions?

### What this project delivers:

- A robust, Unicode-normalized Python pipeline ready for raw data imput and normalization.

- A set of highly optimized, well-documented PostgreSQL views ready for BI dashboard integration.

- Clear visualizations of growth trajectories, revenue concentration, and logistical risk hierarchies.

- :part_alternation_mark: Business-relevant insights for stakeholders interested in marketplace strategy, seller development, and supply chain optimization.

### Data & Source Metadata

External data source (Kaggle):
[Olist E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — contains 100,000 anonymized orders from 2016 to 2018 across the Brazilian marketplace. Authored by André Sionek (2018) and available under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).

### Key variables

Dimensions & Segmentation
- `monthly` or `quarterly`: temporal grain of the fiscal period assumed in the given analysis;
- `product_category`: name of the product category, mapped to English;
- `market_segment`: classification of merchants into Leaders (e.g., leader_01) vs. Residual baseline;
- `seller_state`: geographical origin of the dispatch (e.g., SP, RJ, AM);
- `price_tier` and `performance_tier`: grouping of products or sellers into categorical bins (e.g., Low, Mid, High) based on statistical thresholds to evaluate market distribution.

Logistics & Risk Metrics
- `order_volume`: distinct count of unique order identifiers;
- `late_delivery_rate` and its 6-months trailing equivalent `l6m_late_rate`: percentage of orders delivered past the estimated date, serving as the primary reliability metric;
- `delay_risk_rank`: relative ranking of market segments based on their monthly failure rates to identify underperforming clusters.

Financial & Growth Metrics
- `median_price` of an item and `units_sold` within the category: baseline commercial metrics for evaluating portfolio health;
- `global_revenue_share` and `category_revenue_share`: percentage of total platform GMV contributed by a specific segment or its internal tier;
- `t3m_revenue` (Trailing 3-Month average) and `mom_growth_pct` (Month-over-Month percentage change) indicating the health of the segment's underlying momentum, filtering out high-frequency seasonal noise.

### Tools & Methods

**Programming & Analysis**: PostgreSQL (Window functions, CTEs, Aggregations), Python {`pandas`, `matplotlib`, `seaborn`, `SQLAlchemy`};

**Documentation & Reporting**: Markdown, HTML syntax, Jupyter Notebooks.

**Data Engineering**: Custom Python ETL pipelines, Unicode normalization, ASCII transliteration;

**Environment**: pgAdmin & Jupyter Notebook;

**Version control & sharing**: Git & GitHub;

**Analytics performed**: time-series smoothing (MoM, T3M), geographic risk mapping, percentile thresholding, cohort segmentation, baseline benchmarking.

## :large_orange_diamond: Objectives

1. Execute Pre-Staging by standardizing raw CSV assets, handling encoding detection, and transliterating special characters to ASCII to ensure global compatibility.
<br> Code: `ecommerce_converter.py`

2. Migrate sanitized data into a structured relational environment, clean raw tables, and build normalized data models.
<br> Code: `ecommerce_ETL.sql`

3. (**Q1**) Segment the seller base to evaluate market dominance and audit logistical reliability against transaction scale.
<br> Code: `ecommerce_analysis.ipynb` 

4. (**Q2**) Analyze product portfolio efficiency to isolate high-yield categories and map the platform's dependency on specific price tiers.
<br> Code: `ecommerce_analysis.ipynb`

5. (**Q3**) Identify geographical bottlenecks and quantify the absolute "Revenue at Risk" caused by late deliveries in core vs. peripheral hubs.
<br> Code: `ecommerce_analysis.ipynb`

6. (**Q4**) Model growth velocity and market volatility using Month-over-Month (MoM) and Trailing 3-Month (T3M) smoothing to determine long-term market momentum.
<br> Code: `ecommerce_analysis.ipynb`

## :large_orange_diamond: Examples of programming solutions

**Data Pre-Staging (Python)**: sanitize raw CSVs, handle encoding fallbacks, and transliterate Brazilian Portuguese characters.

`Python`
```
# snippet conceptualizing ecommerce_converter.py logic
df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
# Transliterate special characters to ASCII using unidecode
df = df.map(lambda x: unidecode(str(x)) if isinstance(x, str) else x)
df.to_csv(output_path, encoding='utf-8', index=False)
```

**Dynamic Thresholding (SQL)**: establish data-driven price bands using percentiles rather than hardcoded values.

`SQL`
```
WITH price_thresholds AS (
    SELECT
        PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY price) AS price_p33,
        PERCENTILE_CONT(0.66) WITHIN GROUP (ORDER BY price) AS price_p66
    FROM fact_order_items
)
```

**Time-Series Smoothing (SQL)**: filter out seasonal noise using a Trailing 3-Month (T3M) average.

`SQL`
```
AVG(this_month_revenue) OVER (
    PARTITION BY market_segment 
    ORDER BY monthly 
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
) AS T3M_revenue
```

**Zero-Division Protection (SQL)**: ensure query stability when calculating failure rates for months with no recorded transactions.

`SQL`
```
ROUND(late_deliv_count::numeric / NULLIF(order_volume, 0), 3) AS monthly_late_rate
```

### Limitations & Challenges

- The 100,000-order snapshot is a *historical "frozen" dataset* (2016–2018), which may not reflect recent post-pandemic shifts in Brazilian e-commerce behavior or logistics technology;
- Models do not account for *external macroeconomic factors* such as BRL exchange rate volatility, national strikes, or changes in postal service pricing structures during the window;
- The current SQL views treat *cancelled orders* as lost revenue but cannot distinguish between seller-fault cancellations and customer-driven returns, potentially overstating seller-side failure rates;
- The T3M and MoM growth models assume a degree of continuity that *may overlook micro-seasonal events* (e.g., local holidays) not explicitly flagged in the primary timestamp data.

:grey_exclamation: Offered insights are focused on internal marketplace dynamics and are not based on structured academic knowledge of Brazilian macroeconomics. My aim was to demonstrate the ability to architect data, design robust ETL processes, and extract actionable business intelligence from complex relational datasets.

## :large_orange_diamond: Key findings

**General trends**: The platform achieved significant gross revenue peaks during the 15-month analysis window, but this growth was highly volatile and structurally concentrated. The rapid expansion phase was characterized by reactive "growth spurts" tied to seasonality rather than sustained, linear increases in baseline seller performance.

The platform exhibits **three core vulnerabilities that** require strategic intervention:

- **Revenue Polarisation**: Success is heavily reliant on a small cohort of Market Leaders and a few High-Yield product categories. The stagnation of the "residual" segment indicates the platform lacks a growth engine for middle-class sellers.
<br> :part_alternation_mark: *Business Insight*: Protecting the stability of top leaders is paramount, but long-term survival requires incentivizing the residual segment to break their low-revenue ceilings.

- **Logistical Disconnect**: There is a persistent gap between revenue growth and delivery reliability. High volume does not correlate with operational excellence, showing that Leaders are highly susceptible to supply chain friction.
<br> :part_alternation_mark: *Business Insight*: Scaling has outpaced infrastructure. Growth strategies must be paired with mandatory logistical upgrades for high-volume sellers.

- **Regional Asymmetry**: The logistical landscape is deeply fragmented. São Paulo handles massive volume, meaning even rare delays create severe financial exposure ("Revenue at Risk" of ~700k BRL). Conversely, regions like Amazonas have terrible service but minimal financial impact.
<br> :part_alternation_mark: *Business Insight*: Logistics recovery efforts (premium carriers, dedicated sorting) must be aggressively prioritized in São Paulo to protect the highest-density revenue streams first.

## :large_orange_diamond: Presented skills

**Data Modelling & Engineering**
- Designing a *star-schema model* (fact tables + dimension tables) within a PostgreSQL environment.
- Creating *robust one-to-many and one-to-one relationships* across complex relational datasets.
- Resolving data ambiguity and category mapping using *lookup and bridging tables*.
- Building *advanced aggregation measures* (e.g., Trailing 3-Month smoothing, Month-over-Month growth, percentile-based thresholds).

**Data Visualization & Storytelling**
- Translating a strategic business narrative into a *functional, highly readable analytical report*.
- Highlighting *high-impact segments* (e.g., Market Leaders, high-yield categories, and at-risk regions) visually.
- Organizing metrics and visuals into *purposeful analytical chapters* (Q1–Q4) aligned with an executive audit scenario.
- Preparing *comprehensive documentation* to effectively communicate methodology and translate data into *actionable business insights*.
