# track_record

This is a professional-grade portfolio management and analysis tool designed to provide a verifiable track record of arbitrage trading performance between Bodega and Polymarket.

## Setup
2.  **Install Dependencies:**
    Open your terminal, navigate into the `portfolio_analyzer` directory, and run the following command to install the necessary libraries:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Get API Key:**
    You will need a **Project ID** from [Blockfrost.io](https://blockfrost.io/). The free tier is sufficient for this application.

## Workflow

The application works in two main stages: Data Ingestion and Interactive Analysis.

### Stage 1: Data Ingestion (Command Line)

This is a one-time step to pull all your historical data into the local database.

1.  **Export Polymarket History:**
    Go to your Polymarket account and export your full transaction history. Save the file as `polymarket_history.csv` inside your `portfolio_analyzer` directory.

2.  **Run the Ingestion Script:**
    In your terminal, from inside the `portfolio_analyzer` directory, run the `ingest.py` script. Replace the placeholder arguments with your actual data.

    ```bash
    python ingest.py YOUR_CARDANO_ADDRESS_HERE polymarket_history.csv --blockfrost_key YOUR_BLOCKFROST_KEY_HERE
    ```

    This will create a `portfolio.db` file in your directory containing all your raw transaction data. You only need to run this script again if you want to add new transactions to the database.

### Stage 2: Portfolio Analysis (Streamlit App)

This is the interactive part where you manage and view your portfolio.

1.  **Run the App:**
    In your terminal, run the following command:
    ```bash
    streamlit run app.py
    ```
    This will open the application in your web browser.

2.  **Build Positions:**
    *   Navigate to the **"Position Builder"** page from the sidebar.
    *   You will see lists of your unassigned transactions from both Bodega and Polymarket.
    *   For a specific market (e.g., "Zohran"), check the `select` box for all related transactions.
    *   Enter a name for the position (e.g., "Zohran 2025").
    *   Click "Create Position". The selected transactions will now be grouped and will disappear from this page.

3.  **Analyze Your Portfolio:**
    *   Navigate to the **"Portfolio Dashboard"** page.
    *   The dashboard will automatically display all the positions you've created, with calculations mirroring your spreadsheet.
    *   The data uses live ADA prices for the "Best Case" scenario, giving you a dynamic view of your portfolio's potential.

This setup gives you a powerful, verifiable, and dynamic tool to manage your trading strategy like a professional.


NEEDED CORRECTIONS:

Whittaker (actual profit = 99USD)
SNEK (actual loss = -75USD)
METS VS GIANTS (actual profit = 224$)