1.  **Create a Virtual Environment**:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    pip install -r track_record/requirements.txt 
    ```

3.  **Configure Environment**:
    *   Copy `.env.example` to `.env`.
    *   Fill in your private keys, API keys (Blockfrost), and Discord webhook URL.

4.  **Run the Application**:
    *   Use the `run.cmd` script on Windows or run the components separately on Linux/macOS.
    *   Start the background service: `python auto_matcher.py`
    *   Start the trade executor: `python arb_executor.py`
    *   Start the web dashboard: `streamlit run streamlit_app/main.py`