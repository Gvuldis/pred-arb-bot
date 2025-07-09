python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# In one terminal:
python auto_matcher.py

# In another:
streamlit run streamlit_app/main.py