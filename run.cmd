@echo off
cd /d "%USERPROFILE%\Desktop\arb-bot\pred-arb-bot"
call .venv\Scripts\activate
streamlit run streamlit_app\main.py
