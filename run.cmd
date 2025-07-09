@echo off
REM ──────────────────────────────────────────────────────────────────
REM 1) Set your project root path in one variable
set "PROJ=%USERPROFILE%\Desktop\arb-bot\pred-arb-bot"

REM ──────────────────────────────────────────────────────────────────
REM 2) Open a NEW window for auto_matcher.py
start "Auto Matcher" cmd /k ^
    "cd /d \"%PROJ%\" && ^
     call \"%PROJ%\.venv\Scripts\activate.bat\" && ^
     python \"%PROJ%\auto_matcher.py\""

REM ──────────────────────────────────────────────────────────────────
REM 3) Back in THIS window: go to project, activate venv, run Streamlit
cd /d "%PROJ%"
call ".venv\Scripts\activate.bat"
streamlit run streamlit_app\main.py
