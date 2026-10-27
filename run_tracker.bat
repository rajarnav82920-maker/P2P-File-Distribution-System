@echo off
title P2P Central Tracker
echo ====================================================
echo Starting P2P Central Tracker on port 8000...
echo ====================================================
python tracker_server.py --host 127.0.0.1 --port 8000
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Trying with Python full path...
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" tracker_server.py --host 127.0.0.1 --port 8000
)
pause
