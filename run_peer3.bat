@echo off
title P2P Peer 3 (Port 5003 - Downloader)
echo ====================================================
echo Starting Peer 3 on port 5003...
echo ====================================================
python peer_cli.py --id Peer_5003 --port 5003 --shared-dir peer3_shared --downloads-dir peer3_downloads
if %ERRORLEVEL% NEQ 0 (
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" peer_cli.py --id Peer_5003 --port 5003 --shared-dir peer3_shared --downloads-dir peer3_downloads
)
pause
