@echo off
title P2P Peer 2 (Port 5002)
echo ====================================================
echo Starting Peer 2 on port 5002...
echo ====================================================
python peer_cli.py --id Peer_5002 --port 5002 --shared-dir peer2_shared --downloads-dir peer2_downloads
if %ERRORLEVEL% NEQ 0 (
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" peer_cli.py --id Peer_5002 --port 5002 --shared-dir peer2_shared --downloads-dir peer2_downloads
)
pause
