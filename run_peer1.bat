@echo off
title P2P Peer 1 (Port 5001)
echo ====================================================
echo Starting Peer 1 on port 5001...
echo ====================================================
python peer_cli.py --id Peer_5001 --port 5001 --shared-dir peer1_shared --downloads-dir peer1_downloads
if %ERRORLEVEL% NEQ 0 (
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" peer_cli.py --id Peer_5001 --port 5001 --shared-dir peer1_shared --downloads-dir peer1_downloads
)
pause
