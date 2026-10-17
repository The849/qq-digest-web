@echo off
chcp 65001 >nul
title QQ 群聊日报助手
set "NODE=node"
where node >nul 2>nul
if errorlevel 1 set "NODE=C:\Users\Lenovo\.workbuddy\binaries\node\versions\22.22.2-6\node.exe"
"%NODE%" "%~dp0server.js"
pause
