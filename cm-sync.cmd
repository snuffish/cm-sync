@echo off
rem Windows entry point: put this folder on PATH (cmd resolves %~dp0 to where this file is, not to a link to it).
where py >nul 2>nul
if %errorlevel%==0 (py -3 -I "%~dp0cm_sync.py" %*) else (python -I "%~dp0cm_sync.py" %*)
exit /b %errorlevel%
