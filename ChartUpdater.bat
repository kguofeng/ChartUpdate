tasklist /Fi "WINDOWTITLE eq Administrator:  ChartUpdate* " /V
taskkill /FI "WINDOWTITLE eq Administrator:  ChartUpdate* "
TITLE ChartUpdate

@echo off

cd O:\Tian\Portal\Charts\ChartUpdate\\
@REM rem Define here the path to your conda installation
set CONDAPATH=C:\ProgramData\Anaconda3

@REM rem Define here the name of the environment
set ENVNAME=env_generic

if %ENVNAME%==base (set ENVPATH=%CONDAPATH%) else (set ENVPATH=%CONDAPATH%\envs\%ENVNAME%)

call %CONDAPATH%\Scripts\activate.bat %ENVPATH%

@REM Run a python script in that environment
@REM Run scanner for basis
call python O:\Tian\Portal\Charts\ChartUpdate\charts_updater.py
call python O:\Tian\Portal\Charts\ChartUpdate\charts_updater2.py
call python O:\Tian\Portal\Charts\ChartUpdate\charts_updater3.py
call python O:\Tian\Portal\Charts\ChartUpdate\charts_updater4.py
call python O:\Tian\Portal\Charts\ChartUpdate\charts_updater5.py
@Timeout /t 3600
