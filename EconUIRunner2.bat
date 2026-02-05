tasklist /Fi "WINDOWTITLE eq Administrator:  EconUI* " /V
taskkill /FI "WINDOWTITLE eq Administrator:  EconUI* "
TITLE EconUI

@echo off
@REM rem Define here the path to your conda installation
set CONDAPATH=C:\ProgramData\Anaconda3

@REM rem Define here the name of the environment
set ENVNAME=env_generic

if %ENVNAME%==base (set ENVPATH=%CONDAPATH%) else (set ENVPATH=%CONDAPATH%\envs\%ENVNAME%)

call %CONDAPATH%\Scripts\activate.bat %ENVPATH%

@REM change directory; Specify /D to change the drive also.
echo Current working directory: %CD%
cd /d "O:\Tian\Portal\Charts\ChartUpdate\"
echo Current working directory: %CD%

@REM Run a python script in that environment
call python O:\Tian\Portal\Charts\ChartUpdate\UI_Economy.py -d False
@Timeout /t 3600
