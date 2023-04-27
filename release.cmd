@echo off
REM # Updated 15:23 31/03/2023
REM # ###

cd %~dp0 | exit 1

setlocal

set PYTHONHOME=C:\Python\3.7

set HOME=%cd%
set PYTHONPATH_ORIG=%PYTHONPATH%
set PYTHONPATH_THIS=%HOME%\src 
for %%I in (.) do set REPOS=%%~nI%%~xI
echo Processing Repos: %REPOS%

rem =============================================

echo Uninstalling %REPOS%

set PYTHONPATH=%PYTHONPATH_ORIG%
"%PYTHONHOME%\python" -m pip uninstall -y %REPOS%

rem =============================================

set PYTHONPATH=%PYTHONPATH_THIS%
"%PYTHONHOME%\python" -c "import %REPOS% as x; print(f'Release Version: {x.__version__}')"
echo Confirm all files are saved and closed.
pause

rem =============================================

echo Removing old distribution files

rmdir /s /q .\dist

cd .\src | exit 2
forfiles /P . /M *.egg-info /C "cmd /c rmdir /s /q @file"
cd ..

rem =============================================

echo Installing required packages

"%PYTHONHOME%\python" -m pip install --upgrade  --no-warn-script-location pip
"%PYTHONHOME%\python" -m pip install --upgrade build
"%PYTHONHOME%\python" -m pip install --upgrade  --no-warn-script-location pylint
"%PYTHONHOME%\python" -m pip install --upgrade python-dateutil
"%PYTHONHOME%\python" -m pip install --upgrade  --no-warn-script-location Sphinx
"%PYTHONHOME%\python" -m pip install --upgrade sphinx-rtd-theme
"%PYTHONHOME%\python" -m pip install --upgrade twine

rem =============================================

echo Running pylint

set PYTHONPATH="%PYTHONPATH_THIS%"
"%PYTHONHOME%\python" -m pylint "%HOME%/src/%REPOS%"
echo Confirm pylint was successful
pause

rem =============================================

echo Running unittests

rem =============================================

echo Building project

rem =============================================

echo Creating sphinx documentation

cd "%HOME%\docs"
"%PYTHONHOME%\python" -m sphinx -a -b html . _build

echo Confirm sphinx documentation build was successful
pause

rem =============================================

rem =============================================

rem =============================================

rem =============================================

echo Completed.
pause

endlocal
