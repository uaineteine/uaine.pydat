@echo off
setlocal

REM Read the current version from meta/version.txt
for /f "delims=" %%i in (meta/version.txt) do set current_version=%%i

REM Prompt the user for a new version
set /p new_version=Enter the new version number (current is %current_version%): 

REM Write the new version back to meta/version.txt
echo %new_version% > meta/version.txt

REM Call the Python script to update the version in setup.py
python automation\devscripts\update_version.py %current_version% %new_version% setup.py

REM Call the Python script to update the version in README.md
python automation\devscripts\update_version.py %current_version% %new_version% README.md

echo Version updated from %current_version% to %new_version% in setup.py and README.md

endlocal

rmdir /S /Q build
rmdir /S /Q dist

python setup.py sdist bdist_wheel