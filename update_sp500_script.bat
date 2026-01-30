@echo off
echo Starting Python scripts...
echo.

python "C:\Users\billg\OneDrive\Documents\Trading-Dashboard\Data Extraction Script SP500.py"
echo Finished Data Extraction Script: SP500

python "C:\Users\billg\OneDrive\Documents\Trading-Dashboard\Data Extraction Script NASDAQ100.py"
echo Finished Data Extraction Script: NASDAQ100

python "C:\Users\billg\OneDrive\Documents\Trading-Dashboard\Data Extraction Script DOW30.py"
echo Finished Data Extraction Script: DOW30

echo.
echo === ALL PYTHON SCRIPTS FINISHED ===
pause