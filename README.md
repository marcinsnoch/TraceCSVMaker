# CSVTraceMaker

Script to automate generating CSV files with list of products for Traceability app.

```bash
cp config.txt config.ini
```

Make exe

```bash
pyinstaller --onefile --add-data=last_id.txt:. main.py
```