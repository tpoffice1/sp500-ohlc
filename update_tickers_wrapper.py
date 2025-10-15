import os, importlib.util, runpy
from pathlib import Path

tickers_file = Path(os.environ["TICKERS_PATH"]).resolve()
print(f"✅ Forced tickers path: {tickers_file}")

spec = importlib.util.spec_from_file_location("update_tickers", "update_tickers.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

if hasattr(mod, "TICKERS_TXT"):
    mod.TICKERS_TXT = tickers_file

if hasattr(mod, "main"): 
    mod.main()
else:
    runpy.run_path("update_tickers.py", run_name="__main__")
