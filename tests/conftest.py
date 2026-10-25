import sys
from pathlib import Path

# Die Skripte importieren sich gegenseitig ohne Paket (from fetch_klines import ...)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
