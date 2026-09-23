import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT / "dashboard", ROOT / "producer", ROOT / "spark_processor"):
    sys.path.insert(0, str(path))
