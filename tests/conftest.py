"""Environment the test session needs before any `src` module is imported.

`src.routes.alerts` builds its auth verifier inside a `Depends(...)` default,
so `AuthVerifierBase` reads `KEEP_INGESTION_API_KEY` at import time and raises
if it is unset. A fixture runs too late for that; this module does not.
"""

import os

os.environ.setdefault("KEEP_INGESTION_API_KEY", "test-ingestion-api-key")
