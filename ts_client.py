"""TradingStudio backward-compatibility shim.

This module re-exports the CetaResearch client under the old TradingStudio
names so existing code (blog snippets, external users) continues to work:

    from ts_client import TradingStudio
    ts = TradingStudio(api_key="...")

For new code, use cr_client directly:

    from cr_client import CetaResearch
    cr = CetaResearch(api_key="...")
"""

from cr_client import (
    CetaResearch,
    CetaResearchError,
    QueryTimeoutError,
    QueryFailedError,
    ExecutionError,
)

# Backward-compatible aliases
TradingStudio = CetaResearch
TradingStudioError = CetaResearchError
