"""lakehouse_runledger ("runledger"): event logging and data validation for Databricks Workflows.

Importing this package has no side effects and never imports pyspark.
See docs/design.md for the full design and roadmap.
"""

from lakehouse_runledger._version import __version__

__all__ = ["__version__"]
