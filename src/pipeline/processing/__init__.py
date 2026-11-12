"""Processing package for stream bar aggregation and anomaly detection."""

from pipeline.processing.bars import Bar, build_bars
from pipeline.processing.detectors import Alert, Detector, RollingZScoreDetector

__all__ = [
    "Alert",
    "Bar",
    "Detector",
    "RollingZScoreDetector",
    "build_bars",
]

