from __future__ import annotations


class OMXError(RuntimeError):
    """Base error for OMX runtime failures."""


class ConfigError(OMXError):
    """Raised when application configuration is invalid."""


class CacheError(OMXError):
    """Raised when cache persistence or retrieval fails."""


class ChannelFeedError(OMXError):
    """Raised when channel feed retrieval fails."""
