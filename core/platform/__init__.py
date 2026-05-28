"""Platform abstraction layer — the only place cloud/engine-specific code is allowed.

`factory.get_platform()` reads the LAKEHOUSE_PLATFORM env var and returns the matching
`base.LakehousePlatform` implementation. See ADR-002.
"""
