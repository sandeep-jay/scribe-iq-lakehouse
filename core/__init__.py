"""scribe-iq-lakehouse — engine-agnostic lakehouse package.

Subpackages:
    platform   Cloud I/O abstraction (the only place platform-specific code lives).
    transforms Pure, platform-independent transform logic (returns pyarrow tables).
    ingest     Bronze landing + streaming simulation (local tier).
    gold       Denormalized corpus builders.
    validation Schema + quality checks.
"""
