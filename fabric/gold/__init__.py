"""Fabric-native Gold layer (ADR-022).

Independent Spark DataFrame implementation of the corpus contract. Output
schema matches ``core.gold.encounter_summary`` so downstream consumers
(scribe-iq RAG, clinical-bert-pipeline NLP, Ollama dialogue generation) see
identical columns regardless of which platform built the corpus.
"""
