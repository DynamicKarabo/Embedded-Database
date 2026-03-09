# Product Requirements Document

# Embedded Database Engine

## 1. Overview

The Embedded Database Engine is a lightweight storage system designed to persist structured data efficiently on disk while supporting fast retrieval through indexed lookups.

The system provides a simple key–value interface backed by a custom storage engine. It demonstrates fundamental database architecture concepts including durable storage, indexing, and crash recovery.

The primary goal is to implement core database primitives that power modern storage engines.

---

# 2. Problem Statement

Many applications require reliable data storage that survives process restarts and system failures.

Traditional approaches such as in-memory storage or simple file writes lack:

* efficient querying
* durability guarantees
* indexing mechanisms
* crash recovery

Database systems solve these problems through structured storage engines.

This project aims to implement the foundational components of such a system.

---

# 3. Goals

### Primary Goals

* implement persistent disk storage for structured records
* support efficient key-based data retrieval
* ensure durability using write-ahead logging
* provide a simple query interface

### Secondary Goals

* support basic indexing
* enable fast data lookup through index structures
* maintain predictable performance with large datasets

---

# 4. Non-Goals

The first version will not include:

* full SQL support
* distributed replication
* complex query planning
* advanced transaction isolation levels

The focus is a **single-node storage engine**.

---

# 5. Target Users

Potential users include:

* developers embedding lightweight databases into applications
* engineers studying database internals
* applications requiring simple structured storage

---

# 6. Core Features

## Persistent Storage Engine

Data will be stored on disk using an append-only storage format.

Each record will be serialized and written sequentially.

Example record structure:

```
[length][key][value]
```

This approach ensures efficient writes and simplifies crash recovery.

---

## Write-Ahead Logging

All write operations are first recorded in a log before being applied to the storage engine.

This guarantees durability in the event of system crashes.

The log allows the database to replay operations during recovery.

---

## Indexing System

To support efficient lookups, an index structure maps keys to storage locations.

Possible index structures include:

* hash index
* B-tree index

Indexes allow the system to locate records without scanning the entire dataset.

---

## Key–Value Query Interface

The database will expose a simple API for interacting with stored data.

Example operations:

```
put(key, value)
get(key)
delete(key)
```

These operations form the basis of the database query interface.

---

# 7. System Architecture

### Storage Layer

Responsible for writing and retrieving serialized records from disk.

### Log Manager

Maintains the write-ahead log and handles crash recovery.

### Index Manager

Maintains the in-memory index mapping keys to record offsets.

### Query Interface

Exposes database operations to external applications.

---

# 8. Performance Requirements

The system should support:

* efficient sequential disk writes
* fast indexed lookups
* predictable performance with growing datasets

Indexes must allow record retrieval without scanning the entire dataset.

---

# 9. Success Metrics

The project succeeds if:

* data persists correctly across restarts
* crash recovery restores consistent state
* indexed lookups perform significantly faster than full scans
* the database can store thousands of records without performance degradation

---

# 10. Milestones

Milestone 1 — Storage Engine
Implement append-only storage file.

Milestone 2 — Logging
Add write-ahead logging and crash recovery.

Milestone 3 — Indexing
Implement index structure for fast lookups.