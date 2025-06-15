# COMP2207 Coursework: Distributed File System

This repository contains my solution for **COMP2207: Distributed File System Coursework** at the University of Southampton (2024/25).

---

## Overview

The project implements a distributed storage system in Java, comprising a central **Controller** and multiple **Dstores** (data stores). It supports multiple clients for concurrent file operations (store, load, remove, list), with replication and fault tolerance.

### Main Features

- **Java TCP networking** between Controller, Dstores, and Clients
- File replication across N Dstores (configurable replication factor R)
- Tolerates Dstore failures and allows new Dstores to join at runtime
- Handles concurrent client requests and robust error handling
- Protocol-compliant operation, with detailed logging and timeouts

---

## How It Works

- **Controller**: Manages file metadata, orchestrates requests, and ensures correct replication and distribution.
- **Dstore**: Stores files, communicates with Controller and other Dstores, and handles file operations on disk.
- **Client**: Provided separately; interacts with Controller and Dstores to execute operations.

**Supported Operations:**
- `STORE filename filesize`
- `LOAD filename`
- `REMOVE filename`
- `LIST`

---

## Usage

1. **Compile all Java files:**
   ```bash
   javac *.java
   ```

2. **Run Controller:**
   ```bash
   java Controller <cport> <R> <timeout_ms> <rebalance_period_s>
   ```
   - Example: `java Controller 4000 3 500 20`

3. **Run Dstores (start N, each with unique port and folder):**
   ```bash
   java Dstore <port> <cport> <timeout_ms> <file_folder>
   ```
   - Example: `java Dstore 5001 4000 500 dstore1_folder`

4. **Client:**  
   The official client application used for testing is provided by the University of Southampton and is **not included** here due to copyright.

---


## Key Learning Outcomes

- Java networking and concurrency (TCP, sockets, multi-threading)
- Client-server and distributed objects design
- Robust error handling and fault-tolerant distributed systems
