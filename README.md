# Task 23: Distributed File System (HDFS-lite)

A simplified implementation of HDFS (Hadoop Distributed File System) demonstrating distributed storage, chunk replication, failure handling, and metadata management.

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         NameNode                                │
│                    (Metadata Server)                            │
│                                                                 │
│  • File namespace management                                   │
│  • Chunk location tracking                                     │
│  • DataNode health monitoring (heartbeats)                     │
│  • Replication coordination                                    │
│  • Client request routing                                      │
│                                                                 │
│  Storage:                                                       │
│  - files: {filename -> FileMetadata}                          │
│  - datanodes: {node_id -> DataNodeInfo}                       │
│                                                                 │
└───────────────────┬─────────────────────────────────────────────┘
                    │ Metadata operations
                    │ (file create, locate chunks)
                    │
        ┌───────────┴───────────┬─────────────┬──────────────┐
        │                       │             │              │
        ▼                       ▼             ▼              ▼
┌───────────────┐      ┌───────────────┐   ┌───────────────┐
│   DataNode 1  │      │   DataNode 2  │   │   DataNode 3  │
│               │      │               │   │               │
│ • Stores      │      │ • Stores      │   │ • Stores      │
│   chunks      │      │   chunks      │   │   chunks      │
│ • Sends       │      │ • Sends       │   │ • Sends       │
│   heartbeats  │      │   heartbeats  │   │   heartbeats  │
│ • Replicates  │      │ • Replicates  │   │ • Replicates  │
│   on demand   │      │   on demand   │   │   on demand   │
│               │      │               │   │               │
│ Storage:      │      │ Storage:      │   │ Storage:      │
│ chunk_001     │      │ chunk_001     │   │ chunk_002     │
│ chunk_002     │      │ chunk_003     │   │ chunk_003     │
└───────────────┘      └───────────────┘   └───────────────┘

        ▲                       ▲             ▲
        │                       │             │
        │     Data operations (read/write chunks)
        │                       │             │
        └───────────────────────┴─────────────┘
                                │
                        ┌───────┴────────┐
                        │     Client     │
                        │                │
                        │ • Upload file  │
                        │ • Download     │
                        │ • List files   │
                        │ • Delete file  │
                        └────────────────┘
```

## 📊 Data Flow

### Upload Flow

```
1. Client → NameNode: "I want to upload file.txt (10 MB)"
   
2. NameNode calculates:
   - Chunks needed: 10 MB / 1 MB = 10 chunks
   - Assigns 3 DataNodes per chunk (replication=3)
   
3. NameNode → Client: "Store chunks as follows:"
   Chunk 0: [DN1, DN2, DN3]
   Chunk 1: [DN2, DN3, DN4]
   ...

4. Client splits file into chunks:
   file.txt → [chunk_0, chunk_1, ..., chunk_9]

5. For each chunk:
   Client → DN1: Store chunk_0 (data)
   Client → DN2: Store chunk_0 (data)
   Client → DN3: Store chunk_0 (data)

6. Client → NameNode: "Upload complete"
   NameNode updates metadata
```

### Download Flow

```
1. Client → NameNode: "I want to download file.txt"

2. NameNode → Client: "Chunk locations:"
   Chunk 0: [DN1, DN2, DN3]
   Chunk 1: [DN2, DN3, DN4]
   ...

3. For each chunk:
   Client → DN1: Retrieve chunk_0
   (if DN1 fails, try DN2, then DN3)

4. Client reconstructs file:
   [chunk_0, chunk_1, ..., chunk_9] → file.txt
```

### Heartbeat & Failure Handling

```
Every 10 seconds:
  DataNode → NameNode: Heartbeat
    - Available space
    - Total space
    - Chunk list

NameNode monitors:
  - If no heartbeat for 30s → Mark DataNode as dead
  - Update file metadata (remove dead node from chunk locations)
  - Schedule re-replication if chunks under-replicated
```

## 🔧 Components

### NameNode (namenode.py)

**Responsibilities:**
- Maintain file namespace (filename → metadata)
- Track chunk locations (chunk_id → [datanode_ids])
- Monitor DataNode health via heartbeats
- Coordinate chunk placement and replication
- Handle client metadata requests

**Key Classes:**
- `FileMetadata`: Stores file info and chunk locations
- `DataNodeInfo`: Tracks DataNode health and capacity
- `NameNode`: Main server handling metadata operations

**Endpoints:**
- `register_datanode`: DataNode registration
- `heartbeat`: DataNode health reporting
- `upload_init`: Get chunk placement strategy
- `upload_complete`: Finalize upload metadata
- `download_init`: Get chunk locations for download
- `list_files`: List all files
- `delete_file`: Remove file
- `cluster_status`: Get cluster health

### DataNode (datanode.py)

**Responsibilities:**
- Store file chunks on local disk
- Serve chunk read/write requests
- Send periodic heartbeats to NameNode
- Report available storage capacity
- Handle chunk replication (future)

**Key Features:**
- Local storage directory per DataNode
- Chunk storage with checksums (MD5)
- Heartbeat loop (10s interval)
- Auto-registration with NameNode

**Endpoints:**
- `store_chunk`: Write chunk to disk
- `retrieve_chunk`: Read chunk from disk
- `delete_chunk`: Remove chunk
- `replicate_chunk`: Copy chunk to another node (TODO)

### Client (client.py)

**Responsibilities:**
- Split files into chunks during upload
- Upload chunks to assigned DataNodes
- Download chunks and reconstruct files
- Provide CLI interface for file operations

**Commands:**
- `upload <file>`: Upload file to DFS
- `download <file>`: Download file from DFS
- `list`: List all files
- `delete <file>`: Delete file
- `info <file>`: Show file details
- `status`: Show cluster status

## 📦 Installation

No external dependencies required - uses Python standard library only.

```bash
cd task23
```

## 🚀 Usage

### Step 1: Start NameNode

```bash
python namenode.py
```

**Output:**
```
======================================================================
                     NameNode Server Started                          
======================================================================
Host: localhost
Port: 8000
Chunk Size: 1048576 bytes (1024 KB)
Replication Factor: 3
======================================================================
```

### Step 2: Start DataNodes (3+ nodes recommended)

**Terminal 2:**
```bash
python datanode.py --id node1 --port 8001
```

**Terminal 3:**
```bash
python datanode.py --id node2 --port 8002
```

**Terminal 4:**
```bash
python datanode.py --id node3 --port 8003
```

**Output (each DataNode):**
```
======================================================================
                     DataNode Server Started                          
======================================================================
Node ID: node1
Host: localhost
Port: 8001
Storage: datanode_node1_storage
NameNode: localhost:8000
======================================================================
[INFO] Registered with NameNode at localhost:8000
```

### Step 3: Use Client

#### Upload File
```bash
python client.py upload test.txt
```

**Output:**
```
[UPLOAD] File: test.txt -> test.txt
[UPLOAD] Size: 2048576 bytes (2000.56 KB)
[UPLOAD] Chunks: 2 x 1048576 bytes
[UPLOAD] Chunk 0/1 (1048576 bytes)... ✓ (stored on 3 nodes)
[UPLOAD] Chunk 1/1 (1000000 bytes)... ✓ (stored on 3 nodes)
[SUCCESS] File uploaded: test.txt
```

#### List Files
```bash
python client.py list
```

**Output:**
```
================================================================================
                     FILES IN DISTRIBUTED FILE SYSTEM                          
================================================================================
Filename                                 Size            Chunks     Created             
--------------------------------------------------------------------------------
test.txt                                 2048576 bytes   2          2025-12-13 10:30:45
document.pdf                             5242880 bytes   5          2025-12-13 10:35:12
--------------------------------------------------------------------------------
Total files: 2
================================================================================
```

#### Download File
```bash
python client.py download test.txt --local downloaded_test.txt
```

**Output:**
```
[DOWNLOAD] File: test.txt -> downloaded_test.txt
[DOWNLOAD] Size: 2048576 bytes (2000.56 KB)
[DOWNLOAD] Chunks: 2
[DOWNLOAD] Chunk 0/1... ✓ (1048576 bytes)
[DOWNLOAD] Chunk 1/1... ✓ (1000000 bytes)
[DOWNLOAD] Reconstructing file...
[SUCCESS] File downloaded: downloaded_test.txt
```

#### File Info
```bash
python client.py info test.txt
```

**Output:**
```
================================================================================
                            FILE INFORMATION                                    
================================================================================
Filename: test.txt
Size: 2048576 bytes (2000.56 KB)
Chunk Size: 1048576 bytes
Replication Factor: 3
Chunks: 2

Chunk Locations:
--------------------------------------------------------------------------------
  Chunk 0: node1, node2, node3
  Chunk 1: node1, node2, node3
================================================================================
```

#### Cluster Status
```bash
python client.py status
```

**Output:**
```
==========================================================================================
                                  CLUSTER STATUS                                          
==========================================================================================
Total Files: 2
Total Size: 7291456 bytes (6.95 MB)

DataNodes:
------------------------------------------------------------------------------------------
Node ID         Host:Port                Status     Chunks     Space                      
------------------------------------------------------------------------------------------
node1           localhost:8001           Alive      4          15234 MB / 50000 MB       
node2           localhost:8002           Alive      4          15234 MB / 50000 MB       
node3           localhost:8003           Alive      4          15234 MB / 50000 MB       
==========================================================================================
```

#### Delete File
```bash
python client.py delete test.txt
```

## 🧪 Testing Scenarios

### Test 1: Basic Upload/Download
```bash
# Create test file
echo "Hello, HDFS!" > hello.txt

# Upload
python client.py upload hello.txt

# Download
python client.py download hello.txt --local hello_downloaded.txt

# Verify
diff hello.txt hello_downloaded.txt  # Should be identical
```

### Test 2: Large File (Multi-Chunk)
```bash
# Create 10 MB file
python -c "print('A' * (10 * 1024 * 1024))" > large.txt

# Upload (will split into 10 chunks)
python client.py upload large.txt

# Check file info
python client.py info large.txt

# Download
python client.py download large.txt --local large_downloaded.txt

# Verify size
ls -lh large.txt large_downloaded.txt
```

### Test 3: DataNode Failure Handling

**Setup:** Start 3 DataNodes

```bash
# Upload file with replication=3
python client.py upload test.txt

# Kill one DataNode (Ctrl+C on node2)
# NameNode will detect failure after 30s

# Download still works (uses node1 or node3)
python client.py download test.txt --local test_recovered.txt

# File downloads successfully from remaining nodes
```

### Test 4: Multiple Files
```bash
# Upload multiple files
python client.py upload file1.txt
python client.py upload file2.txt
python client.py upload file3.txt

# List all
python client.py list

# Check cluster status
python client.py status
```

### Test 5: Replication Verification
```bash
# Upload file
python client.py upload test.txt

# Check chunk locations
python client.py info test.txt

# Verify each chunk stored on 3 nodes
# Check DataNode storage directories:
ls datanode_node1_storage/
ls datanode_node2_storage/
ls datanode_node3_storage/
```

## 🔍 Key Features Demonstrated

### 1. File Chunking

Files split into fixed-size chunks (default 1 MB):

```python
chunk_size = 1024 * 1024  # 1 MB
num_chunks = (filesize + chunk_size - 1) // chunk_size

for chunk_id in range(num_chunks):
    chunk_data = file.read(chunk_size)
    store_chunk(chunk_data)
```

**Benefits:**
- Parallel processing
- Load distribution
- Fault tolerance (chunk-level)

### 2. Chunk Replication

Each chunk stored on multiple DataNodes (default replication=3):

```python
for chunk_id in range(num_chunks):
    datanodes = select_datanodes_for_chunk(replication_factor=3)
    for datanode in datanodes:
        store_chunk_to_datanode(datanode, chunk_data)
```

**Benefits:**
- Data redundancy
- High availability
- Read parallelism

### 3. Heartbeat Monitoring

DataNodes send periodic heartbeats (10s interval):

```python
# DataNode side
while running:
    send_heartbeat_to_namenode(
        available_space=get_free_space(),
        total_space=get_total_space(),
        chunks=list_local_chunks()
    )
    time.sleep(10)

# NameNode side
def is_healthy(datanode):
    return (current_time - last_heartbeat) < 30  # 30s timeout
```

**Purpose:**
- Detect node failures
- Monitor capacity
- Track chunk distribution

### 4. Failure Handling

When DataNode fails:

```python
# NameNode detects failure
if not datanode.is_healthy():
    mark_as_dead(datanode)
    
    # Remove from chunk locations
    for file in all_files:
        for chunk in file.chunks:
            chunk.locations.remove(dead_datanode)

# Client downloads from remaining replicas
def download_chunk(chunk_id, datanode_list):
    for datanode in datanode_list:
        try:
            return retrieve_from(datanode)
        except:
            continue  # Try next replica
```

**Resilience:**
- Automatic failover
- No data loss (if replicas exist)
- Transparent to client

### 5. File Reconstruction

Downloaded chunks reassembled in order:

```python
chunks_data = {}
for chunk_id in range(num_chunks):
    chunks_data[chunk_id] = download_chunk(chunk_id)

# Reconstruct
with open(output_file, 'wb') as f:
    for chunk_id in sorted(chunks_data.keys()):
        f.write(chunks_data[chunk_id])
```

## 📈 Performance Characteristics

### Chunk Size Trade-offs

| Chunk Size | Upload Speed | Download Speed | Metadata Overhead |
|------------|--------------|----------------|-------------------|
| 64 KB | Slow | Fast | High |
| 1 MB | ⚖️ Balanced | ⚖️ Balanced | ⚖️ Moderate |
| 64 MB | Fast | Slow | Low |

**Default: 1 MB** - Good balance for small-medium files

### Replication Factor

| Replication | Storage Efficiency | Availability | Read Performance |
|-------------|-------------------|--------------|------------------|
| 1 | 100% | Low | Single node |
| 2 | 50% | Medium | 2x parallelism |
| 3 | 33% | High | 3x parallelism |
| 5 | 20% | Very High | 5x parallelism |

**Default: 3** - Industry standard (HDFS uses 3)

### Scalability

- **NameNode**: Single point of bottleneck (metadata operations)
- **DataNodes**: Horizontally scalable (add more nodes)
- **Client**: Can parallelize chunk uploads/downloads

**Tested Configuration:**
- 3 DataNodes
- 10 MB files
- ~10 files/minute upload rate

## 🏗️ Architecture Patterns

### 1. Master-Slave Pattern
- NameNode = Master (metadata)
- DataNodes = Slaves (data storage)

### 2. Metadata Separation
- Metadata (NameNode) separate from data (DataNodes)
- Enables independent scaling

### 3. Write-Once, Read-Many
- Files immutable after upload
- Simplifies consistency

### 4. Rack Awareness (Future)
```python
# Place replicas on different racks
replicas = [
    select_datanode(rack=1),
    select_datanode(rack=2),
    select_datanode(rack=3)
]
```

## 🚧 Limitations & Future Enhancements

### Current Limitations

1. **Single NameNode** - Single point of failure
2. **No automatic re-replication** - Manual intervention needed
3. **No rack awareness** - Random DataNode selection
4. **No append operations** - Files immutable
5. **No block checksums** - No corruption detection
6. **Synchronous operations** - No async upload/download

### Future Enhancements

#### 1. NameNode High Availability
```python
# Secondary NameNode for failover
SecondaryNameNode:
    - Standby mode
    - Sync metadata from primary
    - Automatic promotion on failure
```

#### 2. Automatic Re-Replication
```python
def replication_manager():
    for file in files:
        under_replicated = file.get_under_replicated_chunks()
        for chunk in under_replicated:
            # Find source DataNode with chunk
            source = find_datanode_with_chunk(chunk)
            # Find target DataNode
            target = select_datanode_for_replication()
            # Trigger replication
            replicate(source, target, chunk)
```

#### 3. Load Balancing
```python
def select_datanode_for_chunk():
    # Consider:
    # - Available space
    # - CPU load
    # - Network bandwidth
    # - Current chunk distribution
    return balanced_datanode
```

#### 4. Data Locality
```python
# Place computation near data (MapReduce pattern)
def run_map_task(file):
    for chunk in file.chunks:
        datanode = chunk.get_preferred_location()
        run_task_on(datanode, chunk)
```

#### 5. Compression
```python
# Compress chunks before storage
chunk_data = compress(original_data)
store_chunk(chunk_data)
```

## 🔒 Security Considerations (Production)

This implementation is for **educational purposes**. Production systems need:

1. **Authentication**
   - Client authentication (Kerberos)
   - DataNode authentication

2. **Authorization**
   - File permissions (read/write/execute)
   - User/group management

3. **Encryption**
   - Data in transit (TLS)
   - Data at rest (disk encryption)

4. **Audit Logging**
   - Track all file operations
   - Compliance requirements

## 📚 Comparison with Real HDFS

| Feature | HDFS-lite (Task 23) | Real HDFS |
|---------|---------------------|-----------|
| Chunk Size | 1 MB | 128 MB |
| Replication | 3 | 3 |
| NameNode HA | ❌ | ✅ (Zookeeper) |
| Auto Re-replication | ❌ | ✅ |
| Rack Awareness | ❌ | ✅ |
| Block Checksums | ❌ | ✅ (CRC32) |
| Snapshots | ❌ | ✅ |
| Quotas | ❌ | ✅ |
| Federation | ❌ | ✅ |
| Erasure Coding | ❌ | ✅ (HDFS 3.0+) |

## 🎓 Learning Objectives

This implementation demonstrates:

1. **Distributed Storage**
   - Data partitioning (chunking)
   - Replication strategies
   - Metadata management

2. **Fault Tolerance**
   - Failure detection (heartbeats)
   - Automatic failover
   - Data redundancy

3. **Scalability Patterns**
   - Master-slave architecture
   - Horizontal scaling (DataNodes)
   - Load distribution

4. **Network Programming**
   - TCP socket communication
   - Protocol design (JSON-based)
   - Large file transfer

5. **System Design**
   - Separation of concerns
   - Stateless workers (DataNodes)
   - Centralized coordination (NameNode)

## 🐛 Troubleshooting

### Issue: NameNode Connection Refused
```
[ERROR] Registration error: [Errno 61] Connection refused
```
**Solution**: Start NameNode first before starting DataNodes

### Issue: Insufficient DataNodes
```
[ERROR] Insufficient DataNodes. Need 3, found 1
```
**Solution**: Start at least 3 DataNodes (or reduce replication factor)

### Issue: Chunk Not Found
```
[ERROR] No healthy DataNodes for chunk 2
```
**Solution**: 
- Check DataNode health: `python client.py status`
- Restart dead DataNodes
- Re-upload file if all replicas lost

### Issue: DataNode Not Appearing
```
DataNodes: []
```
**Solution**:
- Verify DataNode registered: Check NameNode output
- Wait 10s for heartbeat
- Check firewall/port availability

## 📊 Monitoring

### NameNode Statistics (every 30s)
```
[STATS] Nodes: 3/3 | Files: 5 | Chunks: 23
```

### DataNode Registration
```
[DATANODE] Registered: node1 (localhost:8001)
[DATANODE] Registered: node2 (localhost:8002)
```

### Chunk Operations
```
[CHUNK] Stored: chunk_test.txt_0 (1048576 bytes)
[CHUNK] Retrieved: chunk_test.txt_0 (1048576 bytes)
[CHUNK] Deleted: chunk_test.txt_0
```

### File Operations
```
[FILE] Uploaded: test.txt (2048576 bytes, 2 chunks)
[FILE] Deleted: test.txt
```

### Node Failures
```
[DATANODE] Dead: node2 (no heartbeat)
[REPLICATION] Removed node2 from test.txt chunk 0
[REPLICATION] Under-replicated chunks in test.txt: [0, 1]
```

## 📄 License

Educational use only. Not for production deployment.

---

**Task 23 Complete** ✅

Demonstrates distributed file system with chunking, replication, failure detection, and metadata management inspired by HDFS architecture.
