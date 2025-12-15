#!/usr/bin/env python3
"""
Task 23: NameNode (Metadata Server)
Manages file metadata, chunk locations, replication, and DataNode health.
"""

import socket
import threading
import json
import time
import uuid
from datetime import datetime
from typing import Dict, List, Set, Optional
from collections import defaultdict


class FileMetadata:
    """Metadata for a file in the distributed file system."""
    
    def __init__(self, filename: str, size: int, chunk_size: int, replication_factor: int):
        self.filename = filename
        self.size = size
        self.chunk_size = chunk_size
        self.replication_factor = replication_factor
        self.created_at = time.time()
        self.chunks: Dict[int, List[str]] = {}  # chunk_id -> [datanode_ids]
    
    def add_chunk_location(self, chunk_id: int, datanode_id: str):
        """Add a chunk location."""
        if chunk_id not in self.chunks:
            self.chunks[chunk_id] = []
        if datanode_id not in self.chunks[chunk_id]:
            self.chunks[chunk_id].append(datanode_id)
    
    def remove_chunk_location(self, chunk_id: int, datanode_id: str):
        """Remove a chunk location."""
        if chunk_id in self.chunks and datanode_id in self.chunks[chunk_id]:
            self.chunks[chunk_id].remove(datanode_id)
    
    def get_chunk_locations(self, chunk_id: int) -> List[str]:
        """Get DataNode IDs storing this chunk."""
        return self.chunks.get(chunk_id, [])
    
    def is_under_replicated(self) -> List[int]:
        """Get list of under-replicated chunks."""
        under_replicated = []
        for chunk_id, locations in self.chunks.items():
            if len(locations) < self.replication_factor:
                under_replicated.append(chunk_id)
        return under_replicated
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            'filename': self.filename,
            'size': self.size,
            'chunk_size': self.chunk_size,
            'replication_factor': self.replication_factor,
            'created_at': self.created_at,
            'chunks': {str(k): v for k, v in self.chunks.items()}
        }


class DataNodeInfo:
    """Information about a DataNode."""
    
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.last_heartbeat = time.time()
        self.chunks: Set[str] = set()  # chunk_ids stored
        self.available_space = 0
        self.total_space = 0
        self.is_alive = True
    
    def update_heartbeat(self, available_space: int, total_space: int, chunks: List[str]):
        """Update heartbeat information."""
        self.last_heartbeat = time.time()
        self.available_space = available_space
        self.total_space = total_space
        self.chunks = set(chunks)
        self.is_alive = True
    
    def is_healthy(self, timeout: int = 30) -> bool:
        """Check if DataNode is healthy."""
        return (time.time() - self.last_heartbeat) < timeout
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            'node_id': self.node_id,
            'host': self.host,
            'port': self.port,
            'last_heartbeat': self.last_heartbeat,
            'available_space': self.available_space,
            'total_space': self.total_space,
            'is_alive': self.is_alive,
            'chunk_count': len(self.chunks)
        }


class NameNode:
    """NameNode - Metadata server for distributed file system."""
    
    def __init__(self, host: str = 'localhost', port: int = 8000, 
                 chunk_size: int = 1024*1024, replication_factor: int = 3):
        self.host = host
        self.port = port
        self.chunk_size = chunk_size
        self.replication_factor = replication_factor
        
        # Metadata storage
        self.files: Dict[str, FileMetadata] = {}  # filename -> metadata
        self.datanodes: Dict[str, DataNodeInfo] = {}  # node_id -> info
        
        # Locks
        self.files_lock = threading.Lock()
        self.datanodes_lock = threading.Lock()
        
        # Server state
        self.running = False
        self.server_socket = None
    
    def start(self):
        """Start the NameNode server."""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        print("="*70)
        print(f"{'NameNode Server Started':^70}")
        print("="*70)
        print(f"Host: {self.host}")
        print(f"Port: {self.port}")
        print(f"Chunk Size: {self.chunk_size} bytes ({self.chunk_size // 1024} KB)")
        print(f"Replication Factor: {self.replication_factor}")
        print("="*70)
        print()
        
        # Start background threads
        threading.Thread(target=self.heartbeat_monitor, daemon=True).start()
        threading.Thread(target=self.replication_manager, daemon=True).start()
        threading.Thread(target=self.statistics_reporter, daemon=True).start()
        
        # Accept client connections
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                threading.Thread(target=self.handle_client, 
                               args=(client_socket, addr), daemon=True).start()
            except Exception as e:
                if self.running:
                    print(f"[ERROR] Accept error: {e}")
    
    def handle_client(self, client_socket: socket.socket, addr):
        """Handle client requests."""
        try:
            # Receive request
            data = client_socket.recv(65536)
            if not data:
                return
            
            request = json.loads(data.decode('utf-8'))
            command = request.get('command')
            
            # Process command
            if command == 'register_datanode':
                response = self.register_datanode(request)
            elif command == 'heartbeat':
                response = self.handle_heartbeat(request)
            elif command == 'upload_init':
                response = self.handle_upload_init(request)
            elif command == 'upload_complete':
                response = self.handle_upload_complete(request)
            elif command == 'download_init':
                response = self.handle_download_init(request)
            elif command == 'list_files':
                response = self.list_files()
            elif command == 'delete_file':
                response = self.delete_file(request)
            elif command == 'file_info':
                response = self.get_file_info(request)
            elif command == 'cluster_status':
                response = self.get_cluster_status()
            else:
                response = {'status': 'error', 'message': f'Unknown command: {command}'}
            
            # Send response
            client_socket.sendall(json.dumps(response).encode('utf-8'))
            
        except Exception as e:
            print(f"[ERROR] Client handler error: {e}")
            error_response = {'status': 'error', 'message': str(e)}
            try:
                client_socket.sendall(json.dumps(error_response).encode('utf-8'))
            except:
                pass
        finally:
            client_socket.close()
    
    def register_datanode(self, request: dict) -> dict:
        """Register a new DataNode."""
        node_id = request.get('node_id')
        host = request.get('host')
        port = request.get('port')
        
        with self.datanodes_lock:
            if node_id not in self.datanodes:
                self.datanodes[node_id] = DataNodeInfo(node_id, host, port)
                print(f"[DATANODE] Registered: {node_id} ({host}:{port})")
                return {'status': 'success', 'message': 'DataNode registered'}
            else:
                return {'status': 'success', 'message': 'DataNode already registered'}
    
    def handle_heartbeat(self, request: dict) -> dict:
        """Handle DataNode heartbeat."""
        node_id = request.get('node_id')
        available_space = request.get('available_space', 0)
        total_space = request.get('total_space', 0)
        chunks = request.get('chunks', [])
        
        with self.datanodes_lock:
            if node_id in self.datanodes:
                self.datanodes[node_id].update_heartbeat(available_space, total_space, chunks)
                return {'status': 'success'}
            else:
                return {'status': 'error', 'message': 'DataNode not registered'}
    
    def handle_upload_init(self, request: dict) -> dict:
        """Initialize file upload."""
        filename = request.get('filename')
        filesize = request.get('filesize')
        
        # Calculate chunks
        num_chunks = (filesize + self.chunk_size - 1) // self.chunk_size
        
        # Select DataNodes for each chunk
        chunk_assignments = {}
        
        for chunk_id in range(num_chunks):
            # Select DataNodes with most available space
            datanodes = self.select_datanodes_for_chunk(self.replication_factor)
            if len(datanodes) < self.replication_factor:
                return {
                    'status': 'error',
                    'message': f'Insufficient DataNodes. Need {self.replication_factor}, found {len(datanodes)}'
                }
            chunk_assignments[chunk_id] = datanodes
        
        return {
            'status': 'success',
            'chunk_size': self.chunk_size,
            'num_chunks': num_chunks,
            'chunk_assignments': chunk_assignments
        }
    
    def handle_upload_complete(self, request: dict) -> dict:
        """Complete file upload."""
        filename = request.get('filename')
        filesize = request.get('filesize')
        chunks = request.get('chunks')  # {chunk_id: [datanode_ids]}
        
        with self.files_lock:
            # Create file metadata
            metadata = FileMetadata(filename, filesize, self.chunk_size, self.replication_factor)
            
            # Add chunk locations
            for chunk_id_str, datanode_ids in chunks.items():
                chunk_id = int(chunk_id_str)
                for datanode_id in datanode_ids:
                    metadata.add_chunk_location(chunk_id, datanode_id)
            
            self.files[filename] = metadata
            
            print(f"[FILE] Uploaded: {filename} ({filesize} bytes, {len(chunks)} chunks)")
            
            return {'status': 'success', 'message': f'File {filename} uploaded successfully'}
    
    def handle_download_init(self, request: dict) -> dict:
        """Initialize file download."""
        filename = request.get('filename')
        
        with self.files_lock:
            if filename not in self.files:
                return {'status': 'error', 'message': f'File not found: {filename}'}
            
            metadata = self.files[filename]
            
            # Build chunk location map (prefer healthy DataNodes)
            chunk_locations = {}
            for chunk_id, datanode_ids in metadata.chunks.items():
                # Filter healthy DataNodes
                healthy_nodes = []
                for node_id in datanode_ids:
                    if node_id in self.datanodes and self.datanodes[node_id].is_healthy():
                        node_info = self.datanodes[node_id]
                        healthy_nodes.append({
                            'node_id': node_id,
                            'host': node_info.host,
                            'port': node_info.port
                        })
                
                if not healthy_nodes:
                    return {
                        'status': 'error',
                        'message': f'No healthy DataNodes for chunk {chunk_id}'
                    }
                
                chunk_locations[chunk_id] = healthy_nodes
            
            return {
                'status': 'success',
                'filename': filename,
                'filesize': metadata.size,
                'chunk_size': metadata.chunk_size,
                'chunk_locations': chunk_locations
            }
    
    def list_files(self) -> dict:
        """List all files."""
        with self.files_lock:
            files = []
            for filename, metadata in self.files.items():
                files.append({
                    'filename': filename,
                    'size': metadata.size,
                    'chunks': len(metadata.chunks),
                    'created_at': datetime.fromtimestamp(metadata.created_at).strftime('%Y-%m-%d %H:%M:%S')
                })
            
            return {'status': 'success', 'files': files}
    
    def delete_file(self, request: dict) -> dict:
        """Delete a file."""
        filename = request.get('filename')
        
        with self.files_lock:
            if filename not in self.files:
                return {'status': 'error', 'message': f'File not found: {filename}'}
            
            # TODO: Send delete commands to DataNodes
            del self.files[filename]
            
            print(f"[FILE] Deleted: {filename}")
            
            return {'status': 'success', 'message': f'File {filename} deleted'}
    
    def get_file_info(self, request: dict) -> dict:
        """Get file information."""
        filename = request.get('filename')
        
        with self.files_lock:
            if filename not in self.files:
                return {'status': 'error', 'message': f'File not found: {filename}'}
            
            metadata = self.files[filename]
            
            return {
                'status': 'success',
                'file': metadata.to_dict()
            }
    
    def get_cluster_status(self) -> dict:
        """Get cluster status."""
        with self.datanodes_lock:
            datanodes = [node.to_dict() for node in self.datanodes.values()]
        
        with self.files_lock:
            total_files = len(self.files)
            total_size = sum(f.size for f in self.files.values())
        
        return {
            'status': 'success',
            'datanodes': datanodes,
            'total_files': total_files,
            'total_size': total_size
        }
    
    def select_datanodes_for_chunk(self, count: int) -> List[dict]:
        """Select DataNodes for chunk storage."""
        with self.datanodes_lock:
            # Get healthy DataNodes sorted by available space
            healthy_nodes = [
                node for node in self.datanodes.values()
                if node.is_healthy()
            ]
            
            # Sort by available space (descending)
            healthy_nodes.sort(key=lambda n: n.available_space, reverse=True)
            
            # Select top N nodes
            selected = healthy_nodes[:count]
            
            return [
                {
                    'node_id': node.node_id,
                    'host': node.host,
                    'port': node.port
                }
                for node in selected
            ]
    
    def heartbeat_monitor(self):
        """Monitor DataNode heartbeats."""
        while self.running:
            time.sleep(10)
            
            with self.datanodes_lock:
                dead_nodes = []
                for node_id, node in self.datanodes.items():
                    if not node.is_healthy():
                        if node.is_alive:
                            node.is_alive = False
                            dead_nodes.append(node_id)
                            print(f"[DATANODE] Dead: {node_id} (no heartbeat)")
            
            # Handle dead nodes
            if dead_nodes:
                self.handle_node_failures(dead_nodes)
    
    def handle_node_failures(self, dead_nodes: List[str]):
        """Handle DataNode failures."""
        with self.files_lock:
            for filename, metadata in self.files.items():
                for chunk_id in list(metadata.chunks.keys()):
                    for dead_node in dead_nodes:
                        if dead_node in metadata.chunks[chunk_id]:
                            metadata.remove_chunk_location(chunk_id, dead_node)
                            print(f"[REPLICATION] Removed {dead_node} from {filename} chunk {chunk_id}")
    
    def replication_manager(self):
        """Manage chunk replication."""
        while self.running:
            time.sleep(30)
            
            with self.files_lock:
                for filename, metadata in self.files.items():
                    under_replicated = metadata.is_under_replicated()
                    
                    if under_replicated:
                        print(f"[REPLICATION] Under-replicated chunks in {filename}: {under_replicated}")
                        # TODO: Trigger re-replication
    
    def statistics_reporter(self):
        """Report cluster statistics."""
        while self.running:
            time.sleep(30)
            
            with self.datanodes_lock:
                total_nodes = len(self.datanodes)
                healthy_nodes = sum(1 for n in self.datanodes.values() if n.is_healthy())
            
            with self.files_lock:
                total_files = len(self.files)
                total_chunks = sum(len(f.chunks) for f in self.files.values())
            
            print(f"\n[STATS] Nodes: {healthy_nodes}/{total_nodes} | Files: {total_files} | Chunks: {total_chunks}\n")
    
    def stop(self):
        """Stop the NameNode server."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()


def main():
    """Main entry point."""
    namenode = NameNode()
    try:
        namenode.start()
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down NameNode...")
        namenode.stop()


if __name__ == '__main__':
    main()
