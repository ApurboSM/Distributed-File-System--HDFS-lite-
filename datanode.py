#!/usr/bin/env python3
"""
Task 23: DataNode (Chunk Server)
Stores file chunks and reports health to NameNode.
"""

import socket
import threading
import json
import time
import os
import shutil
import hashlib
from typing import Dict, List


class DataNode:
    """DataNode - Chunk server for distributed file system."""
    
    def __init__(self, node_id: str, host: str = 'localhost', port: int = 8001,
                 storage_dir: str = None, namenode_host: str = 'localhost', namenode_port: int = 8000):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        
        # Storage
        self.storage_dir = storage_dir or f"datanode_{node_id}_storage"
        self.ensure_storage_dir()
        
        # Server state
        self.running = False
        self.server_socket = None
        
        # Chunk tracking
        self.chunks: Dict[str, str] = {}  # chunk_id -> file_path
        self.load_chunks()
    
    def ensure_storage_dir(self):
        """Ensure storage directory exists."""
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
    
    def load_chunks(self):
        """Load existing chunks from storage."""
        self.chunks = {}
        if os.path.exists(self.storage_dir):
            for filename in os.listdir(self.storage_dir):
                if filename.startswith('chunk_'):
                    chunk_id = filename
                    self.chunks[chunk_id] = os.path.join(self.storage_dir, filename)
    
    def get_storage_info(self) -> tuple:
        """Get storage information."""
        stat = shutil.disk_usage(self.storage_dir)
        return stat.free, stat.total
    
    def start(self):
        """Start the DataNode server."""
        self.running = True
        
        # Register with NameNode
        if not self.register_with_namenode():
            print("[ERROR] Failed to register with NameNode")
            return
        
        # Start server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        print("="*70)
        print(f"{'DataNode Server Started':^70}")
        print("="*70)
        print(f"Node ID: {self.node_id}")
        print(f"Host: {self.host}")
        print(f"Port: {self.port}")
        print(f"Storage: {self.storage_dir}")
        print(f"NameNode: {self.namenode_host}:{self.namenode_port}")
        print("="*70)
        print()
        
        # Start background threads
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        
        # Accept client connections
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                threading.Thread(target=self.handle_client,
                               args=(client_socket, addr), daemon=True).start()
            except Exception as e:
                if self.running:
                    print(f"[ERROR] Accept error: {e}")
    
    def register_with_namenode(self) -> bool:
        """Register with NameNode."""
        try:
            request = {
                'command': 'register_datanode',
                'node_id': self.node_id,
                'host': self.host,
                'port': self.port
            }
            
            response = self.send_to_namenode(request)
            
            if response.get('status') == 'success':
                print(f"[INFO] Registered with NameNode at {self.namenode_host}:{self.namenode_port}")
                return True
            else:
                print(f"[ERROR] Registration failed: {response.get('message')}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Registration error: {e}")
            return False
    
    def send_to_namenode(self, request: dict) -> dict:
        """Send request to NameNode."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((self.namenode_host, self.namenode_port))
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            data = sock.recv(65536)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            return response
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def heartbeat_loop(self):
        """Send periodic heartbeats to NameNode."""
        while self.running:
            time.sleep(10)
            
            try:
                available, total = self.get_storage_info()
                
                request = {
                    'command': 'heartbeat',
                    'node_id': self.node_id,
                    'available_space': available,
                    'total_space': total,
                    'chunks': list(self.chunks.keys())
                }
                
                response = self.send_to_namenode(request)
                
                if response.get('status') != 'success':
                    print(f"[WARNING] Heartbeat failed: {response.get('message')}")
                    
            except Exception as e:
                print(f"[ERROR] Heartbeat error: {e}")
    
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
            if command == 'store_chunk':
                response = self.store_chunk(request, client_socket)
            elif command == 'retrieve_chunk':
                response = self.retrieve_chunk(request, client_socket)
            elif command == 'delete_chunk':
                response = self.delete_chunk(request)
            elif command == 'replicate_chunk':
                response = self.replicate_chunk(request)
            else:
                response = {'status': 'error', 'message': f'Unknown command: {command}'}
            
            # Send response
            if command not in ['store_chunk', 'retrieve_chunk']:
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
    
    def store_chunk(self, request: dict, client_socket: socket.socket) -> dict:
        """Store a chunk."""
        try:
            chunk_id = request.get('chunk_id')
            chunk_size = request.get('chunk_size')
            
            # Send ready signal
            client_socket.sendall(b'READY')
            
            # Receive chunk data
            chunk_data = b''
            remaining = chunk_size
            
            while remaining > 0:
                data = client_socket.recv(min(8192, remaining))
                if not data:
                    break
                chunk_data += data
                remaining -= len(data)
            
            # Store chunk
            chunk_path = os.path.join(self.storage_dir, chunk_id)
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)
            
            self.chunks[chunk_id] = chunk_path
            
            # Calculate checksum
            checksum = hashlib.md5(chunk_data).hexdigest()
            
            print(f"[CHUNK] Stored: {chunk_id} ({len(chunk_data)} bytes)")
            
            # Send success response
            response = {
                'status': 'success',
                'chunk_id': chunk_id,
                'size': len(chunk_data),
                'checksum': checksum
            }
            client_socket.sendall(json.dumps(response).encode('utf-8'))
            
            return response
            
        except Exception as e:
            print(f"[ERROR] Store chunk error: {e}")
            response = {'status': 'error', 'message': str(e)}
            try:
                client_socket.sendall(json.dumps(response).encode('utf-8'))
            except:
                pass
            return response
    
    def retrieve_chunk(self, request: dict, client_socket: socket.socket) -> dict:
        """Retrieve a chunk."""
        try:
            chunk_id = request.get('chunk_id')
            
            if chunk_id not in self.chunks:
                response = {'status': 'error', 'message': f'Chunk not found: {chunk_id}'}
                client_socket.sendall(json.dumps(response).encode('utf-8'))
                return response
            
            # Read chunk
            chunk_path = self.chunks[chunk_id]
            with open(chunk_path, 'rb') as f:
                chunk_data = f.read()
            
            # Send response header
            response = {
                'status': 'success',
                'chunk_id': chunk_id,
                'size': len(chunk_data)
            }
            client_socket.sendall(json.dumps(response).encode('utf-8'))
            
            # Wait for ready signal
            signal = client_socket.recv(5)
            if signal != b'READY':
                return response
            
            # Send chunk data
            client_socket.sendall(chunk_data)
            
            print(f"[CHUNK] Retrieved: {chunk_id} ({len(chunk_data)} bytes)")
            
            return response
            
        except Exception as e:
            print(f"[ERROR] Retrieve chunk error: {e}")
            response = {'status': 'error', 'message': str(e)}
            try:
                client_socket.sendall(json.dumps(response).encode('utf-8'))
            except:
                pass
            return response
    
    def delete_chunk(self, request: dict) -> dict:
        """Delete a chunk."""
        try:
            chunk_id = request.get('chunk_id')
            
            if chunk_id not in self.chunks:
                return {'status': 'error', 'message': f'Chunk not found: {chunk_id}'}
            
            # Delete file
            chunk_path = self.chunks[chunk_id]
            if os.path.exists(chunk_path):
                os.remove(chunk_path)
            
            del self.chunks[chunk_id]
            
            print(f"[CHUNK] Deleted: {chunk_id}")
            
            return {'status': 'success', 'message': f'Chunk {chunk_id} deleted'}
            
        except Exception as e:
            print(f"[ERROR] Delete chunk error: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def replicate_chunk(self, request: dict) -> dict:
        """Replicate a chunk to another DataNode."""
        # TODO: Implement chunk replication
        return {'status': 'error', 'message': 'Not implemented'}
    
    def stop(self):
        """Stop the DataNode server."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='DataNode Server')
    parser.add_argument('--id', type=str, required=True, help='Node ID')
    parser.add_argument('--host', type=str, default='localhost', help='DataNode host')
    parser.add_argument('--port', type=int, default=8001, help='DataNode port')
    parser.add_argument('--storage', type=str, help='Storage directory')
    parser.add_argument('--namenode-host', type=str, default='localhost', help='NameNode host')
    parser.add_argument('--namenode-port', type=int, default=8000, help='NameNode port')
    
    args = parser.parse_args()
    
    datanode = DataNode(
        node_id=args.id,
        host=args.host,
        port=args.port,
        storage_dir=args.storage,
        namenode_host=args.namenode_host,
        namenode_port=args.namenode_port
    )
    
    try:
        datanode.start()
    except KeyboardInterrupt:
        print(f"\n[INFO] Shutting down DataNode {args.id}...")
        datanode.stop()


if __name__ == '__main__':
    main()
