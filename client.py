#!/usr/bin/env python3
"""
Task 23: DFS Client
Client for uploading and downloading files from distributed file system.
"""

import socket
import json
import os
import sys
import hashlib
from typing import Dict, List


class DFSClient:
    """Client for distributed file system operations."""
    
    def __init__(self, namenode_host: str = 'localhost', namenode_port: int = 8000):
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
    
    def send_to_namenode(self, request: dict) -> dict:
        """Send request to NameNode."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.namenode_host, self.namenode_port))
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            data = sock.recv(65536)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            return response
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def send_to_datanode(self, host: str, port: int, request: dict) -> dict:
        """Send request to DataNode."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((host, port))
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            data = sock.recv(65536)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            return response
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def upload_file(self, local_path: str, remote_filename: str = None):
        """Upload a file to DFS."""
        if not os.path.exists(local_path):
            print(f"[ERROR] File not found: {local_path}")
            return False
        
        if not remote_filename:
            remote_filename = os.path.basename(local_path)
        
        filesize = os.path.getsize(local_path)
        
        print(f"[UPLOAD] File: {local_path} -> {remote_filename}")
        print(f"[UPLOAD] Size: {filesize} bytes ({filesize / 1024:.2f} KB)")
        
        # Step 1: Initialize upload with NameNode
        request = {
            'command': 'upload_init',
            'filename': remote_filename,
            'filesize': filesize
        }
        
        response = self.send_to_namenode(request)
        
        if response.get('status') != 'success':
            print(f"[ERROR] Upload init failed: {response.get('message')}")
            return False
        
        chunk_size = response['chunk_size']
        num_chunks = response['num_chunks']
        chunk_assignments = response['chunk_assignments']
        
        print(f"[UPLOAD] Chunks: {num_chunks} x {chunk_size} bytes")
        
        # Step 2: Split file and upload chunks
        uploaded_chunks = {}
        
        with open(local_path, 'rb') as f:
            for chunk_id in range(num_chunks):
                # Read chunk
                chunk_data = f.read(chunk_size)
                
                print(f"[UPLOAD] Chunk {chunk_id}/{num_chunks-1} ({len(chunk_data)} bytes)... ", end='', flush=True)
                
                # Get DataNodes for this chunk
                datanodes = chunk_assignments.get(str(chunk_id), [])
                
                # Upload to each DataNode
                successful_uploads = []
                for datanode_info in datanodes:
                    node_id = datanode_info['node_id']
                    host = datanode_info['host']
                    port = datanode_info['port']
                    
                    chunk_id_str = f"chunk_{remote_filename}_{chunk_id}"
                    
                    # Store chunk
                    if self.store_chunk_to_datanode(host, port, chunk_id_str, chunk_data):
                        successful_uploads.append(node_id)
                
                if successful_uploads:
                    uploaded_chunks[chunk_id] = successful_uploads
                    print(f"✓ (stored on {len(successful_uploads)} nodes)")
                else:
                    print("✗ Failed")
                    return False
        
        # Step 3: Notify NameNode of completion
        request = {
            'command': 'upload_complete',
            'filename': remote_filename,
            'filesize': filesize,
            'chunks': uploaded_chunks
        }
        
        response = self.send_to_namenode(request)
        
        if response.get('status') == 'success':
            print(f"[SUCCESS] File uploaded: {remote_filename}")
            return True
        else:
            print(f"[ERROR] Upload complete failed: {response.get('message')}")
            return False
    
    def store_chunk_to_datanode(self, host: str, port: int, chunk_id: str, chunk_data: bytes) -> bool:
        """Store chunk to DataNode."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((host, port))
            
            # Send store request
            request = {
                'command': 'store_chunk',
                'chunk_id': chunk_id,
                'chunk_size': len(chunk_data)
            }
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            # Wait for ready signal
            signal = sock.recv(5)
            if signal != b'READY':
                sock.close()
                return False
            
            # Send chunk data
            sock.sendall(chunk_data)
            
            # Receive confirmation
            data = sock.recv(65536)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            return response.get('status') == 'success'
            
        except Exception as e:
            print(f"\n[ERROR] Store chunk error: {e}")
            return False
    
    def download_file(self, remote_filename: str, local_path: str = None):
        """Download a file from DFS."""
        if not local_path:
            local_path = remote_filename
        
        print(f"[DOWNLOAD] File: {remote_filename} -> {local_path}")
        
        # Step 1: Initialize download with NameNode
        request = {
            'command': 'download_init',
            'filename': remote_filename
        }
        
        response = self.send_to_namenode(request)
        
        if response.get('status') != 'success':
            print(f"[ERROR] Download init failed: {response.get('message')}")
            return False
        
        filesize = response['filesize']
        chunk_size = response['chunk_size']
        chunk_locations = response['chunk_locations']
        
        num_chunks = len(chunk_locations)
        
        print(f"[DOWNLOAD] Size: {filesize} bytes ({filesize / 1024:.2f} KB)")
        print(f"[DOWNLOAD] Chunks: {num_chunks}")
        
        # Step 2: Download chunks
        chunks_data = {}
        
        for chunk_id_str, datanodes in chunk_locations.items():
            chunk_id = int(chunk_id_str)
            
            print(f"[DOWNLOAD] Chunk {chunk_id}/{num_chunks-1}... ", end='', flush=True)
            
            # Try each DataNode until successful
            chunk_retrieved = False
            for datanode_info in datanodes:
                host = datanode_info['host']
                port = datanode_info['port']
                
                chunk_id_full = f"chunk_{remote_filename}_{chunk_id}"
                
                chunk_data = self.retrieve_chunk_from_datanode(host, port, chunk_id_full)
                
                if chunk_data:
                    chunks_data[chunk_id] = chunk_data
                    chunk_retrieved = True
                    print(f"✓ ({len(chunk_data)} bytes)")
                    break
            
            if not chunk_retrieved:
                print("✗ Failed")
                return False
        
        # Step 3: Reconstruct file
        print(f"[DOWNLOAD] Reconstructing file...")
        
        try:
            with open(local_path, 'wb') as f:
                for chunk_id in sorted(chunks_data.keys()):
                    f.write(chunks_data[chunk_id])
            
            print(f"[SUCCESS] File downloaded: {local_path}")
            return True
            
        except Exception as e:
            print(f"[ERROR] Reconstruction failed: {e}")
            return False
    
    def retrieve_chunk_from_datanode(self, host: str, port: int, chunk_id: str) -> bytes:
        """Retrieve chunk from DataNode."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((host, port))
            
            # Send retrieve request
            request = {
                'command': 'retrieve_chunk',
                'chunk_id': chunk_id
            }
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            # Receive response header
            data = sock.recv(65536)
            response = json.loads(data.decode('utf-8'))
            
            if response.get('status') != 'success':
                sock.close()
                return None
            
            chunk_size = response['size']
            
            # Send ready signal
            sock.sendall(b'READY')
            
            # Receive chunk data
            chunk_data = b''
            remaining = chunk_size
            
            while remaining > 0:
                data = sock.recv(min(8192, remaining))
                if not data:
                    break
                chunk_data += data
                remaining -= len(data)
            
            sock.close()
            
            return chunk_data
            
        except Exception as e:
            return None
    
    def list_files(self):
        """List all files in DFS."""
        request = {
            'command': 'list_files'
        }
        
        response = self.send_to_namenode(request)
        
        if response.get('status') != 'success':
            print(f"[ERROR] List files failed: {response.get('message')}")
            return
        
        files = response.get('files', [])
        
        if not files:
            print("[INFO] No files in DFS")
            return
        
        print("\n" + "="*80)
        print(f"{'FILES IN DISTRIBUTED FILE SYSTEM':^80}")
        print("="*80)
        print(f"{'Filename':<40} {'Size':<15} {'Chunks':<10} {'Created':<20}")
        print("-"*80)
        
        for file_info in files:
            filename = file_info['filename']
            size = file_info['size']
            chunks = file_info['chunks']
            created = file_info['created_at']
            
            size_str = f"{size} bytes ({size / 1024:.2f} KB)"
            
            print(f"{filename:<40} {size_str:<15} {chunks:<10} {created:<20}")
        
        print("-"*80)
        print(f"Total files: {len(files)}")
        print("="*80 + "\n")
    
    def delete_file(self, remote_filename: str):
        """Delete a file from DFS."""
        request = {
            'command': 'delete_file',
            'filename': remote_filename
        }
        
        response = self.send_to_namenode(request)
        
        if response.get('status') == 'success':
            print(f"[SUCCESS] File deleted: {remote_filename}")
            return True
        else:
            print(f"[ERROR] Delete failed: {response.get('message')}")
            return False
    
    def file_info(self, remote_filename: str):
        """Get file information."""
        request = {
            'command': 'file_info',
            'filename': remote_filename
        }
        
        response = self.send_to_namenode(request)
        
        if response.get('status') != 'success':
            print(f"[ERROR] File info failed: {response.get('message')}")
            return
        
        file_data = response['file']
        
        print("\n" + "="*80)
        print(f"{'FILE INFORMATION':^80}")
        print("="*80)
        print(f"Filename: {file_data['filename']}")
        print(f"Size: {file_data['size']} bytes ({file_data['size'] / 1024:.2f} KB)")
        print(f"Chunk Size: {file_data['chunk_size']} bytes")
        print(f"Replication Factor: {file_data['replication_factor']}")
        print(f"Chunks: {len(file_data['chunks'])}")
        print()
        print("Chunk Locations:")
        print("-"*80)
        
        for chunk_id_str, datanode_ids in file_data['chunks'].items():
            print(f"  Chunk {chunk_id_str}: {', '.join(datanode_ids)}")
        
        print("="*80 + "\n")
    
    def cluster_status(self):
        """Get cluster status."""
        request = {
            'command': 'cluster_status'
        }
        
        response = self.send_to_namenode(request)
        
        if response.get('status') != 'success':
            print(f"[ERROR] Cluster status failed: {response.get('message')}")
            return
        
        datanodes = response.get('datanodes', [])
        total_files = response.get('total_files', 0)
        total_size = response.get('total_size', 0)
        
        print("\n" + "="*90)
        print(f"{'CLUSTER STATUS':^90}")
        print("="*90)
        print(f"Total Files: {total_files}")
        print(f"Total Size: {total_size} bytes ({total_size / 1024 / 1024:.2f} MB)")
        print()
        print("DataNodes:")
        print("-"*90)
        print(f"{'Node ID':<15} {'Host:Port':<25} {'Status':<10} {'Chunks':<10} {'Space':<30}")
        print("-"*90)
        
        for node_info in datanodes:
            node_id = node_info['node_id']
            host_port = f"{node_info['host']}:{node_info['port']}"
            status = "Alive" if node_info['is_alive'] else "Dead"
            chunks = node_info['chunk_count']
            
            available = node_info['available_space']
            total = node_info['total_space']
            space_str = f"{available / 1024 / 1024:.0f} MB / {total / 1024 / 1024:.0f} MB"
            
            print(f"{node_id:<15} {host_port:<25} {status:<10} {chunks:<10} {space_str:<30}")
        
        print("="*90 + "\n")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='DFS Client')
    parser.add_argument('--namenode-host', type=str, default='localhost', help='NameNode host')
    parser.add_argument('--namenode-port', type=int, default=8000, help='NameNode port')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Upload
    upload_parser = subparsers.add_parser('upload', help='Upload file')
    upload_parser.add_argument('local_path', help='Local file path')
    upload_parser.add_argument('--remote', help='Remote filename')
    
    # Download
    download_parser = subparsers.add_parser('download', help='Download file')
    download_parser.add_argument('remote_filename', help='Remote filename')
    download_parser.add_argument('--local', help='Local file path')
    
    # List
    list_parser = subparsers.add_parser('list', help='List files')
    
    # Delete
    delete_parser = subparsers.add_parser('delete', help='Delete file')
    delete_parser.add_argument('remote_filename', help='Remote filename')
    
    # Info
    info_parser = subparsers.add_parser('info', help='File information')
    info_parser.add_argument('remote_filename', help='Remote filename')
    
    # Status
    status_parser = subparsers.add_parser('status', help='Cluster status')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    client = DFSClient(args.namenode_host, args.namenode_port)
    
    if args.command == 'upload':
        client.upload_file(args.local_path, args.remote)
    elif args.command == 'download':
        client.download_file(args.remote_filename, args.local)
    elif args.command == 'list':
        client.list_files()
    elif args.command == 'delete':
        client.delete_file(args.remote_filename)
    elif args.command == 'info':
        client.file_info(args.remote_filename)
    elif args.command == 'status':
        client.cluster_status()


if __name__ == '__main__':
    main()
