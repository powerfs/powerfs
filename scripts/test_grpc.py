#!/usr/bin/env python3
"""Simple gRPC test for PowerFS Filer."""

import grpc
import sys
import os

# Add the powerfs proto to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# We need to compile the proto files first
# For now, let's just test the connection

def test_connection(host, port):
    """Test basic gRPC connection."""
    target = f"{host}:{port}"
    
    # Create channel
    channel = grpc.insecure_channel(target)
    
    try:
        # Try to connect and check if server is responding
        # We'll use a simple approach: just try to create a stub and make a call
        
        # Since we don't have the generated code, let's just test the connection
        # by trying to create a channel and check if it's ready
        
        # Wait for channel to be ready
        grpc.channel_ready_future(channel).result(timeout=5)
        print(f"✅ Successfully connected to gRPC server at {target}")
        
        # Check channel state
        state = channel.get_state()
        print(f"   Channel state: {state}")
        
        return True
        
    except grpc.FutureTimeoutError:
        print(f"❌ Connection timeout: gRPC server at {target} not responding")
        return False
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    finally:
        channel.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <host> <port>")
        print(f"Example: {sys.argv[0]} 127.0.0.1 18889")
        sys.exit(1)
    
    host = sys.argv[1]
    port = int(sys.argv[2])
    
    success = test_connection(host, port)
    sys.exit(0 if success else 1)
