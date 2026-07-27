#!/usr/bin/env python3
"""
Python SQL Server for STOMP Server
Listens on a socket and executes SQL commands on SQLite database
"""

import errno
import socket
import sqlite3
import sys
import threading
from datetime import datetime

# Database file
DB_FILE = 'stomp_server.db'

def init_database():
    """Initialize the database with required tables"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            registration_date TEXT NOT NULL
        )
    ''')
    
    # Login history table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS login_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            login_time TEXT NOT NULL,
            logout_time TEXT,
            FOREIGN KEY (username) REFERENCES users(username)
        )
    ''')
    
    # File tracking table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            filename TEXT NOT NULL,
            upload_time TEXT NOT NULL,
            game_channel TEXT,
            FOREIGN KEY (username) REFERENCES users(username)
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized successfully")

def execute_sql(sql_command):
    """Execute SQL command and return result"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Execute the SQL command
        cursor.execute(sql_command)
        
        # Check if it's a SELECT query
        if sql_command.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            conn.close()
            return f"SUCCESS:{len(results)}|" + "|".join([str(row) for row in results])
        else:
            # For INSERT, UPDATE, DELETE
            conn.commit()
            affected_rows = cursor.rowcount
            conn.close()
            return f"SUCCESS:{affected_rows}"
            
    except sqlite3.Error as e:
        return f"ERROR:{str(e)}"
    except Exception as e:
        return f"ERROR:{str(e)}"

def handle_client(client_socket, addr):
    """Handle client connection"""
    print(f"Client connected from {addr}")
    
    try:
        while True:
            # Receive SQL command (terminated by null character)
            data = b''
            while True:
                chunk = client_socket.recv(1024)
                if not chunk:
                    break
                data += chunk
                if b'\0' in data:
                    break
            
            if not data:
                break
                
            # Remove null terminator and decode
            sql_command = data.rstrip(b'\0').decode('utf-8')
            
            if not sql_command:
                break
                
            print(f"Executing SQL: {sql_command[:100]}...")
            
            # Execute SQL and get result
            result = execute_sql(sql_command)
            
            # Send result back with null terminator
            client_socket.sendall((result + '\0').encode('utf-8'))
            
    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    finally:
        client_socket.close()
        print(f"Client {addr} disconnected")

def is_server_running(host, port):
    """Check whether a server is already bound to host/port"""
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.5)
        return probe.connect_ex((host, port)) == 0
    except OSError:
        return False
    finally:
        probe.close()

def start_server(host='127.0.0.1', port=7779):
    """Start the SQL server"""
    if is_server_running(host, port):
        print(f"SQL Server already running on {host}:{port}")
        print("Stop the existing process or choose a different port.")
        return

    # Initialize database
    init_database()
    
    # Create server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"SQL Server started on {host}:{port}")
        print(f"Database: {DB_FILE}")
        print("Waiting for connections...")
        
        while True:
            client_socket, addr = server_socket.accept()
            # Handle each client in a separate thread
            client_thread = threading.Thread(target=handle_client, args=(client_socket, addr))
            client_thread.daemon = True
            client_thread.start()
            
    except OSError as err:
        if err.errno == errno.EADDRINUSE:
            print(f"Port {port} is currently in use. Stop the other service or pick a new port.")
        else:
            raise
    except KeyboardInterrupt:
        print("\nShutting down SQL server...")
    finally:
        server_socket.close()

if __name__ == '__main__':
    port = 7779
    if len(sys.argv) > 1:
        raw_port = sys.argv[1].strip()
        try:
            port = int(raw_port)
        except ValueError:
            print(f"Invalid port '{raw_port}', falling back to default {port}")
    start_server(port=port)
