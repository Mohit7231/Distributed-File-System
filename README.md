Project Description
This project is a Distributed File System developed in C using UNIX socket programming. It simulates a real-world system where files are routed and stored based on their type, all through a centralized interface. Below are the key highlights:

📁 Modular Server Architecture

S1: Main server that handles client requests and routes files
S2: Stores .pdf files
S3: Stores .txt files
S4: Stores .zip files
💻 Client Interface (w25clients)

Command-line based client tool
Interacts only with S1, unaware of other servers
Allows upload, download, delete, list, and tar operations
🔁 Concurrent Processing

S1 uses fork() to serve multiple clients simultaneously
S2, S3, S4 use threads (pthreads) for parallel handling of requests
🧠 Intelligent File Routing

.c files are stored directly in ~/S1
.pdf, .txt, and .zip files are forwarded to S2, S3, and S4 respectively
Non-.c files are deleted from S1 after being forwarded
📂 File Operations Supported

uploadf <filename> <S1/filepath>
downloadf <filename> <S1/filepath>
removef <S1/filepath/filename>
dispfnames <S1/dir>
downltar <filetype>
🛠 Core Concepts Demonstrated

Socket programming
Inter-process and multi-threaded communication
File I/O and directory management
Distributed system behavior using local simulation
This project is ideal for understanding system-level programming and networking in a Linux environment. It provides hands-on experience with real-world tools like sockets, processes, threads, and file systems.
