/***************************************************************************************
 * w25clients.c - A Client Interface for the Distributed File System
 * -------------------------------------------------------------------------------------
 * This client program establishes a single socket connection to the main server (S1)
 * and remains connected for the duration of the session. The user can issue commands
 * to upload, download, remove files, display filenames in a directory, and download
 * tar files of specific file types. Each command is parsed locally, then relayed to
 * the server for processing. The server's response is subsequently handled by the
 * client, providing feedback and performing any necessary local file I/O.

 ***************************************************************************************/

#include <stdio.h>      // For standard I/O functions like printf
#include <stdlib.h>     // For general utilities like malloc, free, exit
#include <string.h>     // For string manipulation functions (strcpy, strtok, etc.)
#include <unistd.h>     // For close(), read(), write(), and other POSIX calls
#include <fcntl.h>      // For open() flags and modes (O_RDONLY, O_WRONLY, etc.)
#include <errno.h>      // For error codes (errno) and error messages (strerror)
#include <sys/socket.h> // For socket-related functions and structures
#include <netinet/in.h> // For sockaddr_in structure (Internet addresses)
#include <arpa/inet.h>  // For inet_addr()
#include <sys/select.h> // For select() functionality

#define SERVER_PORT 8301         // The port on which S1 (main server) listens
#define MAX_INPUT   1024         // Maximum size for user input commands
#define CHUNK_SIZE  1024         // Chunk size for file data transfers

/*
 * We keep a single global socket descriptor (g_sock) that is shared
 * by all operations. This means we connect once to the server and
 * continue to reuse the connection for every command, closing only
 * when the user is done or if the server disconnects.
 */
static int g_sock = -1;

/**
 * ---------------------------------------------------------------------------------------
 * read_line_client:
 *   This function attempts to read data from the file descriptor 'fd', one byte at a time,
 *   storing the read characters into 'buffer'. It stops when either:
 *     1) A newline character is encountered,
 *     2) The total number of characters read reaches 'maxlen - 1',
 *     3) Or the server side closes the connection / an error occurs.
 *
 *   If the server closes the connection (rc == 0) or there's a read error, we print a
 *   disconnect message, close our global socket if it's open, and mark g_sock as -1.
 *
 *   Returns: The number of characters read (including the newline), or -1 on error
 * ---------------------------------------------------------------------------------------
 */
ssize_t read_line_client(int fd, void *buffer, size_t maxlen) {
    ssize_t bytesRead, rc;       // bytesRead tracks how many chars have been read so far
    char c;                      // temporary storage for each character read
    char *bufPtr = (char *)buffer; // buffer pointer for storing data

    // Start from 1 for convenience; we'll read up to maxlen-1 chars
    for(bytesRead = 1; bytesRead < maxlen; bytesRead++) {
        rc = read(fd, &c, 1);  // attempt to read one character
        if(rc == 1) {
            // Successfully got one character
            *bufPtr++ = c;
            if(c == '\n') {
                // Stop reading if we hit newline
                break;
            }
        } else if(rc == 0) {
            // Server closed the connection
            printf("Client: Server side disconnected.\n");
            // If our global socket was valid, we close it
            if(g_sock >= 0) {
                close(g_sock);
                g_sock = -1;
            }
            return -1; // Indicate to caller that an error or disconnect occurred
        } else {
            // rc < 0 => read error
            perror("Client: Read error");
            printf("Client: Server side disconnected.\n");
            if(g_sock >= 0) {
                close(g_sock);
                g_sock = -1;
            }
            return -1;
        }
    }
    // Null-terminate the buffer so we can treat it like a string
    *bufPtr = '\0';
    return bytesRead; 
}

/**
 * ---------------------------------------------------------------------------------------
 * connect_once:
 *   Attempts to open a single TCP connection to the server S1 running at SERVER_PORT on
 *   localhost (127.0.0.1). If this connection is successful, we store the resulting socket
 *   in g_sock for future use. If it fails, we return -1 to indicate that no connection
 *   was established.
 * ---------------------------------------------------------------------------------------
 */
static int connect_once(void) {
    // Attempt to create a TCP socket
    int tempSock = socket(AF_INET, SOCK_STREAM, 0);
    if(tempSock < 0) {
        perror("Client: Socket creation failed");
        return -1;
    }

    // Prepare server address structure
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));  
    server_addr.sin_family      = AF_INET;         
    server_addr.sin_port        = htons(SERVER_PORT);
    server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    // Attempt to connect to S1
    if(connect(tempSock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        printf("Client: Unable to connect to S1\n");
        close(tempSock);
        return -1;
    }

    // If successful, store in our global socket
    g_sock = tempSock;
    printf("Client: Connected to S1.\n");
    return 0;
}

/**
 * ---------------------------------------------------------------------------------------
 * upload_file:
 *   Sends a "uploadf" command to the server, passing the local 'filename' to be read and
 *   the 'dest_path' within S1 (which might be mapped to local or remote storage on S1, S2,
 *   S3, or S4 as necessary). This function reads the local file from disk in chunks and
 *   writes it to the server over the established socket.
 * ---------------------------------------------------------------------------------------
 */
void upload_file(const char *filename, const char *dest_path) {
    // If we are not connected, we cannot proceed
    if(g_sock < 0) {
        printf("Client: Not connected to S1.\n");
        return;
    }

    // Validate the arguments
    if(!filename || !dest_path || strncmp(dest_path, "S1/", 3) != 0) {
        printf("Client: Invalid syntax. Use: uploadf filename S1/path\n");
        return;
    }
    // Check file extension
    char *ext = strrchr(filename, '.');
    if(!ext || (strcmp(ext, ".c") && strcmp(ext, ".pdf") && strcmp(ext, ".txt") && strcmp(ext, ".zip"))) {
        printf("Client: Unsupported file type. Must be .c, .pdf, .txt, or .zip\n");
        return;
    }

    // Open local file for reading
    int localFd = open(filename, O_RDONLY);
    if(localFd < 0) {
        printf("Client: Cannot open %s: %s\n", filename, strerror(errno));
        return;
    }

    // Determine file size
    off_t fileSize = lseek(localFd, 0, SEEK_END);
    lseek(localFd, 0, SEEK_SET);
    if(fileSize <= 0) {
        printf("Client: Invalid file size\n");
        close(localFd);
        return;
    }

    printf("Client: Sending command: uploadf %s %s\n", filename, dest_path);
    printf("Client: File %s size: %ld bytes\n", filename, (long)fileSize);

    // Construct header to notify server about the command and file size
    char header[MAX_INPUT];
    snprintf(header, sizeof(header), "uploadf|%s|%s|%ld\n", filename, dest_path, (long)fileSize);
    write(g_sock, header, strlen(header));
    printf("Client: Sent command: %s", header);

    // Transmit file data in loops
    char buffer_data[CHUNK_SIZE];
    long remaining = fileSize;
    while(remaining > 0) {
        int bytes = read(localFd, buffer_data, (remaining < CHUNK_SIZE) ? remaining : CHUNK_SIZE);
        if(bytes <= 0)
            break;
        write(g_sock, buffer_data, bytes);
        printf("Client: Sent %d bytes of file data\n", bytes);
        remaining -= bytes;
    }
    close(localFd); 
    printf("Client: File data sent\n");

    // Optionally wait for a short response from the server
    {
        fd_set checkSet;
        FD_ZERO(&checkSet);
        FD_SET(g_sock, &checkSet);
        struct timeval waitTime = { .tv_sec = 2, .tv_usec = 0 };
        if(select(g_sock + 1, &checkSet, NULL, NULL, &waitTime) > 0) {
            char respBuf[MAX_INPUT];
            int n = read(g_sock, respBuf, sizeof(respBuf) - 1);
            if(n > 0) {
                respBuf[n] = '\0';
                printf("Client: Server response: %s\n", respBuf);
            } else {
                // If read fails, the server might have disconnected
                printf("Client: Server side disconnected.\n");
                if(g_sock >= 0) {
                    close(g_sock);
                    g_sock = -1;
                }
            }
        } else {
            printf("Client: No response from server (timeout after 2 sec)\n");
        }
    }
}

/**
 * ---------------------------------------------------------------------------------------
 * download_file:
 *   Sends a "downloadf" command to the server, requesting to download 'filename' from S1's
 *   perspective, storing it locally under the same name in the client's working directory.
 *   The server responds with the file size (OK|size). We then read that many bytes from
 *   g_sock and write them to a local file.
 * ---------------------------------------------------------------------------------------
 */
void download_file(const char *filename, const char *dest_path) {
    // Must be connected
    if(g_sock < 0) {
        printf("Client: Not connected to S1.\n");
        return;
    }
    // Validate input arguments
    if(!filename || !dest_path || strncmp(dest_path, "S1/", 3) != 0) {
        printf("Client: Invalid syntax. Use: downloadf filename S1/path\n");
        return;
    }

    printf("Client: Sending command: downloadf %s %s\n", filename, dest_path);

    // Build the command line to send to server
    char header[MAX_INPUT];
    snprintf(header, sizeof(header), "downloadf|%s|%s\n", filename, dest_path);
    write(g_sock, header, strlen(header));
    printf("Client: Sent command: %s", header);

    // Read the server's immediate response header
    char resp_header[MAX_INPUT];
    ssize_t respLen = read_line_client(g_sock, resp_header, sizeof(resp_header));
    if(respLen <= 0) {
        printf("Client: No response from server for downloadf\n");
        return;
    }
    printf("Client: Received header: %s", resp_header);

    // Expect "OK|filesize\n" or an error
    if(strncmp(resp_header, "OK|", 3) != 0) {
        printf("Client: Server returned error: %s\n", resp_header);
        return;
    }
    // Parse out the file size
    char *sizeStr = resp_header + 3;
    char *nl = strchr(sizeStr, '\n');
    if(nl) *nl = '\0';
    long fsize = atol(sizeStr);
    if(fsize <= 0) {
        printf("Client: Invalid file size received\n");
        return;
    }
    printf("Client: File size to download: %ld bytes\n", fsize);

    // Create local file for writing the downloaded data
    int localFd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if(localFd < 0) {
        printf("Client: Cannot open local file %s for writing: %s\n", filename, strerror(errno));
        return;
    }

    long soFarReceived = 0;
    char xferBuffer[CHUNK_SIZE];
    // Read loop from server
    while(soFarReceived < fsize) {
        int bytes = read(g_sock, xferBuffer, (fsize - soFarReceived) < CHUNK_SIZE ? (fsize - soFarReceived) : CHUNK_SIZE);
        if(bytes <= 0) {
            // Possibly the server disconnected
            printf("Client: Server side disconnected or read error.\n");
            if(g_sock >= 0) {
                close(g_sock);
                g_sock = -1;
            }
            break;
        }
        write(localFd, xferBuffer, bytes);
        soFarReceived += bytes;
        printf("Client: Received %d bytes of file data\n", bytes);
    }
    close(localFd);

    if(soFarReceived == fsize)
        printf("Client: Download complete. File %s saved locally (%ld bytes).\n", filename, soFarReceived);
    else
        printf("Client: Download incomplete. Expected %ld bytes but received %ld bytes.\n", fsize, soFarReceived);
}

/**
 * ---------------------------------------------------------------------------------------
 * dispfnames_cmd:
 *   Issues a "dispfnames" command to the server, specifying a directory in S1. The server
 *   will respond with a list of filenames (.c, .pdf, .txt, .zip) in that directory (some
 *   of which may actually live on S2, S3, or S4).
 * ---------------------------------------------------------------------------------------
 */
void dispfnames_cmd(const char *dir) {
    if(g_sock < 0) {
        printf("Client: Not connected to S1.\n");
        return;
    }
    if(!dir || strncmp(dir, "S1/", 3) != 0) {
        printf("Client: Invalid syntax. Use: dispfnames S1/path\n");
        return;
    }
    printf("Client: Sending command: dispfnames %s\n", dir);

    char cmdBuffer[MAX_INPUT];
    snprintf(cmdBuffer, sizeof(cmdBuffer), "dispfnames|%s\n", dir);
    write(g_sock, cmdBuffer, strlen(cmdBuffer));
    printf("Client: Sent command: %s", cmdBuffer);

    char response[MAX_INPUT * 4];
    ssize_t bytesRead = read_line_client(g_sock, response, sizeof(response));
    if(bytesRead > 0) {
        printf("Client: Received file list:\n%s", response);
    } else {
        printf("Client: No response from server (timeout or error).\n");
    }
}

/**
 * ---------------------------------------------------------------------------------------
 * remove_file:
 *   Sends the "removef" command to the server, indicating which file (including its path
 *   under S1) should be removed. The server will remove the file locally if it's .c or
 *   forward the request to S2, S3, or S4 for other file types. 
 * ---------------------------------------------------------------------------------------
 */
void remove_file(const char *filepath) {
    if(g_sock < 0) {
        printf("Client: Not connected to S1.\n");
        return;
    }
    if(!filepath || strncmp(filepath, "S1/", 3) != 0) {
        printf("Client: Invalid syntax. Use: removef S1/path\n");
        return;
    }
    printf("Client: Sending command: removef %s\n", filepath);

    char cmdLine[MAX_INPUT];
    snprintf(cmdLine, sizeof(cmdLine), "removef|%s\n", filepath);
    write(g_sock, cmdLine, strlen(cmdLine));
    printf("Client: Sent command: %s", cmdLine);

    char respLine[MAX_INPUT];
    ssize_t respLen = read_line_client(g_sock, respLine, sizeof(respLine));
    if(respLen > 0) {
        printf("Client: Received response: %s\n", respLine);
    } else {
        printf("Client: No response from server (timeout or error).\n");
    }
}

/**
 * ---------------------------------------------------------------------------------------
 * downltar_cmd:
 *   Requests from the server a tar archive that contains all files of a certain type
 *   (.c, .pdf, or .txt). The user issues "downltar .pdf" or similar. The server either
 *   creates cfiles.tar locally or gets the relevant tar from S2 or S3, then streams it
 *   back to us. We save the tar under a name like "pdf.tar", "text.tar", or "cfiles.tar".
 * ---------------------------------------------------------------------------------------
 */
void downltar_cmd(const char *filetype) {
    if(g_sock < 0) {
        printf("Client: Not connected to S1.\n");
        return;
    }
    // Build a command to S1
    char commandBuf[MAX_INPUT];
    snprintf(commandBuf, sizeof(commandBuf), "downltar|%s\n", filetype);
    write(g_sock, commandBuf, strlen(commandBuf));
    printf("Client: Sent command: %s", commandBuf);

    // Wait for server's header response "OK|filesize\n"
    char headerResp[MAX_INPUT];
    ssize_t hLen = read_line_client(g_sock, headerResp, sizeof(headerResp));
    if(hLen <= 0) {
        printf("Client: No response from server for downltar\n");
        return;
    }
    if(strncmp(headerResp, "OK|", 3) != 0) {
        // Something went wrong on server side
        printf("Client: Server error: %s\n", headerResp);
        return;
    }

    // Extract the size from the "OK|N\n"
    char *sizePtr = headerResp + 3;
    char *newline = strchr(sizePtr, '\n');
    if(newline) {
        *newline = '\0';
    }
    long archiveSize = atol(sizePtr);
    if(archiveSize <= 0) {
        printf("Client: Invalid tar file size received\n");
        return;
    }

    // Decide on a local file name for the tar
    char tarName[64];
    if(strcmp(filetype, ".c") == 0)
         strcpy(tarName, "cfiles.tar");
    else if(strcmp(filetype, ".pdf") == 0)
         strcpy(tarName, "pdf.tar");
    else if(strcmp(filetype, ".txt") == 0)
         strcpy(tarName, "text.tar");
    else
         strcpy(tarName, "tarfile.tar");

    // Open local tar file for writing
    int outFd = open(tarName, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if(outFd < 0) {
        printf("Client: Cannot create file %s: %s\n", tarName, strerror(errno));
        return;
    }

    long totalReceived = 0;
    char xferBuffer[CHUNK_SIZE];
    // Read the tar data from the socket
    while(totalReceived < archiveSize) {
        int blockBytes = read(g_sock, xferBuffer,
                              (archiveSize - totalReceived) < CHUNK_SIZE
                                  ? (archiveSize - totalReceived)
                                  : CHUNK_SIZE);
        if(blockBytes <= 0) {
            // Possibly the server disconnected
            printf("Client: Server side disconnected or read error.\n");
            if(g_sock >= 0) {
                close(g_sock);
                g_sock = -1;
            }
            break;
        }
        write(outFd, xferBuffer, blockBytes);
        totalReceived += blockBytes;
        printf("Client: Received %d bytes of tar file data\n", blockBytes);
    }
    close(outFd);

    // Verify if we received entire file
    if(totalReceived == archiveSize)
        printf("Client: downltar complete. File %s saved locally (%ld bytes).\n", tarName, totalReceived);
    else
        printf("Client: downltar incomplete. Expected %ld bytes but received %ld bytes.\n", archiveSize, totalReceived);
}

/**
 * ---------------------------------------------------------------------------------------
 * main:
 *   The primary entry point for w25clients. We connect once to S1 (via connect_once()),
 *   then repeatedly prompt the user for commands. If "exit" is entered, we close the socket
 *   and terminate. Otherwise, commands are parsed locally and the appropriate function is
 *   called, which sends the command to S1 and handles the result.
 * ---------------------------------------------------------------------------------------
 */
int main() {
    // Announce readiness
    printf("w25clients started. Ready to send commands to S1.\n");

    // Attempt an initial connection to S1
    if(connect_once() < 0) {
        // If we fail to connect, exit right away
        return 1;
    }

    // We'll read commands until user types "exit" or we get an EOF from stdin
    char userInput[MAX_INPUT];
    while(1) {
        // Print the prompt
        printf("w25clients$ ");
        fflush(stdout);

        // Attempt to read user input (command line)
        if(!fgets(userInput, sizeof(userInput), stdin)) {
            // EOF or error reading from stdin => break
            break;
        }
        // Remove trailing newline from user input
        userInput[strcspn(userInput, "\n")] = '\0';

        // If the user typed "exit", we disconnect and break out
        if(strcmp(userInput, "exit") == 0) {
            printf("Client: Disconnecting from S1...\n");
            close(g_sock);
            g_sock = -1;
            printf("Client: Disconnected from S1\n");
            break;
        }

        // Otherwise, parse the command
        char *cmdTok = strtok(userInput, " ");
        if(cmdTok == NULL) {
            printf("Client: Invalid input.\n");
            continue;
        }

        // Identify which command was used
        if(strcmp(cmdTok, "uploadf") == 0) {
            // Expect two more tokens: filename and S1/path
            char *filePart = strtok(NULL, " ");
            char *destPart = strtok(NULL, " ");
            if(filePart == NULL || destPart == NULL) {
                printf("Client: Invalid input for uploadf. Expected format: uploadf filename S1/path\n");
                continue;
            }
            upload_file(filePart, destPart);

        } else if(strcmp(cmdTok, "downloadf") == 0) {
            // Expect two tokens: filename and S1/path
            char *filePart = strtok(NULL, " ");
            char *destPart = strtok(NULL, " ");
            if(filePart == NULL || destPart == NULL) {
                printf("Client: Invalid input for downloadf. Expected format: downloadf filename S1/path\n");
                continue;
            }
            download_file(filePart, destPart);

        } else if(strcmp(cmdTok, "dispfnames") == 0) {
            // Expect one token: S1/path
            char *dirVal = strtok(NULL, " ");
            if(dirVal == NULL) {
                printf("Client: Invalid input for dispfnames. Expected format: dispfnames S1/path\n");
                continue;
            }
            dispfnames_cmd(dirVal);

        } else if(strcmp(cmdTok, "removef") == 0) {
            // Expect one token: S1/filepath
            char *filePathVal = strtok(NULL, " ");
            if(filePathVal == NULL) {
                printf("Client: Invalid input for removef. Expected format: removef S1/path\n");
                continue;
            }
            remove_file(filePathVal);

        } else if(strcmp(cmdTok, "downltar") == 0) {
            // Expect one token: .c, .pdf, or .txt
            char *fileTypeVal = strtok(NULL, " ");
            if(fileTypeVal == NULL) {
                printf("Client: Invalid input for downltar. Expected format: downltar filetype\n");
                continue;
            }
            downltar_cmd(fileTypeVal);

        } else {
            // Unrecognized command
            printf("Client: Unknown command. Use 'uploadf', 'downloadf', 'dispfnames', 'removef', 'downltar', or 'exit'.\n");
        }
    }

    // If we get here without "exit", we still close the socket if it's open
    if(g_sock >= 0) {
        close(g_sock);
        g_sock = -1;
        printf("Client: Disconnected from S1\n");
    }
    return 0;
}
