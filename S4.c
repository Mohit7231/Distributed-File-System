/***************************************************************************************
 *      S4 Server Application
 *      (COMP 8567 Distributed File System - Summer 2025)
 * This file defines the S4 server, which is responsible for storing and retrieving .zip
 * files at path ~/S4. S4 can:
 *   - Store (.zip) files
 *   - Send those files back when requested
 *   - Remove them if asked
  ***************************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>
#include <sys/types.h>

/*------------------------------------------------------------------------
  S4_PORT: The designated port for S4 to listen on incoming connections.
  BUFFER_SIZE: Number of bytes for temporary data buffer usage.
------------------------------------------------------------------------*/
#define S4_PORT 8304
#define BUFFER_SIZE 1024

/*------------------------------------------------------------------------
  s4_storage: This character array holds the absolute path to S4's root
  directory, which defaults to ~/S4 in the user’s home folder.
------------------------------------------------------------------------*/
char s4_storage[512];

/*------------------------------------------------------------------------
  Prototypes for core helper and request-handling functions.
------------------------------------------------------------------------*/
ssize_t writen(int fd, const void *vptr, size_t n);
ssize_t read_line(int fd, void *buf, size_t maxlen);
ssize_t read_line_remote(int fd, void *buf, size_t maxlen);
void create_directories(const char *full_path);
void process_store_request(int conn_fd, char *path, long file_size);
void process_download_request(int conn_fd, char *filename, char *relative);
void process_dispfnames_S4(int client_sock);
void process_removef(int client_sock);
void prcclient(int client_sock);

/*-------------------------------------------------------------------------
  create_directories:
    - Extracts the directory path component from a provided 'full_path'.
    - Uses 'mkdir -p' to recursively create any missing folders.
    - Ensures S4 can store data in nested subdirectories.
-------------------------------------------------------------------------*/
void create_directories(const char *full_path) {
    /* Copy the path so we can modify it without altering the original. */
    char path_dupe[512];
    strncpy(path_dupe, full_path, sizeof(path_dupe));

    /* Look for the last slash (/) in the path to isolate directory vs. file. */
    char *slash_ptr = strrchr(path_dupe, '/');
    if (slash_ptr) {
        /* Temporarily end the string at that slash to get the directory part. */
        *slash_ptr = '\0';

        /* Prepare a system command that creates that directory tree. */
        char sys_cmd[1024];
        snprintf(sys_cmd, sizeof(sys_cmd), "mkdir -p %s", path_dupe);

        /* Attempt directory creation. */
        if (system(sys_cmd) == -1) {
            perror("S4: mkdir command failed");
        } else {
            printf("S4: Created directory path: %s\n", path_dupe);
        }
    }
}

/*-------------------------------------------------------------------------
  writen:
    - Writes exactly 'n' bytes from 'vptr' to file descriptor 'fd'.
    - Re-attempts the write if interrupted by signals (EINTR).
    - Returns the total number of bytes actually written, or -1 on error.
-------------------------------------------------------------------------*/
ssize_t writen(int fd, const void *vptr, size_t n) {
    size_t remaining = n;
    ssize_t num_written;
    const char *data_ptr = vptr;

    while (remaining > 0) {
        /* Perform a write to the file descriptor. */
        if ((num_written = write(fd, data_ptr, remaining)) <= 0) {
            /* If the write was interrupted, retry. Otherwise, abort on error. */
            if (num_written < 0 && errno == EINTR) {
                num_written = 0;
            } else {
                return -1;
            }
        }
        remaining -= num_written;
        data_ptr  += num_written;
    }
    return n;
}

/*-------------------------------------------------------------------------
  read_line:
    - Reads data from 'fd' byte-by-byte.
    - Stops if it encounters a newline or reaches 'maxlen'.
    - Stores bytes into 'buf' until the exit condition, then appends '\0'.
    - Returns the count of bytes read, or 0 if EOF, or -1 if a read error.
-------------------------------------------------------------------------*/
ssize_t read_line(int fd, void *buf, size_t maxlen) {
    ssize_t idx, rc;
    char c;
    char *buffer_ptr = buf;

    for (idx = 1; idx < maxlen; idx++) {
        rc = read(fd, &c, 1);
        if (rc == 1) {
            *buffer_ptr++ = c;
            if (c == '\n') {
                break;
            }
        } else if (rc == 0) {
            /* If EOF is reached on the first read attempt, return 0. */
            if (idx == 1) {
                return 0;
            } else {
                break;
            }
        } else {
            /* For errors other than EOF, return -1. */
            return -1;
        }
    }
    /* Null-terminate so the buffer can be treated as a C-string. */
    *buffer_ptr = 0;
    return idx;
}

/*-------------------------------------------------------------------------
  read_line_remote: This is a convenience wrapper for read_line when reading
  from a potentially remote source. Just calls read_line internally.
-------------------------------------------------------------------------*/
ssize_t read_line_remote(int fd, void *buf, size_t maxlen) {
    return read_line(fd, buf, maxlen);
}

/*-------------------------------------------------------------------------
  process_store_request:
    - Handles the command "store|<path>|<size>".
    - Receives 'file_size' bytes of data from the client and writes them
      to a newly created file under s4_storage/<path>.
-------------------------------------------------------------------------*/
void process_store_request(int conn_fd, char *path, long file_size) {
    printf("S4: Processing store request for '%s', size: %ld bytes\n", path, file_size);

    /* Build an absolute path where the file will be stored: s4_storage/path. */
    char target_file[512];
    snprintf(target_file, sizeof(target_file), "%s/%s", s4_storage, path);

    /* Ensure that the directories leading to 'target_file' exist. */
    create_directories(target_file);

    /* Open the file for writing (create if doesn't exist, truncate if it does). */
    int local_fd = open(target_file, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (local_fd < 0) {
        fprintf(stderr, "S4: Unable to open %s: %s\n", target_file, strerror(errno));
        writen(conn_fd, "ERR: Cannot open file\n", strlen("ERR: Cannot open file\n"));
        return;
    }

    char buffer_chunk[BUFFER_SIZE];
    long accumulated = 0;

    /* Keep reading data from the client until the entire 'file_size' is received. */
    while (accumulated < file_size) {
        /* Attempt to read up to what remains or up to BUFFER_SIZE. */
        int bytes_read = read(conn_fd, buffer_chunk,
                              (file_size - accumulated) < BUFFER_SIZE
                                  ? (file_size - accumulated)
                                  : BUFFER_SIZE);
        /* If read is zero or negative => incomplete or error. */
        if (bytes_read <= 0) {
            close(local_fd);
            fprintf(stderr, "S4: Incomplete file transfer for %s\n", target_file);
            writen(conn_fd, "ERR: Incomplete transfer\n", strlen("ERR: Incomplete transfer\n"));
            return;
        }
        /* Write exactly 'bytes_read' bytes to the local file. */
        if (writen(local_fd, buffer_chunk, bytes_read) != bytes_read) {
            close(local_fd);
            fprintf(stderr, "S4: Write error on %s\n", target_file);
            writen(conn_fd, "ERR: Write failed\n", strlen("ERR: Write failed\n"));
            return;
        }
        /* Increase the total by however many bytes we successfully wrote. */
        accumulated += bytes_read;
    }

    /* Close the file after writing all data successfully. */
    close(local_fd);
    printf("S4: Successfully stored file => %s\n", target_file);

    /* Send an acknowledgment back to the client. */
    writen(conn_fd, "OK: File stored\n", strlen("OK: File stored\n"));
}

/*-------------------------------------------------------------------------
  process_download_request:
    - Handles the command "downloadf|filename|relativePath".
    - The requested file is read from s4_storage/<relative>/<filename> and
      streamed back to the client.
-------------------------------------------------------------------------*/
void process_download_request(int conn_fd, char *filename, char *relative) {
    /* Construct the absolute path in S4's storage. */
    char file_path[512];
    snprintf(file_path, sizeof(file_path), "%s/%s/%s", s4_storage, relative, filename);
    printf("S4: Processing downloadf for file => %s\n", file_path);

    /* Attempt to open the file for reading. */
    int read_fd = open(file_path, O_RDONLY);
    if (read_fd < 0) {
        writen(conn_fd, "ERR: File not found\n", strlen("ERR: File not found\n"));
        return;
    }

    /* Determine file size by seeking to the end, then reset to the beginning. */
    off_t file_size = lseek(read_fd, 0, SEEK_END);
    lseek(read_fd, 0, SEEK_SET);

    /* If file size is zero or negative => empty or error. */
    if (file_size <= 0) {
        writen(conn_fd, "ERR: Empty file\n", strlen("ERR: Empty file\n"));
        close(read_fd);
        return;
    }

    /* Inform the client of the file size => "OK|<size>\n" */
    char size_header[128];
    snprintf(size_header, sizeof(size_header), "OK|%ld\n", (long)file_size);
    writen(conn_fd, size_header, strlen(size_header));

    /* Stream the file data in chunks to the client. */
    char transmit_buf[BUFFER_SIZE];
    long total_sent = 0;
    while (total_sent < file_size) {
        int portion = read(read_fd, transmit_buf, sizeof(transmit_buf));
        if (portion <= 0) {
            break;
        }
        writen(conn_fd, transmit_buf, portion);
        total_sent += portion;
    }
    printf("S4: Completed download of %s (transmitted %ld bytes)\n", file_path, total_sent);
    close(read_fd);
}

/*-------------------------------------------------------------------------
  process_dispfnames_S4:
    - Handles "dispfnames|S4/whatever/path"
    - Locates all ".zip" files in the subdirectory => s4_storage/<relative>.
    - Sorts them by name and returns the newline-separated list to client.
    - If no .zip files are found => returns "No files found\n".
-------------------------------------------------------------------------*/
void process_dispfnames_S4(int client_sock) {
    /* Extract the line after the command "dispfnames|". */
    char *dir_arg = strtok(NULL, "\n");
    if (!dir_arg) {
        writen(client_sock, "ERR: No directory provided\n", strlen("ERR: No directory provided\n"));
        close(client_sock);
        return;
    }
    /* Must start with "S4/", else invalid. */
    if (strncmp(dir_arg, "S4/", 3) != 0) {
        writen(client_sock, "ERR: Directory must start with S4/\n",
               strlen("ERR: Directory must start with S4/\n"));
        close(client_sock);
        return;
    }

    /* The portion after "S4/" is the actual sub-path in S4's storage. */
    char *relative_sub = dir_arg + 3;

    char local_dir[512];
    snprintf(local_dir, sizeof(local_dir), "%s/%s", s4_storage, relative_sub);

    /* Attempt to open the directory in S4. */
    DIR *dp = opendir(local_dir);
    if (!dp) {
        writen(client_sock, "ERR: Cannot open directory\n", strlen("ERR: Cannot open directory\n"));
        close(client_sock);
        return;
    }

    /* Gather names of all .zip files. */
    int count = 0, cap = 10;
    char **zip_list = malloc(cap * sizeof(char *));
    struct dirent *entry_ptr;
    while ((entry_ptr = readdir(dp)) != NULL) {
        if (entry_ptr->d_type == DT_REG) {
            char *dot_ext = strrchr(entry_ptr->d_name, '.');
            if (dot_ext && strcmp(dot_ext, ".zip") == 0) {
                if (count >= cap) {
                    cap *= 2;
                    zip_list = realloc(zip_list, cap * sizeof(char *));
                }
                zip_list[count++] = strdup(entry_ptr->d_name);
            }
        }
    }
    closedir(dp);

    /* Sort the .zip file list lexicographically. */
    for (int i = 0; i < count - 1; i++) {
        for (int j = i + 1; j < count; j++) {
            if (strcmp(zip_list[i], zip_list[j]) > 0) {
                char *temp = zip_list[i];
                zip_list[i] = zip_list[j];
                zip_list[j] = temp;
            }
        }
    }

    /* Construct a newline-separated result. */
    size_t total_len = 0;
    for (int i = 0; i < count; i++) {
        total_len += strlen(zip_list[i]) + 1; 
    }
    char *result_str = malloc(total_len + 1);
    result_str[0] = '\0';
    for (int i = 0; i < count; i++) {
        strcat(result_str, zip_list[i]);
        strcat(result_str, "\n");
        free(zip_list[i]);
    }
    free(zip_list);

    /* If no .zip files found => respond with "No files found\n". */
    if (strlen(result_str) == 0) {
        strcpy(result_str, "No files found\n");
    }
    writen(client_sock, result_str, strlen(result_str));
    free(result_str);
    close(client_sock);
}

/*-------------------------------------------------------------------------
  process_removef:
    - Expects "removef|filename|relativePath".
    - Removes the file from s4_storage/relativePath/filename if it exists.
    - Returns "OK: File removed" on success or error otherwise.
-------------------------------------------------------------------------*/
void process_removef(int client) {
    char *filename = strtok(NULL, "|");
    char *relative = strtok(NULL, "\n");
    if (!filename || !relative) {
        writen(client, "ERR: Invalid removef format\n", strlen("ERR: Invalid removef format\n"));
        return;
    }

    /* Build the absolute file path in S4. */
    char absolute_file[512];
    snprintf(absolute_file, sizeof(absolute_file), "%s/%s/%s", s4_storage, relative, filename);
    printf("S4: Removing file => %s\n", absolute_file);

    /* Attempt to unlink (delete) the file. */
    if (remove(absolute_file) == 0) {
        writen(client, "OK: File removed\n", strlen("OK: File removed\n"));
    } else {
        char err_msg[128];
        snprintf(err_msg, sizeof(err_msg), "ERR: Removal failed (%s)\n", strerror(errno));
        writen(client, err_msg, strlen(err_msg));
    }
}

/*-------------------------------------------------------------------------
  prcclient:
    - Reads a line from the client to determine the command.
    - Branches based on cmd = {store, downloadf, dispfnames, removef}.
    - Delegates to appropriate function or sends an error if unknown.
    - Closes the connection once the command has finished or if the
      request was invalid.
-------------------------------------------------------------------------*/
void prcclient(int client) {
    char header[BUFFER_SIZE];
    /* Attempt to read a command line from the client. */
    ssize_t hdr_len = read_line(client, header, sizeof(header));
    if (hdr_len <= 0) {
        printf("S4: Client disconnected during header read.\n");
        close(client);
        return;
    }
    printf("S4: Received command: %s", header);

    /* Parse out the command (e.g., "store", "downloadf", etc.). */
    char *cmd = strtok(header, "|");
    if (!cmd) {
        writen(client, "ERR: Empty command\n", strlen("ERR: Empty command\n"));
        close(client);
        return;
    }

    /* store|<path>|<size> => process_store_request */
    if (strcmp(cmd, "store") == 0) {
        char *path_arg = strtok(NULL, "|");
        char *size_str = strtok(NULL, "\n");
        if (!path_arg || !size_str) {
            writen(client, "ERR: Invalid store format\n", strlen("ERR: Invalid store format\n"));
        } else {
            long fsize = atol(size_str);
            if (fsize <= 0) {
                writen(client, "ERR: Invalid file size\n", strlen("ERR: Invalid file size\n"));
            } else {
                process_store_request(client, path_arg, fsize);
            }
        }
        close(client);
    }

    /* downloadf|<filename>|<relative> => process_download_request */
    else if (strcmp(cmd, "downloadf") == 0) {
        char *fname = strtok(NULL, "|");
        char *relpath = strtok(NULL, "\n");
        if (!fname || !relpath) {
            writen(client, "ERR: Invalid download format\n", strlen("ERR: Invalid download format\n"));
        } else {
            process_download_request(client, fname, relpath);
        }
        close(client);
    }

    /* dispfnames => process_dispfnames_S4 */
    else if (strcmp(cmd, "dispfnames") == 0) {
        process_dispfnames_S4(client);
    }

    /* removef => process_removef */
    else if (strcmp(cmd, "removef") == 0) {
        process_removef(client);
        close(client);
    }

    /* If unknown, respond with an error and close. */
    else {
        writen(client, "ERR: Unknown command\n", strlen("ERR: Unknown command\n"));
        close(client);
    }
}

/*-------------------------------------------------------------------------
  main:
    - Initializes S4 storage under ~/S4.
    - Creates the directory if it doesn’t exist.
    - Sets up a listening socket on S4_PORT.
    - Accepts incoming connections and spawns prcclient handler.
-------------------------------------------------------------------------*/
int main() {
    /* Construct the path to S4’s root directory => ~/S4. */
    snprintf(s4_storage, sizeof(s4_storage), "%s/S4", getenv("HOME"));

    /* Create that directory if missing. */
    char mk_dir_command[512];
    snprintf(mk_dir_command, sizeof(mk_dir_command), "mkdir -p %s", s4_storage);
    system(mk_dir_command);
    printf("S4: Initialized storage => %s\n", s4_storage);

    /* Create a TCP socket for listening to incoming client requests. */
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0) {
        perror("S4: Socket creation failed");
        exit(1);
    }

    /* Configure address for binding => port + any IP. */
    struct sockaddr_in bind_addr;
    bind_addr.sin_family      = AF_INET;
    bind_addr.sin_port        = htons(S4_PORT);
    bind_addr.sin_addr.s_addr = INADDR_ANY;

    /* Reuse address to avoid "already in use" bind errors. */
    int reuse_opt = 1;
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &reuse_opt, sizeof(reuse_opt));

    /* Bind the socket to the specified address and port. */
    if (bind(server_sock, (struct sockaddr *) &bind_addr, sizeof(bind_addr)) < 0) {
        perror("S4: Bind failed");
        close(server_sock);
        exit(1);
    }

    /* Listen on the socket for incoming connections (up to backlog=8). */
    if (listen(server_sock, 8) < 0) {
        perror("S4: Listen failed");
        close(server_sock);
        exit(1);
    }
    printf("S4: Listening on port %d, storage: %s\n", S4_PORT, s4_storage);

    /* Main server loop: accept connections and service them. */
    while (1) {
        /* Wait for an incoming client connection. */
        int client_fd = accept(server_sock, NULL, NULL);
        if (client_fd < 0) {
            perror("S4: Accept failed");
            continue;
        }
        printf("S4: New connection accepted.\n");

        /* Process commands from this client. (No fork used => single-threaded approach.) */
        prcclient(client_fd);
    }

    /* Close the main socket if we ever exit the loop (theoretically not). */
    close(server_sock);
    return 0;
}
