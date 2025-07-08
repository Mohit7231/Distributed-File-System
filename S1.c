/* 
   COMP-8567 Project(COMP 8567 Distributed File System - Summer 2025)
   By: Mohit Gupta
   Student ID: 110186495

   S1 Server: Manages a distributed file system, storing .c files locally and forwarding
              .pdf, .txt, and .zip files to S2, S3, and S4 servers respectively.
   Supports commands for uploading, downloading, listing, deleting files, 
   and creating tar archives.
*/

#include <stdio.h>      // Standard I/O operations (printf, etc.)
#include <stdlib.h>     // For memory allocation, system calls, etc.
#include <string.h>     // For string manipulation (strcpy, strtok, etc.)
#include <unistd.h>     // For close(), fork(), read(), write(), etc.
#include <sys/socket.h> // For socket-related system calls
#include <netinet/in.h> // For sockaddr_in and network-related structs
#include <arpa/inet.h>  // For inet_ntoa() and related
#include <sys/stat.h>   // For mkdir, file stats
#include <fcntl.h>      // For open() flags (O_RDONLY, etc.)
#include <errno.h>      // For error codes (errno) and strerror()
#include <dirent.h>     // For directory reading (opendir, readdir)
#include <sys/types.h>  // For data types like off_t, size_t, etc.

#define PRIMARY_PORT 8301      // Port where this S1 server listens
#define PDF_PORT     8302      // Port used by S2 server (.pdf)
#define TEXT_PORT    8303      // Port used by S3 server (.txt)
#define ZIP_PORT     8304      // Port used by S4 server (.zip)
#define MAX_DATA     8192      // General buffer size for data operations

/*
 * Global character array for referencing the root directory of S1's storage.
 * Typically resolved to ~/S1 (by appending "/S1" to HOME).
 */
char storage_root[256];

/* FUNCTION DECLARATIONS (Prototypes) */
ssize_t send_bytes(int socket_fd, const void *data, size_t length);
ssize_t read_until_newline(int socket_fd, void *buffer, size_t max_length);
ssize_t read_remote_line(int socket_fd, void *buffer, size_t max_length);
void setup_directory(const char *dir_path);
void relay_to_server(int client_socket, char *file_path, long data_size, char *data_buffer, int target_port);
char *retrieve_file_list(const char *sub_path, int target_port);
char *alphabetize_files(const char *file_collection);
char *request_file_removal(const char *sub_path, const char *file_name, int target_port);
void handle_file_list_request(int client_socket);
void handle_file_deletion(int client_socket);
void handle_tar_download(int client_socket);
void process_client_session(int client_socket);

/*------------------------------------------------------------------------------
 * setup_directory: Creates directory structures recursively if they don't exist.
 * - dir_path: The directory path that must be ensured on the filesystem.
 * - We use 'mkdir -p' through system() to handle nested directory creation.
 *----------------------------------------------------------------------------*/
void setup_directory(const char *dir_path) {
    char command_buffer[512];
    snprintf(command_buffer, sizeof(command_buffer), "mkdir -p %s", dir_path);
    if (system(command_buffer) != 0) {
        // Log error if system call fails
        fprintf(stderr, "S1: Failed to create directory %s\n", dir_path);
    } else {
        // Confirmation log if directory creation succeeds
        printf("S1: Created directory %s\n", dir_path);
    }
}

/*------------------------------------------------------------------------------
 * send_bytes: Sends exactly 'length' bytes to the given socket descriptor.
 * - Retries sending if interrupted by signals (EINTR).
 * - Returns the number of bytes written or -1 on error.
 *----------------------------------------------------------------------------*/
ssize_t send_bytes(int socket_fd, const void *data, size_t length) {
    size_t remaining_bytes = length;
    const char *data_pointer = (const char *)data;

    // Keep looping until all bytes have been transmitted or an unrecoverable error
    while (remaining_bytes > 0) {
        ssize_t bytes_written = write(socket_fd, data_pointer, remaining_bytes);
        if (bytes_written <= 0) {
            if (bytes_written < 0 && errno == EINTR) {
                // If interrupted by a signal, just retry
                bytes_written = 0;
            } else {
                // Return error indicator if something else went wrong
                return -1;
            }
        }
        // Subtract how many were successfully written from what's left
        remaining_bytes -= bytes_written;
        // Advance pointer by that many bytes
        data_pointer   += bytes_written;
    }
    // If we exit the loop, we've sent everything successfully
    return (ssize_t)length;
}

/*------------------------------------------------------------------------------
 * read_until_newline: Reads from a socket byte-by-byte until we hit '\n' or max.
 * - Returns total bytes read (including newline) or 0 on EOF, -1 on error.
 * - The buffer is null-terminated to form a string at the end of reading.
 *----------------------------------------------------------------------------*/
ssize_t read_until_newline(int socket_fd, void *buffer, size_t max_length) {
    char *buf_ptr = (char *)buffer;
    ssize_t total_chars = 0;

    // Loop up to max_length - 1, so we leave room for null terminator
    for (total_chars = 1; total_chars < (ssize_t)max_length; total_chars++) {
        char current_char;
        ssize_t status = read(socket_fd, &current_char, 1);
        if (status == 1) {
            *buf_ptr++ = current_char;
            if (current_char == '\n') {
                // If newline encountered, stop
                break;
            }
        } else if (status == 0) {
            // If we hit EOF and no data was read so far, return 0
            // Otherwise, we just break out with what we got
            if (total_chars == 1) {
                return 0;
            }
            break;
        } else {
            // If read returned an error, we return -1
            return -1;
        }
    }
    // Null-terminate after reading
    *buf_ptr = '\0';
    return total_chars;
}

/*------------------------------------------------------------------------------
 * read_remote_line: A convenience wrapper for remote line reading.
 * - It just calls 'read_until_newline'.
 * - Could be extended with additional remote-check logic if desired.
 *----------------------------------------------------------------------------*/
ssize_t read_remote_line(int socket_fd, void *buffer, size_t max_length) {
    return read_until_newline(socket_fd, buffer, max_length);
}

/*------------------------------------------------------------------------------
 * relay_to_server: Forwards data for non-.c files to S2 (pdf), S3 (txt), or S4 (zip).
 * - Takes the data from client, connects to the correct remote server, sends it,
 *   then forwards the remote server's response back to the client.
 *----------------------------------------------------------------------------*/
void relay_to_server(int client_socket, char *file_path, long data_size, char *data_buffer, int target_port) {
    // Create a socket to talk to the remote server (S2, S3, or S4)
    int remote_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (remote_socket < 0) {
        send_bytes(client_socket, "ERR: Socket creation failed\n", 28);
        return;
    }

    // Fill out remote server address
    struct sockaddr_in remote_address;
    memset(&remote_address, 0, sizeof(remote_address));
    remote_address.sin_family      = AF_INET;
    remote_address.sin_port        = htons(target_port);
    remote_address.sin_addr.s_addr = inet_addr("127.0.0.1");

    // Attempt to connect to the remote server
    if (connect(remote_socket, (struct sockaddr *)&remote_address, sizeof(remote_address)) < 0) {
        send_bytes(client_socket, "ERR: Server unavailable\n", 24);
        close(remote_socket);
        return;
    }

    // Log that we're forwarding this file to a different server
    printf("S1: Forwarding to port %d, path: %s, size: %ld\n", target_port, file_path, data_size);

    // Send the 'store|<filepath>|<size>' command
    char request_buffer[512];
    snprintf(request_buffer, sizeof(request_buffer), "store|%s|%ld\n", file_path, data_size);

    if (send_bytes(remote_socket, request_buffer, strlen(request_buffer)) < 0) {
        send_bytes(client_socket, "ERR: Failed to send header\n", 28);
        close(remote_socket);
        return;
    }
    // Next, send the actual file data
    if (send_bytes(remote_socket, data_buffer, data_size) < 0) {
        send_bytes(client_socket, "ERR: Failed to send file data\n", 30);
        close(remote_socket);
        return;
    }

    // Read response from the remote server (S2, S3, or S4)
    char response_buffer[MAX_DATA];
    int resp_len = read(remote_socket, response_buffer, sizeof(response_buffer) - 1);
    if (resp_len > 0) {
        response_buffer[resp_len] = '\0';
        printf("S1: Response from port %d: %s", target_port, response_buffer);
        send_bytes(client_socket, response_buffer, resp_len);
    } else {
        send_bytes(client_socket, "ERR: No response from server\n", 29);
    }

    // Clean up
    close(remote_socket);
}

/*------------------------------------------------------------------------------
 * retrieve_file_list: Connects to S2, S3, or S4 to fetch a list of files for
 *                     dispfnames command.
 * - Returns the raw list of filenames or an empty string in case of error or no data.
 *----------------------------------------------------------------------------*/
char *retrieve_file_list(const char *sub_path, int target_port) {
    // Create socket for S2, S3, or S4
    int connection_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (connection_socket < 0) {
        // Return empty string if socket creation fails
        return strdup("");
    }

    // Populate connection details
    struct sockaddr_in server_info;
    memset(&server_info, 0, sizeof(server_info));
    server_info.sin_family      = AF_INET;
    server_info.sin_port        = htons(target_port);
    server_info.sin_addr.s_addr = inet_addr("127.0.0.1");

    // Attempt to connect
    if (connect(connection_socket, (struct sockaddr *)&server_info, sizeof(server_info)) < 0) {
        close(connection_socket);
        return strdup("");
    }

    // Determine the prefix for sending the command (S2/, S3/, or S4/), if any
    char server_prefix[8];
    if      (target_port == PDF_PORT) strcpy(server_prefix, "S2/");
    else if (target_port == TEXT_PORT)strcpy(server_prefix, "S3/");
    else if (target_port == ZIP_PORT) strcpy(server_prefix, "S4/");
    else                              strcpy(server_prefix, "");

    // Build the dispfnames command
    char command_string[512];
    snprintf(command_string, sizeof(command_string), "dispfnames|%s%s\n", server_prefix, sub_path);
    printf("S1: Forwarded dispfnames to port %d: %s", target_port, command_string);

    // Send the command
    send_bytes(connection_socket, command_string, strlen(command_string));

    // Read the response
    char response_data[4096];
    ssize_t received = read(connection_socket, response_data, sizeof(response_data) - 1);
    close(connection_socket);

    if (received <= 0) {
        // If nothing received or error, return empty
        return strdup("");
    }

    // Null-terminate and check for an error or "No ..." response
    response_data[received] = '\0';
    if (strncmp(response_data, "ERR:", 4) == 0 || strncmp(response_data, "No ", 3) == 0) {
        return strdup("");
    }
    return strdup(response_data);
}

/*------------------------------------------------------------------------------
 * alphabetize_files: Takes a string containing filenames separated by newlines,
 *                    performs a basic alphabetical sort, and returns the result.
 *----------------------------------------------------------------------------*/
char *alphabetize_files(const char *file_collection) {
    // Duplicate the input so we can modify it
    char *copy = strdup(file_collection);
    if (!copy) {
        return strdup("");
    }

    // Count how many lines (filenames) exist
    int line_count = 0;
    for (char *p = copy; *p; p++) {
        if (*p == '\n') {
            line_count++;
        }
    }

    // If there's no newline, there's either 0 or 1 file in the list
    if (line_count == 0) {
        free(copy);
        // Just return the original string as no sorting is needed
        return strdup(file_collection);
    }

    // We'll store each line pointer in an array
    char **lines_array = malloc(line_count * sizeof(char *));
    int idx = 0;
    // Tokenize by newline
    char *token = strtok(copy, "\n");
    while (token) {
        lines_array[idx++] = strdup(token);
        token = strtok(NULL, "\n");
    }

    // Basic bubble sort on the lines
    for (int i = 0; i < line_count - 1; i++) {
        for (int j = i + 1; j < line_count; j++) {
            if (strcmp(lines_array[i], lines_array[j]) > 0) {
                char *temp = lines_array[i];
                lines_array[i] = lines_array[j];
                lines_array[j] = temp;
            }
        }
    }

    // Calculate total space for the result
    size_t total_size = 0;
    for (int i = 0; i < line_count; i++) {
        total_size += strlen(lines_array[i]) + 1; // +1 for newline
    }

    // Allocate a new string to hold the sorted list
    char *sorted_result = malloc(total_size + 1);
    sorted_result[0]    = '\0';

    // Build the final newline-separated list
    for (int i = 0; i < line_count; i++) {
        strcat(sorted_result, lines_array[i]);
        strcat(sorted_result, "\n");
        free(lines_array[i]);
    }

    // Clean up
    free(lines_array);
    free(copy);

    return sorted_result;
}

/*------------------------------------------------------------------------------
 * request_file_removal: S1 uses this to tell S2/S3/S4 to remove a remote file.
 * - If it's .pdf => send to S2
 * - If it's .txt => send to S3
 * - If it's .zip => send to S4
 *----------------------------------------------------------------------------*/
char *request_file_removal(const char *sub_path, const char *file_name, int target_port) {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        return strdup("ERR: Socket creation failed\n");
    }

    struct sockaddr_in srv_addr;
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family      = AF_INET;
    srv_addr.sin_port        = htons(target_port);
    srv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(sock_fd, (struct sockaddr *)&srv_addr, sizeof(srv_addr)) < 0) {
        close(sock_fd);
        return strdup("ERR: Unable to connect to remote server\n");
    }

    char command_buf[512];
    snprintf(command_buf, sizeof(command_buf), "removef|%s|%s\n", file_name, sub_path);
    printf("S1: Forwarded removef to port %d: %s", target_port, command_buf);

    send_bytes(sock_fd, command_buf, strlen(command_buf));

    char response_buf[1024];
    ssize_t n = read(sock_fd, response_buf, sizeof(response_buf) - 1);
    close(sock_fd);

    if (n <= 0) {
        return strdup("ERR: No response from remote server\n");
    }

    response_buf[n] = '\0';
    return strdup(response_buf);
}

/*------------------------------------------------------------------------------
 * handle_file_list_request: For the "dispfnames <S1/path>" command.
 * 1) Collect local .c files from the specified directory in S1.
 * 2) Retrieve .pdf from S2, .txt from S3, and .zip from S4.
 * 3) Merge them (in the order: .c, then .pdf, .txt, .zip), sorted within each group.
 * 4) Return the result to the client.
 *----------------------------------------------------------------------------*/
void handle_file_list_request(int client_socket) {
    // Grab the directory argument after "dispfnames|..."
    char *directory_arg = strtok(NULL, "\n");
    if (!directory_arg) {
        send_bytes(client_socket, "ERR: No directory provided\n", 27);
        return;
    }

    // Must start with S1/
    if (strncmp(directory_arg, "S1/", 3) != 0) {
        send_bytes(client_socket, "ERR: Directory must start with S1/\n", 35);
        return;
    }

    // Remove the "S1/" part
    char *relative_path = directory_arg + 3;
    // In case the client typed something like "S1/folderuploadf" by mistake, we can truncate
    char *p = strstr(relative_path, "uploadf");
    if (p) {
        *p = '\0';
    }

    // Construct the actual local path on disk
    char local_path[512];
    snprintf(local_path, sizeof(local_path), "%s/%s", storage_root, relative_path);

    // We'll gather local .c files
    int count_c = 0;
    int cap_c   = 10;
    char **list_c = (char **)malloc(cap_c * sizeof(char *));

    // Attempt to open the directory
    DIR *d = opendir(local_path);
    if (d) {
        struct dirent *ent;
        while ((ent = readdir(d)) != NULL) {
            // Skip . and ..
            if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) {
                continue;
            }
            if (ent->d_type == DT_REG) {
                // Check the extension for .c
                char *dot = strrchr(ent->d_name, '.');
                if (dot && !strcmp(dot, ".c")) {
                    // If we've filled the array, expand it
                    if (count_c >= cap_c) {
                        cap_c *= 2;
                        list_c = realloc(list_c, cap_c * sizeof(char *));
                    }
                    list_c[count_c++] = strdup(ent->d_name);
                }
            }
        }
        closedir(d);
    }

    // Sort the .c filenames in ascending order
    for (int i = 0; i < count_c - 1; i++) {
        for (int j = i + 1; j < count_c; j++) {
            if (strcmp(list_c[i], list_c[j]) > 0) {
                char *temp = list_c[i];
                list_c[i]  = list_c[j];
                list_c[j]  = temp;
            }
        }
    }

    // Build a space-separated string of .c filenames
    size_t local_len = 0;
    for (int i = 0; i < count_c; i++) {
        local_len += strlen(list_c[i]) + 1; // space or newline
    }
    char *local_c = (char *)malloc(local_len + 2);
    local_c[0] = '\0';
    for (int i = 0; i < count_c; i++) {
        strcat(local_c, list_c[i]);
        strcat(local_c, " ");
        free(list_c[i]);
    }
    free(list_c);

    // Now retrieve remote file lists from S2, S3, and S4
    char *remote_pdf = retrieve_file_list(relative_path, PDF_PORT);
    char *remote_txt = retrieve_file_list(relative_path, TEXT_PORT);
    char *remote_zip = retrieve_file_list(relative_path, ZIP_PORT);

    // Sort each group's list
    char *sorted_pdf = alphabetize_files(remote_pdf);
    char *sorted_txt = alphabetize_files(remote_txt);
    char *sorted_zip = alphabetize_files(remote_zip);

    free(remote_pdf);
    free(remote_txt);
    free(remote_zip);

    // Convert newlines into spaces in those lists
    for (char *c = sorted_pdf; *c; c++) {
        if (*c == '\n') *c = ' ';
    }
    for (char *c = sorted_txt; *c; c++) {
        if (*c == '\n') *c = ' ';
    }
    for (char *c = sorted_zip; *c; c++) {
        if (*c == '\n') *c = ' ';
    }

    // Combine into one final list
    size_t total_combined = strlen(local_c) + strlen(sorted_pdf) + 1
                          + strlen(sorted_txt) + 1
                          + strlen(sorted_zip) + 1;

    char *combined = (char *)malloc(total_combined + 10);
    combined[0] = '\0';

    // Add .c files first
    strcat(combined, local_c);
    // Then .pdf
    strcat(combined, sorted_pdf);
    strcat(combined, " ");
    // Then .txt
    strcat(combined, sorted_txt);
    strcat(combined, " ");
    // Then .zip
    strcat(combined, sorted_zip);

    // Trim leading spaces
    char *start = combined;
    while (*start == ' ' || *start == '\t') start++;
    // Trim trailing spaces
    char *end = start + strlen(start);
    while (end > start && (end[-1] == ' ' || end[-1] == '\t')) {
        end--;
    }
    *end = '\0';

    // If empty, return "No files found"
    if (strlen(start) == 0) {
        strcpy(combined, "No files found");
    } else {
        memmove(combined, start, strlen(start) + 1);
    }
    strcat(combined, "\n");

    // Send the result back to the client
    send_bytes(client_socket, combined, strlen(combined));

    // Clean up
    free(local_c);
    free(sorted_pdf);
    free(sorted_txt);
    free(sorted_zip);
    free(combined);
}

/*------------------------------------------------------------------------------
 * handle_file_deletion: For the "removef <S1/path>" command.
 * - If it's .c, remove locally in S1.
 * - Otherwise, forward removal to S2, S3, or S4.
 *----------------------------------------------------------------------------*/
void handle_file_deletion(int client_socket) {
    // Pull out the file path from the command
    char *file_arg = strtok(NULL, "\n");
    if (!file_arg) {
        send_bytes(client_socket, "ERR: No file specified\n", 23);
        return;
    }
    // Must start with S1/
    if (strncmp(file_arg, "S1/", 3) != 0) {
        send_bytes(client_socket, "ERR: File path must start with S1/\n", 35);
        return;
    }
    // Remove "S1/"
    char *rel_path = file_arg + 3;

    char filename[256];
    char dir_sub[512];
    // Separate the directory from the filename
    char *slash_pos = strrchr(rel_path, '/');
    if (slash_pos) {
        strcpy(filename, slash_pos + 1);
        size_t len_dir = slash_pos - rel_path;
        strncpy(dir_sub, rel_path, len_dir);
        dir_sub[len_dir] = '\0';
    } else {
        strcpy(filename, rel_path);
        strcpy(dir_sub, "");
    }

    // Identify file extension
    char *ext = strrchr(filename, '.');
    if (!ext) {
        send_bytes(client_socket, "ERR: Unknown file type\n", 23);
        return;
    }

    // If it's a .c file, remove locally
    if (!strcmp(ext, ".c")) {
        char path_full[512];
        snprintf(path_full, sizeof(path_full), "%s/%s", storage_root, rel_path);
        if (remove(path_full) == 0) {
            send_bytes(client_socket, "OK: File removed\n", 17);
        } else {
            char error_msg[128];
            snprintf(error_msg, sizeof(error_msg), "ERR: Removal failed (%s)\n", strerror(errno));
            send_bytes(client_socket, error_msg, strlen(error_msg));
        }
    } else {
        // For other file types, forward the removal
        int port_target = 0;
        if      (!strcmp(ext, ".pdf")) port_target = PDF_PORT;
        else if (!strcmp(ext, ".txt")) port_target = TEXT_PORT;
        else if (!strcmp(ext, ".zip")) port_target = ZIP_PORT;
        else {
            send_bytes(client_socket, "ERR: Unsupported file type for removal\n", 39);
            return;
        }
        // Send the removal request to S2, S3, or S4
        char *resp = request_file_removal(dir_sub, filename, port_target);
        send_bytes(client_socket, resp, strlen(resp));
        free(resp);
    }
}

/*------------------------------------------------------------------------------
 * handle_tar_download: For the "downltar <filetype>" command:
 * - If .c => create tar from S1's .c files
 * - If .pdf => request from S2
 * - If .txt => request from S3
 * - .zip is not supported for tar in this project
 *----------------------------------------------------------------------------*/
void handle_tar_download(int client_socket) {
    // Extract the filetype argument
    char *file_type = strtok(NULL, "\n");
    if (!file_type) {
        send_bytes(client_socket, "ERR: No filetype provided\n", 26);
        return;
    }

    // If .c => we create a local tar of all .c in ~/S1
    if (!strcmp(file_type, ".c")) {
        char tar_file_path[512];
        snprintf(tar_file_path, sizeof(tar_file_path), "%s/code_archive.tar", storage_root);

        // Construct a shell command to find all .c files in S1's tree and tar them
        char cmd[1024];
        snprintf(cmd, sizeof(cmd),
                 "cd %s && find . -type f -name '*.c' | tar -cf code_archive.tar -T -",
                 storage_root);

        if (system(cmd) != 0) {
            send_bytes(client_socket, "ERR: Failed to create tar archive\n", 34);
            return;
        }

        // Open the tar file
        int fd_tar = open(tar_file_path, O_RDONLY);
        if (fd_tar < 0) {
            send_bytes(client_socket, "ERR: Cannot open tar archive\n", 29);
            return;
        }

        // Get file size
        off_t tar_size = lseek(fd_tar, 0, SEEK_END);
        lseek(fd_tar, 0, SEEK_SET);

        // Send header: "OK|<size>"
        char header_resp[128];
        snprintf(header_resp, sizeof(header_resp), "OK|%ld\n", (long)tar_size);
        send_bytes(client_socket, header_resp, strlen(header_resp));

        // Send the tar content
        char buf[MAX_DATA];
        long bytes_total = 0;
        int read_len;
        while ((read_len = read(fd_tar, buf, MAX_DATA)) > 0) {
            send_bytes(client_socket, buf, read_len);
            bytes_total += read_len;
        }
        close(fd_tar);

        // Remove the temporary tar file
        remove(tar_file_path);

    // If .pdf => ask S2 to create and send us pdf.tar, then forward to client
    } else if (!strcmp(file_type, ".pdf") || !strcmp(file_type, ".txt")) {
        int chosen_port = (!strcmp(file_type, ".pdf")) ? PDF_PORT : TEXT_PORT;

        // Create socket to connect to S2 or S3
        int rem_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (rem_sock < 0) {
            send_bytes(client_socket, "ERR: Socket creation failed\n", 28);
            return;
        }
        struct sockaddr_in rem_addr;
        memset(&rem_addr, 0, sizeof(rem_addr));
        rem_addr.sin_family      = AF_INET;
        rem_addr.sin_port        = htons(chosen_port);
        rem_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

        if (connect(rem_sock, (struct sockaddr *)&rem_addr, sizeof(rem_addr)) < 0) {
            send_bytes(client_socket, "ERR: Remote server unavailable\n", 31);
            close(rem_sock);
            return;
        }

        // Send "downltar|.pdf" or "downltar|.txt"
        char remote_cmd[256];
        snprintf(remote_cmd, sizeof(remote_cmd), "downltar|%s\n", file_type);
        send_bytes(rem_sock, remote_cmd, strlen(remote_cmd));

        // Read the header response: "OK|<size>"
        char resp_hdr[128];
        ssize_t hdr_len = read_remote_line(rem_sock, resp_hdr, sizeof(resp_hdr));
        if (hdr_len <= 0) {
            send_bytes(client_socket, "ERR: No response from remote server\n", 36);
            close(rem_sock);
            return;
        }

        // Check if it's "OK|" at the front
        if (strncmp(resp_hdr, "OK|", 3) != 0) {
            send_bytes(client_socket, resp_hdr, strlen(resp_hdr));
            close(rem_sock);
            return;
        }
        // Forward that header to our client
        send_bytes(client_socket, resp_hdr, strlen(resp_hdr));

        // Extract the file size
        char *size_str = resp_hdr + 3;
        char *nl = strchr(size_str, '\n');
        if (nl) *nl = '\0';

        long remote_size = atol(size_str);
        if (remote_size <= 0) {
            send_bytes(client_socket, "ERR: Invalid file size from remote server\n", 42);
            close(rem_sock);
            return;
        }

        // Read and forward the tar data
        char data_buf[MAX_DATA];
        long total_forwarded = 0;
        int chunk;
        while (total_forwarded < remote_size && (chunk = read(rem_sock, data_buf, sizeof(data_buf))) > 0) {
            send_bytes(client_socket, data_buf, chunk);
            total_forwarded += chunk;
        }
        close(rem_sock);

    } else {
        // .zip or other is not supported
        send_bytes(client_socket, "ERR: Unsupported file type for downltar\n", 40);
    }
}

/*------------------------------------------------------------------------------
 * process_client_session: This is the main loop for each connected client.
 *  - Repeatedly reads commands, parses them, and calls the appropriate handler.
 *  - Exits when the client disconnects or an error occurs reading from the socket.
 *----------------------------------------------------------------------------*/
void process_client_session(int client_socket) {
    // Log which client is talking to us
    struct sockaddr_in client_info;
    socklen_t addr_len = sizeof(client_info);
    getpeername(client_socket, (struct sockaddr *)&client_info, &addr_len);
    printf("S1: New client session from %s:%d\n",
           inet_ntoa(client_info.sin_addr), ntohs(client_info.sin_port));

    char command_buffer[MAX_DATA];

    // Keep handling commands until the client disconnects
    while (1) {
        ssize_t read_len = read_until_newline(client_socket, command_buffer, sizeof(command_buffer));
        if (read_len <= 0) {
            // If 0 or negative, the client likely disconnected
            printf("S1: Client disconnected.\n");
            close(client_socket);
            return;
        }
        printf("S1: Received command: %s", command_buffer);

        // The command is the first token up to '|'
        char *cmd = strtok(command_buffer, "|");
        if (!cmd) {
            send_bytes(client_socket, "ERR: Empty command\n", 19);
            continue;
        }

        // Upload request
        if (!strcmp(cmd, "uploadf")) {
            char *file_name  = strtok(NULL, "|");
            char *dest_path  = strtok(NULL, "|");
            char *size_str   = strtok(NULL, "\n");

            if (!file_name || !dest_path || !size_str || strncmp(dest_path, "S1/", 3) != 0) {
                send_bytes(client_socket, "ERR: Invalid upload request\n", 28);
                continue;
            }
            long file_size = atol(size_str);
            if (file_size <= 0) {
                send_bytes(client_socket, "ERR: Invalid file size\n", 23);
                continue;
            }
            printf("S1: Processing upload %s to %s, size: %ld\n", file_name, dest_path, file_size);

            // Allocate a buffer to receive the file from the client
            char *data_buf = (char *)malloc(file_size);
            if (!data_buf) {
                send_bytes(client_socket, "ERR: Memory allocation failed\n", 30);
                continue;
            }

            // Read the file data in a loop
            long collected = 0;
            while (collected < file_size) {
                int n = read(client_socket, data_buf + collected, file_size - collected);
                if (n <= 0) {
                    free(data_buf);
                    send_bytes(client_socket, "ERR: File receive failed\n", 25);
                    break;
                }
                collected += n;
            }
            if (collected < file_size) {
                // If we didn't get the whole file, skip
                free(data_buf);
                continue;
            }

            // Construct the full local path in S1
            char *rel = dest_path + 3;  // skip "S1/"
            char full_path[512];
            snprintf(full_path, sizeof(full_path), "%s/%s/%s", storage_root, rel, file_name);
            printf("S1: Full path: %s\n", full_path);

            // Identify the file extension
            char *ext = strrchr(file_name, '.');
            if (!ext) {
                free(data_buf);
                send_bytes(client_socket, "ERR: Unknown file type\n", 23);
                continue;
            }

            // If it's a .c file, store it locally on S1
            if (!strcmp(ext, ".c")) {
                char dir_path[512];
                strncpy(dir_path, full_path, sizeof(dir_path));
                char *slash = strrchr(dir_path, '/');
                if (slash) {
                    *slash = '\0';
                    // Make sure the directory structure exists
                    setup_directory(dir_path);
                }
                printf("S1: Storing .c file at %s\n", full_path);

                int fd = open(full_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
                if (fd < 0) {
                    fprintf(stderr, "S1: Failed to open %s: %s\n", full_path, strerror(errno));
                    free(data_buf);
                    send_bytes(client_socket, "ERR: Storage failed\n", 20);
                    continue;
                }
                if (send_bytes(fd, data_buf, file_size) != (ssize_t)file_size) {
                    fprintf(stderr, "S1: Failed to write %s: %s\n", full_path, strerror(errno));
                    free(data_buf);
                    close(fd);
                    send_bytes(client_socket, "ERR: Storage failed\n", 20);
                    continue;
                }
                close(fd);
                send_bytes(client_socket, "OK: File stored\n", 16);

            // Otherwise, forward .pdf to S2, .txt to S3, .zip to S4
            } else {
                char xfer_path[512];
                snprintf(xfer_path, sizeof(xfer_path), "%s/%s", rel, file_name);

                int out_port = 0;
                if      (!strcmp(ext, ".pdf")) out_port = PDF_PORT;
                else if (!strcmp(ext, ".txt")) out_port = TEXT_PORT;
                else if (!strcmp(ext, ".zip")) out_port = ZIP_PORT;

                if (out_port) {
                    relay_to_server(client_socket, xfer_path, file_size, data_buf, out_port);
                } else {
                    send_bytes(client_socket, "ERR: Unsupported file type\n", 27);
                }
            }
            free(data_buf);

        // Download request
        } else if (!strcmp(cmd, "downloadf")) {
            char *f_name = strtok(NULL, "|");
            char *dest_p = strtok(NULL, "\n");
            if (!f_name || !dest_p || strncmp(dest_p, "S1/", 3) != 0) {
                send_bytes(client_socket, "ERR: Invalid download request\n", 30);
                continue;
            }
            char *relp = dest_p + 3;
            char fpath[512];
            snprintf(fpath, sizeof(fpath), "%s/%s/%s", storage_root, relp, f_name);
            printf("S1: Processing download for %s\n", fpath);

            char *extn = strrchr(f_name, '.');
            if (extn && !strcmp(extn, ".c")) {
                // Local .c file
                int fd2 = open(fpath, O_RDONLY);
                if (fd2 < 0) {
                    send_bytes(client_socket, "ERR: File not found\n", 20);
                    continue;
                }
                off_t sz = lseek(fd2, 0, SEEK_END);
                lseek(fd2, 0, SEEK_SET);
                if (sz <= 0) {
                    send_bytes(client_socket, "ERR: Empty file\n", 16);
                    close(fd2);
                    continue;
                }
                char resp[128];
                snprintf(resp, sizeof(resp), "OK|%ld\n", (long)sz);
                send_bytes(client_socket, resp, strlen(resp));

                char dbuf[MAX_DATA];
                long snt = 0;
                while (snt < sz) {
                    int n = read(fd2, dbuf, sizeof(dbuf));
                    if (n <= 0) break;
                    send_bytes(client_socket, dbuf, n);
                    snt += n;
                }
                printf("S1: Completed download of %s (%ld bytes)\n", fpath, snt);
                close(fd2);

            } else {
                // .pdf, .txt, or .zip => retrieve from S2, S3, or S4
                int prt = 0;
                if      (extn && !strcmp(extn, ".pdf")) prt = PDF_PORT;
                else if (extn && !strcmp(extn, ".txt")) prt = TEXT_PORT;
                else if (extn && !strcmp(extn, ".zip")) prt = ZIP_PORT;
                else {
                    send_bytes(client_socket, "ERR: Unsupported file type for download\n", 40);
                    continue;
                }

                int rsock = socket(AF_INET, SOCK_STREAM, 0);
                if (rsock < 0) {
                    send_bytes(client_socket, "ERR: Socket creation failed\n", 28);
                    continue;
                }
                struct sockaddr_in raddr;
                memset(&raddr, 0, sizeof(raddr));
                raddr.sin_family      = AF_INET;
                raddr.sin_port        = htons(prt);
                raddr.sin_addr.s_addr = inet_addr("127.0.0.1");

                if (connect(rsock, (struct sockaddr *)&raddr, sizeof(raddr)) < 0) {
                    send_bytes(client_socket, "ERR: Unable to connect to remote server\n", 39);
                    close(rsock);
                    continue;
                }
                char remote_head[512];
                snprintf(remote_head, sizeof(remote_head), "downloadf|%s|%s\n", f_name, relp);
                send_bytes(rsock, remote_head, strlen(remote_head));
                printf("S1: Sent remote download: %s", remote_head);

                char remotebuf[MAX_DATA];
                ssize_t rresp = read_remote_line(rsock, remotebuf, sizeof(remotebuf));
                if (rresp <= 0) {
                    send_bytes(client_socket, "ERR: No response from remote server\n", 36);
                    close(rsock);
                    continue;
                }
                printf("S1: Received remote response: %s", remotebuf);

                if (strncmp(remotebuf, "OK|", 3) != 0) {
                    send_bytes(client_socket, remotebuf, strlen(remotebuf));
                    close(rsock);
                    continue;
                }
                // Parse the file size from "OK|<size>"
                char *szstr  = remotebuf + 3;
                char *newln  = strchr(szstr, '\n');
                if (newln) *newln = '\0';
                long remotesz = atol(szstr);
                if (remotesz <= 0) {
                    send_bytes(client_socket, "ERR: Invalid file size from remote server\n", 42);
                    close(rsock);
                    continue;
                }
                // Send "OK|size" to our client
                char local_resp[128];
                snprintf(local_resp, sizeof(local_resp), "OK|%ld\n", remotesz);
                send_bytes(client_socket, local_resp, strlen(local_resp));

                // Read the data from remote, forward to our client
                char tbuf[MAX_DATA];
                long forwarded = 0;
                while (forwarded < remotesz) {
                    int rdb = read(rsock, tbuf, sizeof(tbuf));
                    if (rdb <= 0) break;
                    send_bytes(client_socket, tbuf, rdb);
                    forwarded += rdb;
                }
                printf("S1: Completed proxy download for %s (%ld bytes)\n", f_name, forwarded);
                close(rsock);
            }

        // Listing request (dispfnames)
        } else if (!strcmp(cmd, "dispfnames")) {
            handle_file_list_request(client_socket);

        // Removing a file (removef)
        } else if (!strcmp(cmd, "removef")) {
            handle_file_deletion(client_socket);

        // Tar download (downltar)
        } else if (!strcmp(cmd, "downltar")) {
            handle_tar_download(client_socket);

        } else {
            // Unknown command
            send_bytes(client_socket, "ERR: Unknown command\n", 21);
        }
    }
}

/*------------------------------------------------------------------------------
 * main: Sets up S1 server, binds to PRIMARY_PORT, and loops to accept clients.
 *       For each client, we fork and run process_client_session in the child.
 *----------------------------------------------------------------------------*/
int main() {
    // Form the S1 storage path => HOME/S1
    snprintf(storage_root, sizeof(storage_root), "%s/S1", getenv("HOME"));
    // Make sure ~/S1 exists
    setup_directory(storage_root);

    // Create a TCP socket
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        perror("S1: Socket creation failed");
        exit(1);
    }

    // Fill out server address
    struct sockaddr_in server_address;
    server_address.sin_family      = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;          // Listen on all interfaces
    server_address.sin_port        = htons(PRIMARY_PORT); // Use the specified port

    // Allow reuse of port
    int socket_option = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &socket_option, sizeof(socket_option));

    // Bind to the chosen port
    if (bind(server_socket, (struct sockaddr *)&server_address, sizeof(server_address)) < 0) {
        perror("S1: Bind failed");
        close(server_socket);
        exit(1);
    }

    // Start listening with a backlog of 10
    if (listen(server_socket, 10) < 0) {
        perror("S1: Listen failed");
        close(server_socket);
        exit(1);
    }

    printf("S1 listening on port %d, home: %s\n", PRIMARY_PORT, storage_root);

    // Enter the infinite accept loop
    while (1) {
        struct sockaddr_in client_address;
        socklen_t client_length = sizeof(client_address);
        // Wait for a new client
        int client_sock = accept(server_socket, (struct sockaddr *)&client_address, &client_length);
        if (client_sock < 0) {
            perror("S1: Accept failed");
            continue;
        }
        printf("S1: New client from %s:%d\n",
               inet_ntoa(client_address.sin_addr),
               ntohs(client_address.sin_port));

        // Fork a child to handle that client
        if (fork() == 0) {
            // Child process: close the main server socket so child can handle client
            close(server_socket);
            process_client_session(client_sock);
            exit(0);
        }
        // Parent: close the accepted socket and keep waiting for more clients
        close(client_sock);
    }

    // If we break out of while(1), close the server socket (unlikely here).
    close(server_socket);
    return 0;
}
