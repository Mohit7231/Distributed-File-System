/***************************************************************
 *      S2 Server Application
 *      (COMP 8567 Distributed File System - Summer 2025)
 *  S2 SERVER (Handles .pdf files and interacts with S1) 
 *  This file sets up a TCP-based server listening on port S2_PORT.
 *  Clients (specifically, S1) connect to it for storing, retrieving,
 *  listing, and removing .pdf files. Additionally, it can bundle
 *  all .pdf files in a tar archive and send them back upon request.
 ***************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>        // Directory operations (e.g., opendir, readdir)
#include <sys/types.h>     // Data types for system calls like socket, etc.

#define S2_PORT 8302       // Port for the S2 server
#define BUFFER_CAPACITY 2048  // Size used for data transfers

/***************************************************************
 * Global variable that holds the path to the S2 root directory
 * This is the base location (~/S2) where .pdf files are stored.
 ***************************************************************/
char s2_root[256];

/* Forward declarations of S2-specific functions. */
ssize_t read_line(int fd, void *buffer, size_t maxlen);
void ensure_directory_exists(const char *path);
void process_store_request(int conn_fd, char *path, long file_size);
void process_download_request(int conn_fd, char *filename, char *relative);
void process_dispfnames_pdf(int conn_fd);
void process_removef(int conn_fd);
void process_downltar_pdf(int conn_fd);  // Handling the request to tar up .pdf files

/***************************************************************************
 * ensure_directory_exists():
 *   A helper function that creates the directories in a recursive manner
 *   if they don't already exist, ensuring the file path can be used.
 ***************************************************************************/
void ensure_directory_exists(const char *path) {
    // Prepare a system command to create nested directories
    char createCmd[256];
    snprintf(createCmd, sizeof(createCmd), "mkdir -p %s", path);

    // Run the command and print the outcome for logging
    system(createCmd);
    printf("S2: Ensured directory %s exists\n", path);
}

/***************************************************************************
 * read_line():
 *   Reads from a socket one character at a time, storing each byte into
 *   'buffer' until a newline is encountered, up to 'maxlen' characters.
 *   This is primarily used to read an entire line (commands, headers, etc.).
 ***************************************************************************/
ssize_t read_line(int fd, void *buffer, size_t maxlen) {
    ssize_t n, rc;
    char c;
    char *outPtr = buffer;

    // Read at most maxlen-1 bytes (leaving space for the null terminator)
    for(n = 1; n < maxlen; n++) {
        rc = read(fd, &c, 1);
        if(rc == 1) {
            // Store the read byte
            *outPtr++ = c;
            // Break if newline is found
            if(c == '\n') {
                break;
            }
        } else if(rc == 0) {
            // EOF reached
            if(n == 1) {
                return 0; // No data read
            } else {
                break;    // Some data was read
            }
        } else {
            // If negative => read error, so return -1
            return -1;
        }
    }
    *outPtr = '\0';  // Null-terminate the string
    return n;
}

/***************************************************************************
 * process_store_request():
 *   This function is triggered upon receiving a "store" command from S1.
 *   S2 will then read exactly 'file_size' bytes from the socket and write
 *   them to disk in the S2 directory structure. This is how .pdf files
 *   get placed into the S2 server filesystem from S1's perspective.
 ***************************************************************************/
void process_store_request(int conn_fd, char *path, long file_size) {
    // Logging to see which file path is requested and how big the file is
    printf("S2: Processing store request for %s, size: %ld\n", path, file_size);

    // Construct the absolute path under S2's root directory
    char full_filepath[512];
    snprintf(full_filepath, sizeof(full_filepath), "%s/%s", s2_root, path);

    // Find directory portion, ensure it exists by calling ensure_directory_exists
    char *slash = strrchr(full_filepath, '/');
    if(slash) {
        *slash = '\0';
        ensure_directory_exists(full_filepath);
        *slash = '/';
    }

    // Attempt to open the file for writing
    FILE *file_obj = fopen(full_filepath, "wb");
    if(!file_obj) {
        fprintf(stderr, "S2: Failed to open %s: %s\n", full_filepath, strerror(errno));
        write(conn_fd, "ERR: Cannot open file\n", strlen("ERR: Cannot open file\n"));
        return;
    }

    // Buffer for reading data from the socket
    char data_buf[BUFFER_CAPACITY];
    long total_received = 0;

    // Keep reading until the entire file is received
    while(total_received < file_size) {
        int chunk = read(conn_fd, data_buf,
                         (file_size - total_received) < BUFFER_CAPACITY
                             ? (file_size - total_received)
                             : BUFFER_CAPACITY);
        if(chunk <= 0) {
            // If we fail to read the expected amount, mark incomplete
            fclose(file_obj);
            fprintf(stderr, "S2: Incomplete transfer for %s\n", full_filepath);
            write(conn_fd, "ERR: Incomplete file transfer\n",
                  strlen("ERR: Incomplete file transfer\n"));
            return;
        }
        // Write that chunk to the file
        fwrite(data_buf, 1, chunk, file_obj);
        total_received += chunk;
    }
    fclose(file_obj);

    // Inform that storing succeeded
    printf("S2: Successfully stored %s\n", full_filepath);
    write(conn_fd, "OK: File stored\n", strlen("OK: File stored\n"));
}

/***************************************************************************
 * process_download_request():
 *   Responds to a "downloadf" command by reading the named .pdf file from
 *   disk and streaming it back to the socket for the requesting client (S1).
 ***************************************************************************/
void process_download_request(int conn_fd, char *filename, char *relative) {
    // Build full path relative to S2
    char combined_path[512];
    snprintf(combined_path, sizeof(combined_path),
             "%s/%s/%s", s2_root, relative, filename);
    printf("S2: Processing downloadf for file: %s\n", combined_path);

    // Attempt to open that file in read-only mode
    int file_fd = open(combined_path, O_RDONLY);
    if(file_fd < 0) {
        write(conn_fd, "ERR: File not found\n", strlen("ERR: File not found\n"));
        return;
    }
    // Determine file size
    off_t file_size = lseek(file_fd, 0, SEEK_END);
    lseek(file_fd, 0, SEEK_SET);
    if(file_size <= 0) {
        write(conn_fd, "ERR: Empty file\n", strlen("ERR: Empty file\n"));
        close(file_fd);
        return;
    }
    // Send a header "OK|filesize\n" so the client knows how many bytes to read
    char header[128];
    snprintf(header, sizeof(header), "OK|%ld\n", (long)file_size);
    write(conn_fd, header, strlen(header));

    // Send the contents in a loop
    char local_buffer[BUFFER_CAPACITY];
    long total_bytes_sent = 0;
    while(total_bytes_sent < file_size) {
        int chunk = read(file_fd, local_buffer, sizeof(local_buffer));
        if(chunk <= 0) {
            // end of file or error
            break;
        }
        write(conn_fd, local_buffer, chunk);
        total_bytes_sent += chunk;
    }
    printf("S2: Completed download of %s (%ld bytes sent)\n",
           combined_path, total_bytes_sent);
    close(file_fd);
}

/***************************************************************************
 * process_dispfnames_pdf():
 *   Handles the "dispfnames" command specifically for .pdf files in S2.
 *   It expects "dispfnames|S2/relative_path". We open the directory,
 *   gather all .pdf file names, sort them, and send them back.
 ***************************************************************************/
void process_dispfnames_pdf(int conn_fd) {
    // Extract the directory argument
    char *dir_arg = strtok(NULL, "\n");
    if(!dir_arg) {
        write(conn_fd, "ERR: No directory provided\n", strlen("ERR: No directory provided\n"));
        close(conn_fd);
        return;
    }
    // Expect the argument to start with "S2/"
    if(strncmp(dir_arg, "S2/", 3) != 0) {
        write(conn_fd, "ERR: Directory must start with S2/\n",
              strlen("ERR: Directory must start with S2/\n"));
        close(conn_fd);
        return;
    }
    // Remove the "S2/" prefix
    char *relative_part = dir_arg + 3;

    // Construct the actual path in S2's file system
    char local_path[512];
    snprintf(local_path, sizeof(local_path), "%s/%s", s2_root, relative_part);

    // Attempt to open the directory
    DIR *dir_obj = opendir(local_path);
    if(!dir_obj) {
        write(conn_fd, "ERR: Cannot open directory\n",
              strlen("ERR: Cannot open directory\n"));
        close(conn_fd);
        return;
    }

    // We'll read everything in the directory and store just .pdf
    int capacity = 10;
    int count = 0;
    char **pdf_list = malloc(capacity * sizeof(char *));
    struct dirent *d_entry;

    while((d_entry = readdir(dir_obj)) != NULL) {
        if(d_entry->d_type == DT_REG) {
            // Look for ".pdf" extension
            char *dot = strrchr(d_entry->d_name, '.');
            if(dot && strcmp(dot, ".pdf") == 0) {
                // If we reached the capacity, grow the array
                if(count >= capacity) {
                    capacity *= 2;
                    pdf_list = realloc(pdf_list, capacity * sizeof(char *));
                }
                pdf_list[count++] = strdup(d_entry->d_name);
            }
        }
    }
    closedir(dir_obj);

    // Sort the .pdf files alphabetically
    for(int i = 0; i < count - 1; i++) {
        for(int j = i + 1; j < count; j++) {
            if(strcmp(pdf_list[i], pdf_list[j]) > 0) {
                char *swap_temp = pdf_list[i];
                pdf_list[i] = pdf_list[j];
                pdf_list[j] = swap_temp;
            }
        }
    }

    // Build a newline-separated list
    size_t total_len = 0;
    for(int i = 0; i < count; i++) {
        total_len += strlen(pdf_list[i]) + 1; 
    }
    char *send_back = malloc(total_len + 1);
    send_back[0] = '\0';
    for(int i = 0; i < count; i++) {
        strcat(send_back, pdf_list[i]);
        strcat(send_back, "\n");
    }

    // If none found, we note it
    if(strlen(send_back) == 0)
        strcpy(send_back, "No .pdf files found\n");

    // Send the string to S1
    write(conn_fd, send_back, strlen(send_back));

    // Free memory
    for(int i = 0; i < count; i++) {
        free(pdf_list[i]);
    }
    free(pdf_list);
    free(send_back);

    // Close the connection
    close(conn_fd);
}

/***************************************************************************
 * process_removef():
 *   Responds to "removef|<filename>|<relative>". This deletes the specified
 *   .pdf file from S2's filesystem if it exists in the correct location.
 ***************************************************************************/
void process_removef(int conn_fd) {
    // Extract the filename and relative path
    char *filename = strtok(NULL, "|");
    char *relative = strtok(NULL, "\n");
    if(!filename || !relative) {
         write(conn_fd, "ERR: Invalid removef format\n",
               strlen("ERR: Invalid removef format\n"));
         return;
    }
    // Build the absolute path
    char absolute_path[512];
    snprintf(absolute_path, sizeof(absolute_path),
             "%s/%s/%s", s2_root, relative, filename);
    printf("S2: Removing file: %s\n", absolute_path);

    // Attempt to remove the file
    if(remove(absolute_path) == 0) {
         write(conn_fd, "OK: File removed\n", strlen("OK: File removed\n"));
    } else {
         char msg_err[128];
         snprintf(msg_err, sizeof(msg_err), "ERR: Removal failed (%s)\n", strerror(errno));
         write(conn_fd, msg_err, strlen(msg_err));
    }
}

/***************************************************************************
 * process_downltar_pdf():
 *   Packages all .pdf files under ~/S2 into "pdf.tar" and streams it back.
 ***************************************************************************/
void process_downltar_pdf(int conn_fd) {
    // Prepare the path for the temporary tar file 
    char tar_path[512];
    snprintf(tar_path, sizeof(tar_path), "%s/pdf.tar", s2_root);

    // Remove any existing pdf.tar
    remove(tar_path);

    // Use a system command to find all .pdf files and tar them
    char system_cmd[1024];
    snprintf(system_cmd, sizeof(system_cmd),
             "cd %s && find . -type f -name '*.pdf' | tar -cf pdf.tar -T -",
             s2_root);
    int cmd_status = system(system_cmd);
    if(cmd_status != 0) {
        write(conn_fd, "ERR: Failed to create tar archive\n",
              strlen("ERR: Failed to create tar archive\n"));
        return;
    }

    // Open the newly created tar
    int fd = open(tar_path, O_RDONLY);
    if(fd < 0) {
        write(conn_fd, "ERR: Cannot open tar archive\n",
              strlen("ERR: Cannot open tar archive\n"));
        return;
    }
    // Determine file size
    off_t size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    if(size <= 0) {
        write(conn_fd, "ERR: Empty archive\n", strlen("ERR: Empty archive\n"));
        close(fd);
        return;
    }
    // Notify the client: "OK|<size>\n"
    char response[128];
    snprintf(response, sizeof(response), "OK|%ld\n", (long)size);
    write(conn_fd, response, strlen(response));

    // Transfer the archive in chunks
    char buf[BUFFER_CAPACITY];
    long total_sent = 0;
    while(total_sent < size) {
        int chunk = read(fd, buf, sizeof(buf));
        if(chunk <= 0) break;
        write(conn_fd, buf, chunk);
        total_sent += chunk;
    }
    close(fd);

    // Remove that tar file
    remove(tar_path);

    printf("S2: Sent pdf.tar (%ld bytes)\n", total_sent);
}

/***************************************************************************
 * main():
 *   Entry point for the S2 server:
 *   1) Setup the root directory (~/S2).
 *   2) Create a socket, bind to S2_PORT, and listen.
 *   3) On each client connection, parse commands and call relevant functions.
 ***************************************************************************/
int main() {
    // Compose the path ~/S2 using $HOME
    snprintf(s2_root, sizeof(s2_root), "%s/S2", getenv("HOME"));
    ensure_directory_exists(s2_root);

    // Create a socket for the server
    int srv_socket = socket(AF_INET, SOCK_STREAM, 0);
    if(srv_socket < 0) {
         perror("S2: Socket creation failed");
         exit(1);
    }

    // Initialize server address struct
    struct sockaddr_in srv_addr;
    srv_addr.sin_family      = AF_INET;
    srv_addr.sin_addr.s_addr = INADDR_ANY;
    srv_addr.sin_port        = htons(S2_PORT);

    // Allow reusing the port if it’s in TIME_WAIT
    int opt = 1;
    setsockopt(srv_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind the socket to the specified port
    if(bind(srv_socket, (struct sockaddr*)&srv_addr, sizeof(srv_addr)) < 0) {
         perror("S2: Bind failed");
         close(srv_socket);
         exit(1);
    }
    // Start listening with a backlog of 10
    if(listen(srv_socket, 10) < 0) {
         perror("S2: Listen failed");
         close(srv_socket);
         exit(1);
    }

    printf("S2 active on port %d, root: %s\n", S2_PORT, s2_root);

    // Accept connections indefinitely
    while(1) {
         int client_fd = accept(srv_socket, NULL, NULL);
         if(client_fd < 0) {
              perror("S2: Accept failed");
              continue;
         }
         printf("S2: New connection accepted\n");

         // Read the initial command line from S1
         char header[BUFFER_CAPACITY];
         ssize_t header_len = read_line(client_fd, header, sizeof(header));
         if(header_len <= 0) {
              printf("S2: Client disconnected or header read error\n");
              close(client_fd);
              continue;
         }

         // Print the command that was received
         printf("S2: Received command: %s\n", header);

         // Break the line by '|', parse
         char *cmd = strtok(header, "|");
         if(cmd && strcmp(cmd, "store") == 0) {
              // Format: store|path|size
              char *path     = strtok(NULL, "|");
              char *size_str = strtok(NULL, "\n");
              if(!path || !size_str) {
                   write(client_fd, "ERR: Invalid store format\n",
                         strlen("ERR: Invalid store format\n"));
              } else {
                   long file_size = atol(size_str);
                   if(file_size <= 0) {
                        write(client_fd, "ERR: Invalid file size\n",
                              strlen("ERR: Invalid file size\n"));
                   } else {
                        process_store_request(client_fd, path, file_size);
                   }
              }
         }
         else if(cmd && strcmp(cmd,"downloadf")==0) {
              // Format: downloadf|filename|relative
              char *fname     = strtok(NULL,"|");
              char *relativep = strtok(NULL,"\n");
              if(!fname || !relativep) {
                   write(client_fd,"ERR: Invalid download format\n",
                         strlen("ERR: Invalid download format\n"));
              } else {
                   process_download_request(client_fd, fname, relativep);
              }
         }
         else if(cmd && strcmp(cmd,"dispfnames")==0) {
              // dispfnames|S2/relative
              process_dispfnames_pdf(client_fd);
         }
         else if(cmd && strcmp(cmd,"removef")==0) {
              // removef|filename|relative
              process_removef(client_fd);
         }
         else if(cmd && strcmp(cmd,"downltar")==0) {
              // downltar|.pdf
              char *extension = strtok(NULL,"\n");
              if(extension && strcmp(extension,".pdf")==0) {
                  process_downltar_pdf(client_fd);
              } else {
                  write(client_fd,
                        "ERR: Unsupported filetype for tar operation\n",
                        strlen("ERR: Unsupported filetype for tar operation\n"));
              }
         }
         else {
              // If no recognized command, respond with an error
              write(client_fd, "ERR: Unknown command\n", strlen("ERR: Unknown command\n"));
         }

         // Close out the connection after command is processed
         close(client_fd);
    }

    close(srv_socket);
    return 0;
}
