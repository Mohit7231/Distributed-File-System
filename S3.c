/***************************************************************************************
 *      S3 Server Application
 *      (COMP 8567 Distributed File System - Summer 2025)
 * This file defines the S3 server, which is responsible for storing and retrieving .txt
 * files at path ~/S3. S3 can:
 *   - Store (.txt) files
 *   - Send those files back when requested
 *   - Remove them if asked
 *   - Provide a downltar (.txt) tarball to S1 upon request
 *
 
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

/* 
 * The port at which this S3 server will listen.
 * Make sure it matches your usage in the main program's definitions.
 */
#define S3_PORT 8303      

/* 
 * The buffer size used for data transfers and reading lines.
 */
#define BUFFER_CAPACITY 2048  

/*
 * A global character array that indicates the root location on the filesystem
 * for storing and retrieving .txt files. This is typically ~/S3.
 */
char s3_root_path[256];

/*
 * -----------------------------------------------------------------------------------------
 * Function Declarations (forward):
 * -----------------------------------------------------------------------------------------
 * We declare all functions that will be defined below, giving us a clearer overview
 * of what is implemented here. 
 */
ssize_t readdata_line(int descriptor, void *buff, size_t max_length);
ssize_t readdata_line_remote(int descriptor, void *buff, size_t max_length);
void ensure_dir_exists(const char *some_path);
void s3_process_store(int conn_fd, char *file_path, long fsize);
void s3_process_download(int conn_fd, char *fname, char *rel_path);
void s3_process_dispfnames(int conn_fd);
void s3_process_removef(int conn_fd);
void s3_process_downltar_txt(int conn_fd);  // Our specialized function for downltar .txt
void s3_prcclient(int connector_fd);        // The central client-handling function

/*******************************************************************************************
 * ensure_dir_exists:
 *  - This function uses a system("mkdir -p ...") approach to ensure that the specified path
 *    exists, creating directories recursively if needed. Prints a small log message for info.
 ******************************************************************************************/
void ensure_dir_exists(const char *some_path) {
    char mkdir_cmd[256];
    snprintf(mkdir_cmd, sizeof(mkdir_cmd), "mkdir -p %s", some_path);
    system(mkdir_cmd);
    printf("S3: Ensured directory %s exists\n", some_path);
}

/*******************************************************************************************
 * readdata_line:
 *  - Reads data from a file descriptor line-by-line until newline or maximum length reached.
 *  - This is especially useful for reading commands from the incoming socket. 
 ******************************************************************************************/
ssize_t readdata_line(int descriptor, void *buff, size_t max_length) {
    ssize_t bytes_count, read_count;
    char c;
    char *ptr = (char *) buff;

    for(bytes_count = 1; bytes_count < max_length; bytes_count++) {
        read_count = read(descriptor, &c, 1);
        if(read_count == 1) {
            *ptr++ = c;
            if(c == '\n') {
                break; 
            }
        } else if(read_count == 0) {
            if(bytes_count == 1) 
                return 0; 
            else 
                break;
        } else {
            return -1;
        }
    }
    *ptr = 0; 
    return bytes_count;
}

/*******************************************************************************************
 * readdata_line_remote:
 *  - For clarity, we can wrap readdata_line logic for remote usage, 
 *    but here it just calls the same function. 
 ******************************************************************************************/
ssize_t readdata_line_remote(int descriptor, void *buff, size_t max_length) {
    return readdata_line(descriptor, buff, max_length);
}

/*******************************************************************************************
 * s3_process_store:
 *  - This function receives file data from the socket and stores it under the ~/S3 directory.
 *  - The path has already been verified, and we do only .txt in this server, but from S1's 
 *    perspective, it just sees "store" commands. We save it to disk and confirm success/failure.
 ******************************************************************************************/
void s3_process_store(int conn_fd, char *file_path, long fsize) {
    printf("S3: Processing store request for %s, size: %ld\n", file_path, fsize);

    // Build a full absolute path for saving the file under ~/S3
    char full_save_path[512];
    snprintf(full_save_path, sizeof(full_save_path), "%s/%s", s3_root_path, file_path);

    // Ensure the directory portion of the path is created
    char *last_slash = strrchr(full_save_path, '/');
    if(last_slash) {
        *last_slash = '\0'; 
        ensure_dir_exists(full_save_path);
        *last_slash = '/'; 
    }

    FILE *out_file = fopen(full_save_path, "wb");
    if(!out_file) {
        fprintf(stderr, "S3: Failed opening %s: %s\n", full_save_path, strerror(errno));
        write(conn_fd, "ERR: Cannot open file\n", strlen("ERR: Cannot open file\n"));
        return;
    }

    char buffer_area[BUFFER_CAPACITY];
    long accumulated = 0;

    // Read from the socket in chunks, writing them to the file.
    while(accumulated < fsize) {
        int chunk = read(conn_fd, buffer_area, (fsize - accumulated) < BUFFER_CAPACITY ?
                                          (fsize - accumulated) : BUFFER_CAPACITY);
        if(chunk <= 0) {
            fclose(out_file);
            fprintf(stderr, "S3: Incomplete file receive %s\n", full_save_path);
            write(conn_fd, "ERR: Incomplete file transfer\n", strlen("ERR: Incomplete file transfer\n"));
            return;
        }
        fwrite(buffer_area, 1, chunk, out_file);
        accumulated += chunk;
    }

    fclose(out_file);
    printf("S3: Successfully stored %s\n", full_save_path);
    write(conn_fd, "OK: File stored\n", strlen("OK: File stored\n"));
}

/*******************************************************************************************
 * s3_process_download:
 *  - Looks up the requested file from disk, sends "OK|filesize" then streams the contents
 *    back to the connected client or server that requested it. 
 ******************************************************************************************/
void s3_process_download(int conn_fd, char *fname, char *rel_path) {
    // Construct the full location from ~/S3 plus the relative path + filename
    char entire_path[512];
    snprintf(entire_path, sizeof(entire_path), "%s/%s/%s", s3_root_path, rel_path, fname);
    printf("S3: Processing downloadf for file: %s\n", entire_path);

    int fdesc = open(entire_path, O_RDONLY);
    if(fdesc < 0) {
        write(conn_fd, "ERR: File not found\n", strlen("ERR: File not found\n"));
        return;
    }

    off_t data_size = lseek(fdesc, 0, SEEK_END);
    lseek(fdesc, 0, SEEK_SET);
    if(data_size <= 0) {
        write(conn_fd, "ERR: Empty file\n", strlen("ERR: Empty file\n"));
        close(fdesc);
        return;
    }

    char reply[128];
    snprintf(reply, sizeof(reply), "OK|%ld\n", (long)data_size);
    write(conn_fd, reply, strlen(reply));

    char local_buffer[BUFFER_CAPACITY];
    long sum_sent = 0;
    while(sum_sent < data_size) {
        int readbytes = read(fdesc, local_buffer, sizeof(local_buffer));
        if(readbytes <= 0) {
            break;
        }
        write(conn_fd, local_buffer, readbytes);
        sum_sent += readbytes;
    }
    printf("S3: Completed sending %s (%ld bytes sent)\n", entire_path, sum_sent);

    close(fdesc);
}

/*******************************************************************************************
 * s3_process_dispfnames_S3:
 *  - This reads the next token as a directory argument, ensures it starts with S3/, 
 *    and lists all .txt files from that directory. Then sorts them alphabetically. 
 ******************************************************************************************/
void s3_process_dispfnames(int conn_fd) {
    // We parse the argument (directory) after "dispfnames|..."
    char *dir_arg = strtok(NULL, "\n");
    if(!dir_arg) {
         write(conn_fd, "ERR: No directory provided\n", strlen("ERR: No directory provided\n"));
         close(conn_fd);
         return;
    }
    if(strncmp(dir_arg, "S3/", 3) != 0) {
         write(conn_fd, "ERR: Directory must start with S3/\n", strlen("ERR: Directory must start with S3/\n"));
         close(conn_fd);
         return;
    }
    // Remove the "S3/" prefix to get the relative portion
    char *rel_offset = dir_arg + 3;

    // Construct the actual path under s3_root_path
    char real_dir[512];
    snprintf(real_dir, sizeof(real_dir), "%s/%s", s3_root_path, rel_offset);

    DIR *directory_obj = opendir(real_dir);
    if(!directory_obj) {
         write(conn_fd, "ERR: Cannot open directory\n", strlen("ERR: Cannot open directory\n"));
         close(conn_fd);
         return;
    }

    int file_count = 0, capacity = 10;
    char **file_listing = malloc(capacity * sizeof(char *));
    struct dirent *entry_ref;
    while((entry_ref = readdir(directory_obj)) != NULL) {
         if(entry_ref->d_type == DT_REG) {
              char *dot_ref = strrchr(entry_ref->d_name, '.');
              // We only handle .txt here
              if(dot_ref && strcmp(dot_ref, ".txt") == 0) {
                   if(file_count >= capacity) {
                       capacity *= 2;
                       file_listing = realloc(file_listing, capacity * sizeof(char*));
                   }
                   file_listing[file_count++] = strdup(entry_ref->d_name);
              }
         }
    }
    closedir(directory_obj);

    // Sort them by name
    for(int i = 0; i < file_count - 1; i++) {
         for(int j = i + 1; j < file_count; j++) {
              if(strcmp(file_listing[i], file_listing[j]) > 0) {
                   char *temp_swp = file_listing[i];
                   file_listing[i] = file_listing[j];
                   file_listing[j] = temp_swp;
              }
         }
    }

    // Build a newline-separated list
    size_t total_len = 0;
    for(int i=0; i<file_count; i++){
        total_len += strlen(file_listing[i]) + 1;
    }
    char *result_str = malloc(total_len + 1);
    result_str[0] = '\0';
    for(int i=0; i<file_count; i++){
        strcat(result_str, file_listing[i]);
        strcat(result_str, "\n");
        free(file_listing[i]);
    }
    free(file_listing);

    if(strlen(result_str) == 0) {
         strcpy(result_str, "No files found\n");
    }
    // Send it back to the caller
    write(conn_fd, result_str, strlen(result_str));

    free(result_str);
    close(conn_fd);
}

/*******************************************************************************************
 * s3_process_removef:
 *  - Handles removef|<filename>|<relative> for S3. 
 *  - We build a full path to the file and remove() it from disk if present.
 ******************************************************************************************/
void s3_process_removef(int conn_fd) {
    char *fname = strtok(NULL, "|");
    char *rel_str = strtok(NULL, "\n");
    if(!fname || !rel_str) {
         write(conn_fd, "ERR: Invalid removef format\n", strlen("ERR: Invalid removef format\n"));
         return;
    }
    char remove_path[512];
    snprintf(remove_path, sizeof(remove_path), "%s/%s/%s", s3_root_path, rel_str, fname);
    printf("S3: Removing file: %s\n", remove_path);
    if(remove(remove_path) == 0) {
         write(conn_fd, "OK: File removed\n", strlen("OK: File removed\n"));
    } else {
         char error_msg[128];
         snprintf(error_msg, sizeof(error_msg), "ERR: Removal failed (%s)\n", strerror(errno));
         write(conn_fd, error_msg, strlen(error_msg));
    }
}

/*******************************************************************************************
 * s3_process_downltar_txt:
 *  - Creates a tar called text.tar containing all .txt under ~/S3.
 *  - Sends that tar to whoever asked (S1, presumably).
 ******************************************************************************************/
void s3_process_downltar_txt(int conn_fd) {
    // We place the tar in s3_root_path for convenience
    char tar_path[512];
    snprintf(tar_path, sizeof(tar_path), "%s/text.tar", s3_root_path);

    // Make sure no old leftover remains
    remove(tar_path);

    // We'll build a command to locate .txt under s3_root_path and pack them into text.tar
    char command[1024];
    snprintf(command, sizeof(command),
        "cd %s && find . -type f -name '*.txt' | tar -cf text.tar -T -",
        s3_root_path);
    int exec_status = system(command);
    if(exec_status != 0) {
        write(conn_fd, "ERR: Failed to create tar archive\n", strlen("ERR: Failed to create tar archive\n"));
        return;
    }

    // Attempt to open the newly created tar file
    int fd_open = open(tar_path, O_RDONLY);
    if(fd_open < 0) {
        write(conn_fd, "ERR: Cannot open tar archive\n", strlen("ERR: Cannot open tar archive\n"));
        return;
    }

    // Determine the file size for our header
    off_t tar_size = lseek(fd_open, 0, SEEK_END);
    lseek(fd_open, 0, SEEK_SET);
    if(tar_size <= 0) {
        write(conn_fd, "ERR: Empty archive\n", strlen("ERR: Empty archive\n"));
        close(fd_open);
        return;
    }

    // Announce how big it is
    char header_line[128];
    snprintf(header_line, sizeof(header_line), "OK|%ld\n", (long)tar_size);
    write(conn_fd, header_line, strlen(header_line));

    // Stream the entire tar to the client
    char xfer_buf[BUFFER_CAPACITY];
    long accumulate = 0;
    while(accumulate < tar_size) {
        int block = read(fd_open, xfer_buf, sizeof(xfer_buf));
        if(block <= 0) {
            break;
        }
        write(conn_fd, xfer_buf, block);
        accumulate += block;
    }
    close(fd_open);

    // remove the temp tar to clean up
    remove(tar_path);

    printf("S3: Sent text.tar (%ld bytes)\n", accumulate);
}

/*******************************************************************************************
 * s3_prcclient:
 *  - This function processes every command from the connected client. The main 
 *    commands we expect are "store", "downloadf", "dispfnames", "removef", or "downltar|.txt".
 ******************************************************************************************/
void s3_prcclient(int client_sock) {
    char in_header[BUFFER_CAPACITY];
    ssize_t hdr_size = readdata_line(client_sock, in_header, sizeof(in_header));
    if(hdr_size <= 0) {
         printf("S3: Client disconnected during header read\n");
         close(client_sock);
         return;
    }
    printf("S3: Received: %s", in_header);

    // Break the header line by '|'
    char *init_cmd = strtok(in_header, "|");
    if(!init_cmd) {
         write(client_sock, "ERR: Empty command\n", strlen("ERR: Empty command\n"));
         close(client_sock);
         return;
    }

    // If the main command is "store"
    if(strcmp(init_cmd, "store") == 0) {
         char *file_path = strtok(NULL, "|");
         char *size_string = strtok(NULL, "\n");
         if(!file_path || !size_string) {
              write(client_sock, "ERR: Invalid store format\n", strlen("ERR: Invalid store format\n"));
         } else {
              long f_size = atol(size_string);
              if(f_size <= 0) {
                   write(client_sock, "ERR: Invalid file size\n", strlen("ERR: Invalid file size\n"));
              } else {
                   s3_process_store(client_sock, file_path, f_size);
              }
         }
         close(client_sock);
    }

    // If the command is "downloadf"
    else if(strcmp(init_cmd, "downloadf") == 0) {
         char *f_name = strtok(NULL, "|");
         char *relative_path = strtok(NULL, "\n");
         if(!f_name || !relative_path) {
              write(client_sock, "ERR: Invalid download format\n", strlen("ERR: Invalid download format\n"));
         } else {
              s3_process_download(client_sock, f_name, relative_path);
         }
         close(client_sock);
    }

    // If the command is "dispfnames"
    else if(strcmp(init_cmd, "dispfnames") == 0) {
         s3_process_dispfnames(client_sock);
    }

    // If the command is "removef"
    else if(strcmp(init_cmd, "removef") == 0) {
         s3_process_removef(client_sock);
         close(client_sock);
    }

    // If the command is "downltar", we might have .txt
    else if(strcmp(init_cmd, "downltar") == 0) {
         char *extension = strtok(NULL, "\n");
         if(extension && strcmp(extension, ".txt") == 0) {
             s3_process_downltar_txt(client_sock);
         } else {
             write(client_sock, "ERR: Unsupported filetype for tar operation\n",
                   strlen("ERR: Unsupported filetype for tar operation\n"));
         }
         close(client_sock);
    }

    // If command is unknown
    else {
         write(client_sock, "ERR: Unknown command\n", strlen("ERR: Unknown command\n"));
         close(client_sock);
    }
}

/*******************************************************************************************
 * main:
 *  - Sets up S3: creates ~/S3, binds to the S3_PORT, and runs an infinite accept() loop.
 *  - For each new connection, it spawns s3_prcclient() that handles the request.
 ******************************************************************************************/
int main() {
    // Derive the root directory => ~/S3
    snprintf(s3_root_path, sizeof(s3_root_path), "%s/S3", getenv("HOME"));
    ensure_dir_exists(s3_root_path);

    // Create a TCP socket
    int s3_socket = socket(AF_INET, SOCK_STREAM, 0);
    if(s3_socket < 0) {
         perror("S3: Socket creation failed");
         exit(1);
    }

    struct sockaddr_in s3_addr;
    s3_addr.sin_family      = AF_INET;
    s3_addr.sin_addr.s_addr = INADDR_ANY;
    s3_addr.sin_port        = htons(S3_PORT);

    int optval = 1;
    setsockopt(s3_socket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    // Bind to the specified S3 port
    if(bind(s3_socket, (struct sockaddr*)&s3_addr, sizeof(s3_addr)) < 0) {
         perror("S3: Bind failed");
         close(s3_socket);
         exit(1);
    }

    // Listen for up to 10 connections
    if(listen(s3_socket, 10) < 0) {
         perror("S3: Listen failed");
         close(s3_socket);
         exit(1);
    }

    printf("S3 running on port %d, root: %s\n", S3_PORT, s3_root_path);

    // Accept loop
    while(1) {
         int incoming_fd = accept(s3_socket, NULL, NULL);
         if(incoming_fd < 0) {
              perror("S3: Accept failed");
              continue;
         }
         printf("S3: New connection accepted\n");

         // For simplicity, we handle each request in a single function call:
         s3_prcclient(incoming_fd);
    }

    // If we ever break out (normally we won't), we close the server socket
    close(s3_socket);
    return 0;
}
