/*
Fetching triggered by cpu

This way the data path is:
network -> fpga
instead of
network -> cpu/memory -> pcie -> fpga

Need to implement in hardware?
macaddress?
This is the hardest part. S3 uses HTTP, which requires a reliable TCP connection. You need a hardware IP block that handles TCP handshakes, sequence numbers, and packet reassembly.
HTTP State Machine: Your custom logic.
*/

#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

int main() {
    // 1. Setup Connection Parameters
    const char* minio_ip = "10.253.74.70";
    int minio_port = 9000;
    std::string bucket = "test";      // The bucket you created
    std::string object = "test.csv";  // The file we'll download

    // 2. Create Socket and Connect
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "Error creating socket\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(minio_port);
    inet_pton(AF_INET, minio_ip, &server_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Connection failed - is MinIO running on " << minio_ip << ":" << minio_port << "?\n";
        close(sock);
        return 1;
    }
    std::cout << "Connected to MinIO server.\n";

    // 3. Send HTTP Request
    std::string request = 
        "GET /" + bucket + "/" + object + " HTTP/1.1\r\n" +
        "Host: " + std::string(minio_ip) + ":" + std::to_string(minio_port) + "\r\n" +
        "Connection: close\r\n\r\n";
        
    send(sock, request.c_str(), request.length(), 0);
    std::cout << "Request sent.\n\n";

    // 4. Read Response (Headers + Data)
    char buffer[4096];
    int bytes_received;
    
    std::cout << "--- Response ---\n";
    while ((bytes_received = recv(sock, buffer, sizeof(buffer) - 1, 0)) > 0) {
        buffer[bytes_received] = '\0'; // Null-terminate for printing
        std::cout << buffer;
    }
    std::cout << "\n----------------\n";

    close(sock);
    return 0;
}
