#include <iostream>
#include <fstream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

#define PORT 8080
#define BUFFER_SIZE 65536  // 64 KB

int main() {
    int sock = 0;
    struct sockaddr_in serv_addr;
    char buffer[BUFFER_SIZE];

    // Create socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation error");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    // Connect to server (replace with Windows server IP)
    if (inet_pton(AF_INET, "192.168.1.100", &serv_addr.sin_addr) <= 0) {
        perror("Invalid address/Address not supported");
        return -1;
    }

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Connection failed");
        return -1;
    }

    // Receive file size
    std::streamsize fileSize;
    read(sock, &fileSize, sizeof(fileSize));
    std::cout << "Expecting file of size " << fileSize << " bytes.\n";

    std::ofstream outfile("downloaded.bin", std::ios::binary);

    std::streamsize received = 0;
    int bytesRead;
    while (received < fileSize && (bytesRead = read(sock, buffer, BUFFER_SIZE)) > 0) {
        outfile.write(buffer, bytesRead);
        received += bytesRead;
    }

    std::cout << "File downloaded successfully (" << received << " bytes).\n";

    outfile.close();
    close(sock);

    return 0;
}
