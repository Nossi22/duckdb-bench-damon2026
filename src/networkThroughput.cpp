#include <iostream>
#include <fstream>
#include <chrono>
#include <duckdb.hpp>

// Reads from the network interface you specified earlier
uint64_t get_rx_bytes() {
    std::ifstream file("/sys/class/net/data1/statistics/rx_bytes");
    uint64_t bytes = 0;
    if (file.is_open()) file >> bytes;
    return bytes;
}

int main() {
    duckdb::DBConfig config;
    duckdb::DuckDB db(nullptr, &config);
    duckdb::Connection con(db);

    // 1. Setup MinIO directly
    con.Query("INSTALL httpfs; LOAD httpfs;");
    con.Query("CREATE SECRET minio_sec (TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', "
              "ENDPOINT '10.253.74.70:9000', URL_STYLE 'path', USE_SSL false);");

    // 2. Use your known peak thread count
    con.Query("SET GLOBAL threads TO 64;");

    std::cout << "Starting pure I/O network test..." << std::endl;

    // 3. Start timing and network counters
    uint64_t rx_before = get_rx_bytes();
    auto start_time = std::chrono::high_resolution_clock::now();

    // 4. THE I/O TEST: Force a scan of all files without complex joins
    // A simple aggregation forces DuckDB to pull and decompress everything
    auto result = con.Query("SELECT SUM(l_extendedprice) FROM 's3://testbench/tpch-30/lineitem.parquet';");

    // 5. Stop timing
    auto end_time = std::chrono::high_resolution_clock::now();
    uint64_t rx_after = get_rx_bytes();

    if (result->HasError()) {
        std::cerr << "Query failed: " << result->GetError() << std::endl;
        return 1;
    }

    // 6. Calculate strictly network throughput
    std::chrono::duration<double> elapsed = end_time - start_time;
    double network_gbps = (rx_after - rx_before) * 8.0 / elapsed.count() / 1e9;

    std::cout << "Time elapsed: " << elapsed.count() << " seconds\n";
    std::cout << "Network Throughput: " << network_gbps << " Gbit/s\n";

    return 0;
}