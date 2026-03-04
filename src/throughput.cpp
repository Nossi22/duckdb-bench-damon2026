#include <chrono>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <thread>
#include <mutex>
#include <atomic>
#include <getopt.h>
#include <cstdint>

#include <duckdb.hpp>

#include "common.hpp"

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    std::string data_dir = "/mnt/ramdisk";
    std::string storage_ip = "";
    std::string net_iface = "";
    Source source = Source::PARQUET;
    std::vector<uint32_t> threads{1};
    uint32_t repetitions = 5;
    uint32_t streams = 2;  // Number of concurrent query streams
};

struct BenchmarkResult {
    std::vector<double> runtimes;
    std::vector<uint64_t> net_rx_bytes;
    std::vector<double> net_gbps;
};

// ============================================================================
// Query Stream Worker
// ============================================================================

class QueryStream {
private:
    int stream_id_;
    duckdb::Connection con_;
    std::vector<std::string> queries_;

public:
    QueryStream(int stream_id, 
                std::shared_ptr<duckdb::DuckDB> db,
                const std::vector<std::string> &queries)
        : stream_id_(stream_id)
        , con_(*db)
        , queries_(queries)
    {
        std::mt19937 g(stream_id);
        std::shuffle(queries_.begin(), queries_.end(), g);
    }

    void run() {
        for (const auto &query : queries_) {
            auto r = con_.Query(query);
            r->Fetch();
        }
    }
};

// ============================================================================
// Benchmark Execution
// ============================================================================

uint64_t read_rx_bytes(const std::string &iface) {
    std::string counter_path = "/sys/class/net/" + iface + "/statistics/rx_bytes";
    std::ifstream in(counter_path);
    if (!in.is_open()) {
        throw std::runtime_error("Failed to read network counter: " + counter_path);
    }

    uint64_t bytes = 0;
    in >> bytes;
    if (in.fail()) {
        throw std::runtime_error("Failed to parse network counter: " + counter_path);
    }
    return bytes;
}

BenchmarkResult run_throughput_benchmark(std::shared_ptr<duckdb::DuckDB> db, uint32_t num_streams,
        uint32_t num_repetitions, const std::vector<std::string>& queries, const std::string &net_iface) {
    std::vector<QueryStream> streams;
    
    for (int i = 0; i < num_streams; ++i) {
        streams.push_back(QueryStream(i, db, queries));
    }
    
    Timer timer;
    BenchmarkResult result;
    bool measure_network = !net_iface.empty();

    for (int i = 0; i < num_repetitions; i++) {
        std::vector<std::thread> threads;

        uint64_t rx_before = 0;
        if (measure_network) {
            rx_before = read_rx_bytes(net_iface);
        }

        timer.start();
        
        for (int j = 0; j < num_streams; j++) {
            threads.emplace_back([&streams, j]() {
                streams[j].run();
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        double elapsed = timer.stop_seconds();

        if (measure_network) {
            uint64_t rx_after = read_rx_bytes(net_iface);
            uint64_t delta = 0;
            if (rx_after >= rx_before) {
                delta = rx_after - rx_before;
            }
            double gbps = (static_cast<double>(delta) * 8.0) / (elapsed * 1e9);
            result.net_rx_bytes.push_back(delta);
            result.net_gbps.push_back(gbps);
        }

        result.runtimes.push_back(elapsed);
    }

    return result;
}

// ============================================================================
// Command Line Parsing
// ============================================================================

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "TPC-H throughput benchmark with concurrent query streams.\n"
              << "\n"
              << "Options:\n"
              << "  -s, --source SRC      Input source: parquet, csv, json, memory,\n"
              << "                        filtered, projected (default: parquet)\n"
              << "  -r, --repetitions N   Number of repetitions per query (default: 5)\n"
              << "  -t, --threads N       Number of DuckDB threads as range \"1-8\",\n"
              << "                        list \"1,2,4\", or single value (default: 1)\n"
              << "  -S, --streams N       Number of concurrent query streams (default: 2)\n"
              << "  -d, --data-dir DIR    Data directory (default: /mnt/ramdisk)\n"
              << "  -i, --storage-ip IP   MinIO/S3 endpoint IP or host[:port] (e.g. 10.253.74.66:9000)\n"
              << "  -n, --net-iface IFACE Network interface for RX byte counting (e.g. enp1s0f0)\n"
              << "  -h, --help            Show this help message\n";
}

Config parse_args(int argc, char* argv[]) {
    Config config;
    
    static struct option long_options[] = {
        {"source",      required_argument, 0, 's'},
        {"repetitions", required_argument, 0, 'r'},
        {"threads",     required_argument, 0, 't'},
        {"streams",     required_argument, 0, 'S'},
        {"data-dir",    required_argument, 0, 'd'},
        {"storage-ip",  required_argument, 0, 'i'},
        {"net-iface",   required_argument, 0, 'n'},
        {"help",        no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    
    int opt;
    while ((opt = getopt_long(argc, argv, "s:r:t:S:d:i:n:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 's':
                config.source = string_to_source(optarg);
                break;
            case 't':
                try {
                    config.threads = parse_thread_arg(optarg);
                } catch (const std::exception& e) {
                    std::cerr << "Error parsing threads arg: " << e.what() << std::endl;
                    print_usage(argv[0]);
                    exit(1);
                }
                break;
            case 'r':
                config.repetitions = std::stoi(optarg);
                break;
            case 'S':
                config.streams = std::stoi(optarg);
                break;
            case 'd':
                config.data_dir = optarg;
                break;
            case 'i':
                config.storage_ip = optarg;
                break;
            case 'n':
                config.net_iface = optarg;
                break;
            case 'h':
                print_usage(argv[0]);
                exit(0);
            default:
                print_usage(argv[0]);
                exit(1);
        }
    }
    
    return config;
}

// ============================================================================
// Write JSON
// ============================================================================

void write_json(uint32_t num_threads, const Config& config, BenchmarkResult result,
                const std::string& output_file) {
    std::ofstream out(output_file);

    std::sort(result.runtimes.begin(), result.runtimes.end());

    std::vector<double> net_gbps_sorted = result.net_gbps;
    if (!net_gbps_sorted.empty()) {
        std::sort(net_gbps_sorted.begin(), net_gbps_sorted.end());
    }
    
    // Write result
    out << "{\n";
    out << "    \"runtime_sec\": " << std::fixed << std::setprecision(6) 
        << result.runtimes[result.runtimes.size() / 2] << ",\n";
    out << "    \"runtimes\": [";
    for (size_t i = 0; i < result.runtimes.size(); ++i) {
        out << std::fixed << std::setprecision(6) << result.runtimes[i];
        if (i + 1 < result.runtimes.size())
            out << ", ";
    }
    out << "],\n";
    out << "    \"net_iface\": \"" << config.net_iface << "\",\n";
    out << "    \"net_rx_bytes\": [";
    for (size_t i = 0; i < result.net_rx_bytes.size(); ++i) {
        out << result.net_rx_bytes[i];
        if (i + 1 < result.net_rx_bytes.size()) {
            out << ", ";
        }
    }
    out << "],\n";
    out << "    \"net_gbps\": [";
    for (size_t i = 0; i < result.net_gbps.size(); ++i) {
        out << std::fixed << std::setprecision(6) << result.net_gbps[i];
        if (i + 1 < result.net_gbps.size()) {
            out << ", ";
        }
    }
    out << "],\n";
    out << "    \"median_net_gbps\": " << std::fixed << std::setprecision(6);
    if (!net_gbps_sorted.empty()) {
        out << net_gbps_sorted[net_gbps_sorted.size() / 2] << ",\n";
    } else {
        out << "0.0,\n";
    }
    out << "    \"threads\": " << num_threads << ",\n";
    out << "    \"streams\": " << config.streams << ",\n";
    out << "    \"data_dir\": \"" << config.data_dir << "\",\n";
    out << "    \"source\": \"" << source_to_string(config.source) << "\",\n";
    out << "    \"repetitions\": " << config.repetitions << "\n";
    out << "}\n";
    
    out.close();
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char* argv[]) {
    Config config = parse_args(argc, argv);

    bool is_s3_data = config.data_dir.rfind("s3://", 0) == 0;
    
    // Get queries based on source
    std::vector<fs::path> query_paths = get_queries(config.source);
    std::vector<std::string> queries;
    for (const auto &query_path : query_paths) {
        std::ifstream f(query_path);
        std::string query((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        queries.push_back(query);
    }

    // Initialize DuckDB instance
    duckdb::DBConfig duck_config;
    duck_config.SetOptionByName("allow_unsigned_extensions", duckdb::Value::BOOLEAN(true));
    auto db = std::make_shared<duckdb::DuckDB>(nullptr, &duck_config);
    duckdb::Connection setup_con(*db);

    if (is_s3_data || !config.storage_ip.empty()) {
        std::string endpoint = config.storage_ip;
        if (!endpoint.empty() && endpoint.find(':') == std::string::npos) {
            endpoint += ":9000";
        }

        if (endpoint.empty()) {
            throw std::runtime_error("S3 data path detected but no storage endpoint provided. Use --storage-ip <ip[:port]>");
        }

        auto r = setup_con.Query("INSTALL httpfs; LOAD httpfs");
        if (r->HasError()) {
            throw std::runtime_error("Failed to load httpfs extension: " + r->GetError());
        }

        std::string secret_sql =
            "CREATE OR REPLACE SECRET cluster_test ("
            "TYPE S3, "
            "KEY_ID 'minioadmin', "
            "SECRET 'minioadmin', "
            "ENDPOINT '" + endpoint + "', "
            "URL_STYLE 'path', "
            "USE_SSL false"
            ")";

        r = setup_con.Query(secret_sql);
        if (r->HasError()) {
            throw std::runtime_error("Failed to create S3 secret 'cluster_test': " + r->GetError());
        }
    }
    
    //setup_con.Query("SET parquet_metadata_cache TO true");

    // Load view rewriter extension
    setup_con.Query("LOAD 'extension/build/release/extension/view_rewriter/view_rewriter.duckdb_extension'");
    
    // Load data
    setup_con.Query("INSTALL json; LOAD json");

    std::cout << "Loading data..." << std::endl;
    load_data(setup_con, config.data_dir, config.source);

    // Run benchmark
    for (uint32_t num_threads : config.threads) {
        setup_con.Query("SET GLOBAL threads TO " + std::to_string(num_threads));

        std::cout << "Running throughput benchmark for " << std::to_string(num_threads) << " thread(s)..." << std::endl;
        auto result = run_throughput_benchmark(db, config.streams, config.repetitions, queries, config.net_iface);
        
        std::string output_file = "measurements/throughput/" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".json";
        write_json(num_threads, config, result, output_file);
        
        std::cout << "Results written to: " << output_file << std::endl;
    }
    
    return 0;
}