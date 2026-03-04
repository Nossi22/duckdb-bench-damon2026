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

#include <duckdb.hpp>

#include "common.hpp"

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    std::string data_dir = "/mnt/ramdisk";
    Source source = Source::PARQUET;
    std::vector<uint32_t> threads{1};
    uint32_t repetitions = 5;
    uint32_t streams = 2;  // Number of concurrent query streams
    std::string s3_endpoint = "minio:9000";
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

uint64_t get_rx_bytes() {
    // change if needed
    std::ifstream file("/sys/class/net/data1/statistics/rx_bytes");
    uint64_t bytes = 0;
    if (file.is_open()) {
        file >> bytes;
    }
    return bytes;
}

std::vector<double> run_throughput_benchmark(std::shared_ptr<duckdb::DuckDB> db, uint32_t num_streams, 
        uint32_t num_repetitions, const std::vector<std::string>& queries, std::vector<double>* throughput_gbps) {        
    std::vector<QueryStream> streams;
    
    for (int i = 0; i < num_streams; ++i) {
        streams.push_back(QueryStream(i, db, queries));
    }
    
    Timer timer;
    std::vector<double> runtimes;

    for (int i = 0; i < num_repetitions; i++) {
        std::vector<std::thread> threads;

        uint64_t rx_bytes_before = get_rx_bytes();
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
        uint64_t rx_bytes_after = get_rx_bytes();
        throughput_gbps->push_back((rx_bytes_after - rx_bytes_before) * 8.0 / elapsed / 1e9);
        runtimes.push_back(elapsed);
    }
    
    
    return runtimes;
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
              << "  -e, --s3-endpoint ENDPOINT S3 endpoint (default: minio:9000)\n"
              << "  -d, --data-dir DIR    Data directory (default: /mnt/ramdisk)\n"
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
        {"s3-endpoint", required_argument, 0, 'e'},
        {"help",        no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    
    int opt;
    while ((opt = getopt_long(argc, argv, "s:r:t:S:d:e:h", long_options, nullptr)) != -1) {
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
            case 'e':
                config.s3_endpoint = optarg;
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

void write_json(uint32_t num_threads, const Config& config, std::vector<double> result, 
                const std::string& output_file) {
    std::ofstream out(output_file);

    std::sort(result.begin(), result.end());
    
    // Write result
    out << "{\n";
    out << "    \"runtime_sec\": " << std::fixed << std::setprecision(6) 
        << result[result.size() / 2] << ",\n";
    out << "    \"runtimes\": [";
    for (size_t i = 0; i < result.size(); ++i) {
        out << std::fixed << std::setprecision(6) << result[i];
        if (i + 1 < result.size()) 
            out << ", ";
    }
    out << "],\n";
    out << "    \"threads\": " << num_threads << ",\n";
    out << "    \"streams\": " << config.streams << ",\n";
    out << "    \"s3_endpoint\": \"" << config.s3_endpoint << "\",\n";
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
    
    setup_con.Query("SET parquet_metadata_cache TO true");

    // Load view rewriter extension
    setup_con.Query("LOAD 'extension/build/release/extension/view_rewriter/view_rewriter.duckdb_extension'");
    
    // Load data
    setup_con.Query("INSTALL json; LOAD json");
    setup_con.Query("INSTALL httpfs; LOAD httpfs;");
    
    if (config.source == Source::MINIO) {
        setup_con.Query(
            "DROP SECRET IF EXISTS cluster_test; "
            "CREATE SECRET cluster_test ("
            "TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', "
            "ENDPOINT '" + config.s3_endpoint + ":9000', " 
            "URL_STYLE 'path', USE_SSL false);"
        );


        //check connection to MinIO by reading metadata of one parquet file
        auto preflight = setup_con.Query(
            "SELECT * FROM read_parquet('s3://" + config.data_dir + "/*.parquet') LIMIT 1"
        );
        if (preflight->HasError()) {
            throw std::runtime_error(
                "MinIO preflight failed for s3://" + config.data_dir + "/*.parquet: " +
                preflight->GetError()
            );
        }
        preflight->Fetch();
        if (preflight->HasError()) {
            throw std::runtime_error(
                "MinIO preflight failed for s3://" + config.data_dir + "/*.parquet: " +
                preflight->GetError()
            );
        }
        std::cout << "MinIO preflight OK for s3://" << config.data_dir << "/*.parquet" << std::endl;
    }

    std::cout << "Loading data..." << std::endl;
    load_data(setup_con, config.data_dir, config.source);

    // Run benchmark
    for (uint32_t num_threads : config.threads) {
        setup_con.Query("SET GLOBAL threads TO " + std::to_string(num_threads));

        std::cout << "Running throughput benchmark for " << std::to_string(num_threads) << " thread(s)..." << std::endl;
        std::vector<double> throughput_gbps;
        auto result = run_throughput_benchmark(db, config.streams, config.repetitions, queries, &throughput_gbps);
        
        std::string output_file = "measurements/throughput/" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".json";
        write_json(num_threads, config, result, output_file);
        std::sort(throughput_gbps.begin(), throughput_gbps.end());
        std::cout << "Throughput Gbit/s Peak: " << throughput_gbps[throughput_gbps.size() - 1] << " \n";
        std::cout << "Results written to: " << output_file << std::endl;
    }
    
    return 0;
}