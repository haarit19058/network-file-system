#include <shared_mutex>
#include <unordered_map>
#include <string>
#include <mutex>

/**
 * @brief Manages reader-writer locks mapped to file paths.
 * * Provides shared (read) locking and unique (write) locking based on a key (file path).
 */
class RWLockManager {
private:
    // The main map: path (string) -> shared_mutex
    // NOTE: This shared_mutex instance is itself protected by its parent mutex.
    std::unordered_map<std::string, std::shared_mutex> lock_map;
    
    // Mutex to protect the map itself from concurrent insertions/deletions.
    std::mutex map_mutex;

public:
    // Helper to acquire a unique lock (for writing)
    std::unique_lock<std::shared_mutex> acquire_write_lock(const std::string& path) {
        // 1. Lock the map to ensure safe access/insertion
        std::lock_guard<std::mutex> map_lock(map_mutex);
        
        // 2. Insert the path if it doesn't exist, and return a reference to its mutex
        // The default-constructed shared_mutex will be used if a new key is inserted.
        std::shared_mutex& target_mutex = lock_map[path];
        
        // 3. Return a unique_lock on the target_mutex.
        // The unique_lock immediately tries to acquire the exclusive lock.
        return std::unique_lock<std::shared_mutex>(target_mutex);
    }

    // Helper to acquire a shared lock (for reading)
    std::shared_lock<std::shared_mutex> acquire_read_lock(const std::string& path) {
        // 1. Lock the map to ensure safe access/insertion
        std::lock_guard<std::mutex> map_lock(map_mutex);
        
        // 2. Insert the path if it doesn't exist, and return a reference to its mutex
        std::shared_mutex& target_mutex = lock_map[path];
        
        // 3. Return a shared_lock on the target_mutex.
        // The shared_lock immediately tries to acquire the shared lock.
        return std::shared_lock<std::shared_mutex>(target_mutex);
    }
    
};