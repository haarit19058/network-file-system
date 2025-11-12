#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>

/**
 * @brief A fixed-size thread pool for managing worker threads.
 *
 * This implementation uses a single synchronized queue for tasks.
 */
class ThreadPool {
public:
    ThreadPool(size_t threads)
        : stop(false)
    {
        if (threads == 0) throw std::invalid_argument("Thread count must be greater than zero.");
        
        // Start worker threads
        for(size_t i = 0; i < threads; ++i) {
            workers.emplace_back(
                [this] { // The worker thread function
                    while(true) {
                        std::function<void()> task;
                        
                        // Critical section to acquire a task
                        {
                            std::unique_lock<std::mutex> lock(this->queue_mutex);
                            
                            // Wait until the queue is not empty OR the pool is asked to stop
                            this->condition.wait(lock, 
                                [this]{ return this->stop || !this->tasks.empty(); });
                            
                            // If stopping and no more tasks, exit the worker thread
                            if(this->stop && this->tasks.empty())
                                return;
                            
                            // Get the task and remove it from the queue
                            task = std::move(this->tasks.front());
                            this->tasks.pop();
                        }
                        
                        // Execute the task outside the mutex lock
                        task();
                    }
                }
            );
        }
    }

    // Template function to submit a task to the queue
    template<class F, class... Args>
    void enqueue(F&& f, Args&&... args) {
        // Wrap the function and arguments into a callable object
        auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);

        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            
            // Don't allow submission if the pool is stopped
            if(stop)
                throw std::runtime_error("enqueue on stopped ThreadPool");
            
            // Add the task to the queue
            tasks.emplace(task);
        }
        
        // Notify one waiting worker thread that a new task is available
        condition.notify_one();
    }
    
    // Destructor joins all worker threads
    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        
        // Wake up all waiting threads so they can exit the loop
        condition.notify_all();
        
        // Wait for all worker threads to finish their execution
        for(std::thread &worker: workers) {
            if(worker.joinable())
                worker.join();
        }
    }

private:
    // Need to keep track of threads so we can join them
    std::vector<std::thread> workers;
    
    // The task queue
    std::queue<std::function<void()>> tasks;
    
    // Synchronization primitives
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};