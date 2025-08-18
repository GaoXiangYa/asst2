#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stop_token>
#include <thread>
#include <vector>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
  TaskSystemSerial(int num_threads);
  ~TaskSystemSerial();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
public:
  TaskSystemParallelSpawn(int num_threads);
  ~TaskSystemParallelSpawn();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSpinning(
      int num_threads = std::thread::hardware_concurrency());
  ~TaskSystemParallelThreadPoolSpinning();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

private:
  struct TaskGroup {
    std::atomic<int> remain_tasks; // current task group remain tasks
    int num_total_tasks;           // current task group total tasks
    IRunnable *runnable;
    std::condition_variable completed_cv;
    std::mutex completed_mutex;
  };

private:
  template <typename Func, typename... Args>
  auto enqueue(Func &&func, Args &&...args)
      -> std::future<std::invoke_result_t<Func, Args...>> {
    using ReturnType = std::invoke_result_t<Func, Args...>;

    auto task = std::make_shared<std::packaged_task<ReturnType()>>(
        [func = std::forward<Func>(func),
         ... args = std::forward<Args>(args)]() {
          if constexpr (std::is_void_v<ReturnType>) {
            std::invoke(std::move(func), std::move(args)...);
          } else {
            return std::invoke(std::move(func), std::move(args)...);
          }
        });

    std::future<ReturnType> result = task->get_future();

    {
      std::unique_lock<std::mutex> lock(mutex_);
      task_queue_.emplace([task]() { (*task)(); });
    }

    task_queue_cv_.notify_one();
    return result;
  }

  void submitTaskGroupTasks(std::shared_ptr<TaskGroup> &task_group);

private:
  std::vector<std::thread> thread_pool_;
  std::queue<std::function<void()>> task_queue_;
  std::mutex mutex_;
  std::condition_variable task_queue_cv_;
  bool stop_{false};
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class ThreadPool;
class Worker {

  using Task = std::function<void()>;

public:
  Worker(const int worker_id, ThreadPool *thread_pool)
      : worker_id_(worker_id), thread_pool_(thread_pool) {}

  void join() { thread_.join(); }

  void run();

  std::optional<Task> tryStealTask();

public:
  std::deque<Task> task_queue;
  std::condition_variable_any cv;
  int worker_id_;

private:
  std::thread thread_;
  std::mutex mutex_;
  ThreadPool *thread_pool_;
};
class ThreadPool {
public:
  ThreadPool(const int num_threads);

  ~ThreadPool();

  template <typename Func, typename... Args>
  auto submitTask(Func &&func, Args &&...args)
      -> std::future<std::invoke_result_t<Func, Args...>> {
    using ReturnType = std::invoke_result_t<Func, Args...>;

    auto task = std::make_shared<std::packaged_task<ReturnType()>>(
        [func = std::forward<Func>(func),
         ... args = std::forward<Args>(args)]() {
          if constexpr (std::is_void_v<ReturnType>) {
            std::invoke(std::move(func), std::move(args)...);
          } else {
            return std::invoke(std::move(func), std::move(args)...);
          }
        });

    std::future<ReturnType> result = task->get_future();

    {
      std::unique_lock<std::mutex> lock(mutex_);
      worker_idx_ = (worker_idx_ + 1) % num_threads_;
      thread_pool_[worker_idx_]->task_queue.emplace_back([task] { (*task)(); });
      thread_pool_[worker_idx_]->cv.notify_one();
      this->global_task_count_.fetch_add(1);
    }

    return result;
  }

  void execute();

  Worker *getVictim();

public:
  std::vector<std::unique_ptr<Worker>> thread_pool_;
private:
  int worker_idx_ = 0;
  int num_threads_;
  std::mutex mutex_;
  std::stop_source stop_source_;

public:
  std::stop_token stop_token_ = stop_source_.get_token();
  std::atomic<int> global_task_count_;
};

class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSleeping(
      int num_threads = std::thread::hardware_concurrency());
  ~TaskSystemParallelThreadPoolSleeping();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

private:
  struct TaskGroup {
    std::atomic<int> remain_tasks; // current task group remain tasks
    int num_total_tasks;           // current task group total tasks
    IRunnable *runnable;
    std::condition_variable completed_cv;
    std::mutex completed_mutex;
  };

private:
  void submitTaskGroupTasks(std::shared_ptr<TaskGroup> &task_group);

private:
  ThreadPool thread_pool_;
  std::mutex mutex_;
  int num_threads_;
};

#endif
