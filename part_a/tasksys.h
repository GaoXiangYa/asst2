#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <thread>

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
    int remain_tasks; // current task group remain tasks
    int num_total_tasks;  // current task group total tasks
    IRunnable* runnable;
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

  void submitTaskGroupTasks(std::unique_ptr<TaskGroup>& task_group);

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
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

#endif
