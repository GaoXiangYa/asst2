#include "tasksys.h"
#include "itasksys.h"
#include <atomic>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <utility>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable,
                                          int num_total_tasks,
                                          const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemSerial::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Part A.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //

  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  auto func = [this]() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        task_queue_cv_.wait(lock,
                            [this]() { return !task_queue_.empty() || stop_; });
        if (stop_ && task_queue_.empty()) {
          return;
        }
        task = std::move(task_queue_.front());
        task_queue_.pop();
      }
      task();
    }
  };

  for (int i = 0; i < num_threads; ++i) {
    thread_pool_.emplace_back(func);
  }
}

void TaskSystemParallelThreadPoolSpinning::submitTaskGroupTasks(
    std::shared_ptr<TaskGroup> &task_group) {
  auto func = [task_group](int task_id) {
    task_group->runnable->runTask(task_id, task_group->num_total_tasks);
    // fetch_sub return old value
    int remain_task = task_group->remain_tasks.fetch_sub(1);
    if (remain_task == 1) {
      {
        std::unique_lock<std::mutex> lock(task_group->completed_mutex);
        task_group->completed_cv.notify_one();
      }
    }
  };
  for (int i = 0; i < task_group->num_total_tasks; ++i) {
    this->enqueue(func, i);
  }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    stop_ = true;
  }
  task_queue_cv_.notify_all();
  for (auto &thread : thread_pool_) {
    thread.join();
  }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable,
                                               int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Part A.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //
  auto task_group =
      std::make_shared<TaskGroup>(num_total_tasks, num_total_tasks, runnable);
  submitTaskGroupTasks(task_group);
  {
    std::unique_lock<std::mutex> lock(task_group->completed_mutex);
    task_group->completed_cv.wait(
        lock, [&task_group] { return task_group->remain_tasks == 0; });
  }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

std::optional<Worker::Task> Worker::tryStealTask() {
  std::unique_lock<std::mutex> lock(this->mutex_);
  if (this->task_queue.empty()) {
    return std::nullopt;
  }
  auto task = std::move(this->task_queue.front());
  this->task_queue.pop_front();
  return task;
}

void Worker::pushTask(Worker::Task task) {
  {
    std::unique_lock<std::mutex> lock(this->mutex_);
    this->task_queue.push_back(std::move(task));
  }
  this->cv.notify_one();
}

void Worker::run() {
  auto func = [this]() {
    while (true) {
      Worker::Task task;

      {
        std::unique_lock<std::mutex> lock(this->mutex_);

        this->cv.wait(lock, this->thread_pool_->stop_token_, [this] {
          return !this->task_queue.empty() ||
                 this->thread_pool_->stop_token_.stop_requested() ||
                 this->thread_pool_->global_task_count_ > 0;
        });

        if (this->thread_pool_->stop_token_.stop_requested() &&
            this->thread_pool_->global_task_count_.load() == 0) {
          break;
        }

        if (!this->task_queue.empty()) {
          task = std::move(task_queue.back());
          task_queue.pop_back();
        }
      }

      if (task == nullptr) {
        for (int attempt = 0;
             attempt < this->thread_pool_->thread_pool_.size() * 2; ++attempt) {
          auto victim = this->thread_pool_->getVictim();
          if (victim && victim != this) {
            if (auto stolen = victim->tryStealTask(); stolen.has_value()) {
              task = std::move(*stolen);
              break;
            }
          }
        }
      }

      if (task != nullptr) {
        this->thread_pool_->global_task_count_.fetch_sub(1);
        task();
      } else {
        std::this_thread::yield();
      }
    }
  };

  thread_ = std::move(std::thread(func));
}

ThreadPool::ThreadPool(const int num_threads) : num_threads_(num_threads) {
  thread_pool_.reserve(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    thread_pool_.push_back(std::make_unique<Worker>(i, this));
  }

  for (const auto &worker : thread_pool_) {
    worker->run();
  }
}

Worker *ThreadPool::getVictim() {
  int low = 0;
  int high = num_threads_ - 1;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dist(low, high);

  int random_num = dist(gen);

  return thread_pool_[random_num].get();
}

ThreadPool::~ThreadPool() {
  stop_source_.request_stop();

  for (auto &worker : thread_pool_) {
    worker->join();
  }
}

const char *TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads), thread_pool_(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  //
  // TODO: CS149 student implementations may decide to perform cleanup
  // operations (such as thread pool shutdown construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
}

void TaskSystemParallelThreadPoolSleeping::submitTaskGroupTasks(
    std::shared_ptr<TaskGroup> &task_group) {
  auto func = [task_group, this](int task_id) {
    task_group->runnable->runTask(task_id, task_group->num_total_tasks);
    int remain_task = task_group->remain_tasks.fetch_sub(1);
    if (remain_task == 1) {
      {
        std::unique_lock<std::mutex> lock(task_group->completed_mutex);
        task_group->completed_cv.notify_one();
      }
    }
  };
  for (int i = 0; i < task_group->num_total_tasks; ++i) {
    // task_set_.insert(i);
    thread_pool_.submitTask(func, i);
  }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Parts A and B.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //

  auto task_group =
      std::make_shared<TaskGroup>(num_total_tasks, num_total_tasks, runnable);

  submitTaskGroupTasks(task_group);
  {
    std::unique_lock<std::mutex> lock(task_group->completed_mutex);
    task_group->completed_cv.wait(
        lock, [&task_group] { return task_group->remain_tasks == 0; });
  }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {

  //
  // TODO: CS149 students will implement this method in Part B.
  //

  return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //

  return;
}
