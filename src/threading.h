/*    Copyright Charlie Page 2014
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include <atomic>
#include <assert.h>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <queue>
#include <thread>

namespace tools {

typedef std::mutex Mutex;
typedef std::lock_guard<std::mutex> MutexLockGuard;
typedef std::function<void()> ThreadFunction;
typedef std::condition_variable ConditionVariable;
typedef std::unique_lock<std::mutex> MutexUniqueLock;

//TODO: refactor everything to PIMPL
class ThreadPool;

/**
 * Thread pool worker thread, shouldn't exist outside of threadpool
 */
class ThreadPoolWorker {
public:
    ThreadPoolWorker(ThreadPool& pool) :
            _pool(pool) {
    }

    void operator()();

private:
    ThreadPool &_pool;
};

/**
 * Basic thread management object.
 * Accepts work functions and runs threads against them serially
 * Does not schedule against different functions
 * All work functions must be void fun(void)
 */
class ThreadPool {
public:
    enum class Status { running, ending, terminating };

    ThreadPool(size_t size) :
            _status(Status::running) {
        do {
            _threads.push_back(std::thread(ThreadPoolWorker(*this)));
        } while (--size);
    }

    ~ThreadPool() {
        terminateInitiate();
        //Do not return until all threads are halted, may need a release function
        join();
    }

    /**
     * Enqueues a work function
     */
    void queue(ThreadFunction func) {
        MutexLockGuard lock(_workMutex);
        _workQueue.push_back(func);
        _workNotify.notify_one();
    }

    /**
     * Enqueues a work function numWorkers times
     * If numbers workers is < 0, then maxthreads - numWorkers
     * The function is always queued once
     */
    void queue(ThreadFunction func, int numWorkers) {
        if (numWorkers <= 0) {
            if (numWorkers == 0)
                numWorkers = 1;
            else {
                numWorkers += _threads.size();
                if (numWorkers < 0)
                    numWorkers = 1;
            }
        }
        MutexLockGuard lock(_workMutex);
        for (int i = 0; i < numWorkers; ++i)
            _workQueue.push_back(func);
        _workNotify.notify_all();
    }

    /**
     * Queue this function once for each thread in the queue
     */
    void queueForEach(ThreadFunction func) {
        MutexLockGuard lock(_workMutex);
        for (size_t i = 0; i < _threads.size(); ++i)
            _workQueue.push_back(func);
        _workNotify.notify_all();
    }

    /**
     * Joins all threads.  Does NOT stop them.
     */
    void join() {
        for (auto& thread : _threads)
            if (thread.joinable())
                thread.join();
    }

    /**
     * Get the terminate flag
     */
    bool terminating() {
        return _status == Status::terminating;
    }

    /**
     * Get the endWait flag
     */
    bool ending() {
        return _status >= Status::ending;
    }

    /**
     * Sets the terminate flag
     * must ALWAYS set the _endWait flag too, only _endWait is checked in loops
     */
    void terminateInitiate() {
        if (_status < Status::terminating) {
            _status = Status::terminating;
            _workNotify.notify_all();
        }
    }

    /**
     * Sets the endWait flag
     */
    void endWaitInitiate() {
        if (_status == Status::running) {
            _status = Status::ending;
            _workNotify.notify_all();
        }
    }

    /*
     * Complete processing the queue and join all threads
     */
    void endWaitJoin() {
        endWaitInitiate();
        join();
    }

    /**
     * @return the size of the workQueue.
     */
    size_t queueSize() const {
        MutexLockGuard lock(_workMutex);
        return _workQueue.size();
    }

    /**
     * @return the max number of threads available
     */
    size_t threadsSize() const {
        return _threads.size();
    }

private:
    friend class ThreadPoolWorker;
    void _workLoop();

    std::atomic<Status> _status;

    std::deque<std::thread> _threads;
    mutable Mutex _workMutex;
    mutable ConditionVariable _workNotify;
    std::deque<ThreadFunction> _workQueue;
};

inline void ThreadPoolWorker::operator()() {
    _pool._workLoop();
}

/**
 * Wait queue.  If the queue is empty consumers wait, if it is at the max producers wait.
 *
 * A single signal is used, only consumers or producers should be waiting.
 */
template<typename Value>
class WaitQueue {
public:
    WaitQueue(size_t queueMaxSize) :
            _queueMaxSize(queueMaxSize) {
    }

    /**
     * Puts a value on the queue.
     * If we are at zero values initially notify the consumers
     * To guard against producers being slow notify all is ran on size 2, on size one
     * only notify one is ran.
     */
    void push(Value &&value) {
        MutexUniqueLock lock(_mutex);
        if (!full()) {
            auto queueSize = _queue.size();
            _queue.emplace(value);
            //If there is only one element notify all waiters
            if (queueSize == 0)
                _queueNotify.notify_one();
            else if (queueSize == 1)
                _queueNotify.notify_all();
            return;
        }
        _queueNotify.wait(lock, [this]() {return !this->full();});
        _queue.emplace(value);
    }

    /**
     * Pops a value.  If there are no values forces a wait.
     * If things backup we assume that generally they'll stay that way, so notify one thread
     * however, just in case that isn't the case we notify all threads on 1 away from the limit
     * This should get called on the way up but be harmless (hopefully we don't synchronize
     * around 1 off the limit)
     */
    bool pop(Value& value) {
        MutexUniqueLock lock(_mutex);
        //Wait for the queue to be non-empty, should only break for an empty queue on exit
        _queueNotify.wait(lock, [this]() {return !this->_queue.empty() || _endWait;});
        if (_queue.empty())
            return false;
        value = _queue.front();
        auto tolimit = _queueMaxSize - _queue.size();
        if (tolimit == 0)
            _queueNotify.notify_one();
        else if (tolimit == 1)
            _queueNotify.notify_all();
        _queue.pop();
        return true;
    }

    /**
     * Stop waiting and start returning false on nothing to do
     */
    void endWait() {
        _endWait = true;
        _queueNotify.notify_all();
    }

private:
    mutable Mutex _mutex;
    mutable ConditionVariable _queueNotify;
    size_t _queueMaxSize;
    std::queue<Value> _queue;
    std::atomic_bool _endWait { };bool inline full() const {
        return _queue.size() >= _queueMaxSize;
    }
};

/**
 * Thread safe container that cycles through values
 */
template<typename T, typename H = std::deque<T>>
class RoundRobin {
public:
    using Container = H;
    using Value = T;

    /**
     * Pass initialization args to the container
     */
    template<typename ... Args>
    RoundRobin(Args ...args) :
            _container(args...) {
        init();
    }

    /**
     * Initialize by moving in the container
     */
    RoundRobin(Container &&container) :
            _container(std::forward<Container>(container)) {
        init();
    }

    /**
     * Places the next value into the passed pointer
     * @param pointer to hold the returned value, unchanged if there is nothing to return
     * @return true if there is a value, false if empty
     */
    bool next(Value* ret) const {
        tools::MutexLockGuard lock(_mutex);

        if (_container.empty())
            return false;
        if (++_position == _container.end())
            _position = _container.begin();
        *ret = *_position;
        return true;
    }

    /**
     * Removes all instances of the value if it exists in the RR
     */
    //TODO: Make this more robust wrt to hitting the start again, i.e. dump init
    void remove(const Value& value) {
        tools::MutexLockGuard lock(_mutex);
        _container.erase(std::remove(_container.begin(), _container.end(), value),
                _container.end());
        init();
    }

private:
    /**
     * Container of values to return
     */
    Container _container;
    /**
     * Current position in the container
     */
    typename Container::iterator _position;
    tools::Mutex _mutex;

    void init() {
        _position = _container.begin();
    }
};

}  //namespace tools
