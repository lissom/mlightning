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

#include <algorithm>
#include <deque>
#include <functional>
#include <type_traits>
#include "threading.h"

namespace tools {

    /**
     * BasicConcurrentQueue is a thread safe queue.
     * It sorts on demand.  There is a method to unsafely access for different sorting.
     */
    template<typename _Tp, template<typename, typename> class _Tc>
    class BasicConcurrentQueue {
    public:
        using ValueType = _Tp;
        using ContainerType = _Tc<_Tp, std::allocator<_Tp>>;

        BasicConcurrentQueue() :
                _mutex(new Mutex), _maxSizeNotify(new ConditionVariable) { }

        /**
         * Swaps the container with the one given.
         * @from the container to swap from.  from must be safe for writes.
         */
        void swap(ContainerType& from) noexcept {
            MutexLockGuard lock(*_mutex);
            _container.swap(from);
        }

        /**
         * Returns a value form the container.  As the return is a move if destruction is expensive
         * it's good hygiene to pass something empty or otherwise in a cheap state to destroy as
         * the destruction takes place inside the lock.
         *
         * @param ret This value has the value in the container moved it
         * @return Returns true if there is a value to return
         */
        bool pop(ValueType& ret) noexcept {
            MutexLockGuard lock(*_mutex);
            if (_container.empty()) return false;
            ret = (std::move(_container.front()));
            _container.pop_front();
            _maxSizeNotify->notify_one();
            return true;
        }

        /**
         * Push a value onto the container
         */
        void push(ValueType&& value) {
            MutexLockGuard lock(*_mutex);
            _container.push_back(std::forward<ValueType>(value));
        }

        /*
         * Push and get the containers size
         * @return the size() of the underlying container
         */
        size_t pushGetSize(ValueType&& value) {
            MutexLockGuard lock(*_mutex);
            _container.push_back(std::forward(value));
            return _container.size();
        }

        /**
         * Push to the queue and check to see if the max size is reached, wait for a pull if not
         * and then push.
         */
        void pushCheckMax(ValueType value) {
            MutexUniqueLock lock(*_mutex);
            _maxSizeNotify->wait(lock, [this]()
            {   return this->_maxSize && (this->_container.size() >
                        this->_maxSize);});
            _container.push_back(std::move(value));
        }

        /**
         * Takes a container and then pushes onto it using push_back
         * This does NOT clear the contain prior to pushed
         * @return true if there are any entries added to the container.
         */
        template<typename U>
        bool popToPushBack(U* ret, size_t count = 10) {
            MutexLockGuard lock(*_mutex);
            if (_container.size() < count) {
                if (_container.empty()) return false;
                count = _container.size();
            }

            auto stop = _container.begin();
            std::advance(stop, count);
            for (auto i = _container.begin(); i != stop; ++i)
                ret->push_back(std::move(*i));
            _container.erase(_container.begin(), stop);
            return true;
        }

        /**
         * @return size from the underlying container
         */
        size_t size() const noexcept {
            MutexLockGuard lock(*_mutex);
            return _container.size();
        }

        /**
         * @return True if .size() is zero
         */
        bool empty() const noexcept {
            //Call to size locks
            return size() == 0;
        }

        /**
         * Changes the max size for waiting.
         * If the new size max will open more queue slots then release that number of threads
         * If the new size max is smaller, then the queue has to drain naturally
         */
        void setSizeMax(size_t sizeMax) noexcept {
            MutexLockGuard lock(*_mutex);
            size_t doNotify = _maxSize && (sizeMax > _maxSize) ? sizeMax - _maxSize : 0;
            _maxSize = sizeMax;
            for(; doNotify; --doNotify) _maxSizeNotify->notify_one();
        }

        /**
         * Uses std::move to place each item from the iterators into the container.
         */
        template<typename InputIterator>
        void moveIn(InputIterator first, InputIterator last) {
            if (first == last) return;
            MutexLockGuard lock(*_mutex);
            do {
                _container.emplace_back(std::move(*first));
            }
            while (++first != last);
        }

        /**
         * Uses std::move to place each item from the input container in the queue's container.
         */
        template<typename U>
        void moveIn(U* u) {
            moveIn(u->begin(), u->end());
        }

        /**
         * Uses std::move to place each item from the input container in the queue's container.
         */
        template<typename U>
        void copy(U* u) {
            MutexLockGuard lock(*_mutex);
            for (auto&& itr: *u)
                _container.emplace_back(itr);
        }

        /**
         * Sorts using compare.
         * @param compare the comparison function to sort with
         */
        template<typename Cmp = std::less<_Tp>>
        void sort(Cmp compare) {
            MutexLockGuard lock(*_mutex);
            std::sort(_container.begin(), _container.end(), compare);
        }

        /**
         * Returns the underlying container.
         * NOT THREAD SAFE
         */
        ContainerType& unSafeAccess() {
            return _container;
        }

    private:
        using iterator = typename ContainerType::iterator;
        using reverse_iterator = std::reverse_iterator<iterator>;

        ContainerType _container;
        mutable std::unique_ptr<Mutex> _mutex;
        mutable std::unique_ptr<ConditionVariable> _maxSizeNotify;
        size_t _maxSize{};
    };

    template<typename Value, template<typename, typename > class Container = std::deque> using ConcurrentQueue = BasicConcurrentQueue<Value, Container>;

}  //namespace tools
