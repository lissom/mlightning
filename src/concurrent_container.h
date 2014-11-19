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
     * BasicConcurrentQueue is a thread safe queue.  It sorts on demand.
     * There is a method to unsafely access it so that different sorting can be used.
     *
     * This also supports the concept of max size and waiting for and notifying on it
     * Max size is lazily set to converge (that means it's safe with Intel basically) on RISC
     * you may get differing sizes, it shouldn't matter as long as that isn't going to break
     * things, in the code here it shoudln't.
     *
     * Max size is* not* enforced, the users need to call the checking functions if that is desired.
     */
    template<typename _Tp, template<typename, typename > class _Tc>
    class BasicConcurrentQueue {
    public:
        typedef _Tp value_type;
        typedef value_type ValueType;
        typedef _Tc<_Tp, std::allocator<_Tp>> ContainerType;

        //TODO: check for move or copy construct and do w/e can be done.
        static_assert(std::is_constructible<value_type, typename ContainerType::value_type>::value,
                "Cannot copy construct return value from container value.");

        BasicConcurrentQueue() :
                _mutex(new Mutex), _sizeMaxNotify(new ConditionVariable)
        {
        }

        void swap(ContainerType& from) {
            MutexLockGuard lock(*_mutex);
            _container.swap(from);
        }

        bool pop(value_type& ret) {
            MutexLockGuard lock(*_mutex);
            if (_container.empty()) return false;
            ret = (std::move(_container.front()));
            _container.pop_front();
            _sizeMaxNotify->notify_one();
            return true;
        }

        void push(value_type value) {
            MutexLockGuard lock(*_mutex);
            _container.push_back(std::move(value));
        }

        /*
         * push and get the containers size
         */
        size_t pushGetSize(value_type value) {
            MutexLockGuard lock(*_mutex);
            _container.push_back(std::move(value));
            return _container.size();
        }

        /**
         * Push to the queue and check to see if the max size is reached, wait for a pull if not
         * and then push.
         */
        void pushCheckMax(value_type value) {
            MutexUniqueLock lock(*_mutex);
            _sizeMaxNotify->wait(lock, [this]()
            {   return this->_sizeMax && (this->_container.size() >
                        this->_sizeMax);});
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

        size_t size() const {
            MutexLockGuard lock(*_mutex);
            return _container.size();
        }

        bool empty() const {
            return size() == 0;
        }

        /**
         * Changes the max size for waiting.
         */
        void sizeMaxSet(size_t sizeMax) {
            bool doNotify = _sizeMax && (sizeMax > _sizeMax);
            _sizeMax = sizeMax;
            if (doNotify) _sizeMaxNotify->notify_all();
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
            for (auto&& itr: u)
                _container.emplace_back(itr);
        }

        /*
         * Sorts using compare.
         */
        template<typename Cmp = std::less<_Tp>>
        void sort(Cmp compare) {
            MutexLockGuard lock(*_mutex);
            std::sort(_container.begin(), _container.end(), compare);
        }

        /*
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
        mutable std::unique_ptr<ConditionVariable> _sizeMaxNotify;
        volatile size_t _sizeMax {};
    };

    template<typename Value, template<typename, typename > class Container = std::deque> using ConcurrentQueue = BasicConcurrentQueue<Value, Container>;

}  //namespace tools
