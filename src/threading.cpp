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

#include "threading.h"

namespace tools {

    void ThreadPool::_workLoop() {
        MutexUniqueLock lock(_workMutex, std::defer_lock);
        for (;;) {
            lock.lock();
            _workNotify.wait(lock, [this]() {return !this->_workQueue.empty() || this->terminate()
                                    || endWait();});
            if (terminate() || (_workQueue.empty() && endWait())) break;
            ThreadFunction func = std::move(_workQueue.front());
            _workQueue.pop_front();
            lock.unlock();
            func();
        }
    }
}  //namespace tools
