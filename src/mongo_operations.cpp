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

#include "mongo_operations.h"

namespace tools {
    namespace mtools {
        OpQueueNoLock::~OpQueueNoLock() {
            //defensive: clear the queue so that pointers are deleted
            DbOpPointer dbOp;
            while (pop(dbOp))
                ;
        }

        OpQueueLocking1::~OpQueueLocking1() {
            //defensive: clear the queue so that pointers are deleted
            DbOpPointer dbOp;
            while (pop(dbOp))
                ;
        }

        namespace {
            //TODO: change error code impl to inspect and handle different codes
            OpReturnCode CheckConnError(Connection* conn) {
                //TODO: mongo::BSONObj le = GetLastErrorDetailed
                std::string error = conn->getLastError();
                if (!error.empty()) {
                    std::cerr << error << std::endl;
                    throw std::logic_error("Write failed");
                }
                return true;
            }
        }

        OpQueueBulkInsertUnordered::OpQueueBulkInsertUnordered(std::string ns,
                                                               DataQueue* data,
                                                               int flags,
                                                               const WriteConcern* wc) :
                _ns(std::move(ns)), _data(std::move(*data)), _flags(flags), _wc(wc)
        {
        }

        OpReturnCode OpQueueBulkInsertUnordered::run(Connection* conn) {
            conn->insert(_ns, _data, _flags, _wc);
            if (!_wc || !_wc->requiresConfirmation()) return true;
            return CheckConnError(conn);
        }
    }
}  //namespace mtools
