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

#include <boost/lockfree/queue.hpp>
#include "mongo_cxxdriver.h"
#include "threading.h"

/*
 * virtual inline ... final
 *
 * Can the compiler do whole program optimization and realize that virual isn't necessary?
 */
namespace tools {
    namespace mtools {
        /*
         * The eventual idea here is to be able to track operations success etc.
         * The functors should allow for an easy retry method etc.
         */
        using WriteConcern = mongo::WriteConcern;
        constexpr auto* DEFAULT_WRITE_CONCERN = &WriteConcern::majority;
        using OpReturnCode = bool;
        using Data = mongo::BSONObj;
        using DataQueue = std::vector<Data>;
        using Connection = mongo::DBClientBase;
        /**
         * Public interface for a database operation
         * Base database operation
         * Holds everything required to run the operation against the database
         */
        struct DbOp {
        public:
            //TODO: use return codes for real async.  Just a function, keep empty base class.
            DbOp() {
            }
            virtual ~DbOp() {
            }
            /**
             * Executes the operation against the database connection
             */
            virtual OpReturnCode run(Connection* conn) = 0;
        };
        using DbOpPointer = std::unique_ptr<DbOp>;

        /**
         * Public interface for a queue of database operations
         */
        class OpQueue {
        public:
            OpQueue() {
            }
            virtual ~OpQueue() {
            }

            virtual OpReturnCode push(DbOpPointer& dbOp) = 0;
            virtual OpReturnCode pop(DbOpPointer& dbOp) = 0;
            /**
             * Called when the queue should exit with no more work to do
             */
            virtual void endWait() = 0;
        };

        /**
         * Lockfree implementation of the OpQueue
         */
        class OpQueueNoLock : public OpQueue {
        public:

            OpQueueNoLock(size_t queueSize) :
                    _queue(queueSize)
            {
            }
            virtual ~OpQueueNoLock() final;

            virtual inline OpReturnCode push(DbOpPointer& dbOp) final {
                return _queue.push(dbOp.release());
            }

            virtual inline OpReturnCode pop(DbOpPointer& dbOp) final {
                DbOp* rawptr;
                bool result = _queue.pop(rawptr);
                if (result) dbOp.reset(rawptr);
                return result;
            }

            //Nothing to do here.
            virtual inline void endWait() final {}

        private:
            boost::lockfree::queue<DbOp*> _queue;
        };

        /**
         *  Blocks on full/no work
         */
        class OpQueueLocking1 : public OpQueue {
        public:

            OpQueueLocking1(size_t queueSize) :
                    _queue(queueSize)
            {
            }
            virtual ~OpQueueLocking1() final;

            virtual inline OpReturnCode push(DbOpPointer& dbOp) final {
                _queue.push(dbOp.release());
                return true;
            }

            virtual inline OpReturnCode pop(DbOpPointer& dbOp) final {
                DbOp* rawptr;
                bool result = _queue.pop(rawptr);
                if (result) dbOp.reset(rawptr);
                return result;
            }

            virtual inline void endWait() final { _queue.endWait(); }

        private:
            tools::WaitQueue<DbOp*> _queue;
        };

        /**
         * Bulk insert operation.  Unordered.
         */
        struct OpQueueBulkInsertUnordered : public DbOp {
            OpQueueBulkInsertUnordered(std::string ns,
                                       DataQueue* data,
                                       int flags = 0,
                                       const WriteConcern* wc = DEFAULT_WRITE_CONCERN);
            OpReturnCode run(Connection* conn);
            std::string _ns;
            DataQueue _data;
            int _flags;
            const WriteConcern* _wc;

            static DbOpPointer make(std::string ns,
                                    DataQueue* data,
                                    int flags = 0,
                                    const WriteConcern* wc = DEFAULT_WRITE_CONCERN)
            {
                return DbOpPointer(new OpQueueBulkInsertUnordered(ns, data, flags, wc));
            }
        };
    }  //namespace mtools
}  //namespace tools
