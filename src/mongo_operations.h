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
        //todo: change to using enum for return codes
        using OpReturnCode = bool;
        using Data = mongo::BSONObj;
        using DataQueue = std::vector<Data>;
        using Connection = mongo::DBClientBase;
        /**
         * Public interface for a database operation
         * Base database operation
         * Holds everything required to run the operation against the database
         * The empty callback function avoids an if statement and branch prediction misses
         */
        struct DbOp {
        public:
            using callBackFun = std::function<void(DbOp &, OpReturnCode)>;
            DbOp(callBackFun callBack = emptyCallBack) : _callBack(callBack = emptyCallBack) {};
            virtual ~DbOp() {}
            /**
             * Executes the operation against the database connection
             * Does callback on completion
             */
            OpReturnCode execute(Connection* const conn) {
                OpReturnCode retVal = run(conn);
                _callBack(*this, retVal);
                return retVal;
            }

            static void emptyCallBack(DbOp &, OpReturnCode) {};

            callBackFun _callBack;

        protected:
            virtual OpReturnCode run(Connection* const conn) = 0;
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
                    _queue(queueSize) {}
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
        struct OpQueueBulkInsertUnorderedv24_0 : public DbOp {
            OpQueueBulkInsertUnorderedv24_0(std::string ns,
                                       DataQueue* data,
                                       int flags = 0,
                                       const WriteConcern* wc = DEFAULT_WRITE_CONCERN);
            std::string _ns;
            DataQueue _data;
            int _flags;
            const WriteConcern* _wc;

            static DbOpPointer make(std::string ns,
                                    DataQueue* data,
                                    int flags = 0,
                                    const WriteConcern* wc = DEFAULT_WRITE_CONCERN)
            {
                return DbOpPointer(new OpQueueBulkInsertUnorderedv24_0(ns, data, flags, wc));
            }

        protected:
            OpReturnCode run(Connection* const conn);
        };

        struct OpQueueBulkInsertUnorderedv26_0 : public DbOp {
            OpQueueBulkInsertUnorderedv26_0(std::string ns,
                                       DataQueue* data,
                                       int flags = 0,
                                       const WriteConcern* wc = DEFAULT_WRITE_CONCERN);
            std::string _ns;
            DataQueue _data;
            int _flags;
            const WriteConcern* _wc;
            mongo::WriteResult _writeResult;

            static DbOpPointer make(std::string ns,
                                    DataQueue* data,
                                    int flags = 0,
                                    const WriteConcern* wc = DEFAULT_WRITE_CONCERN)
            {
                return DbOpPointer(new OpQueueBulkInsertUnorderedv26_0(ns, data, flags, wc));
            }

        protected:
            OpReturnCode run(Connection* const conn);
        };

        /**
         * Performs a query.
         * All data will be retrieved then the callback is issued, so this should only be used when
         * the size of the result set is known to be constrained.
         */
        struct OpQueueQueryBulk : public DbOp {
            OpQueueQueryBulk(callBackFun callBack,
                            const std::string ns,
                            const mongo::Query query,
                            const mongo::BSONObj* const fieldsToReturn,
                            const int queryOptions) :
                            DbOp(callBack),
                            _ns(ns),
                            _query(query),
                            _fieldsToReturn(fieldsToReturn),
                            _queryOptions(queryOptions) {}

            static DbOpPointer make(callBackFun callBack,
                                    const std::string& ns,
                                    const mongo::Query query,
                                    const mongo::BSONObj* const fieldsToReturn = 0,
                                    int queryOptions = 0)
            {
                return DbOpPointer(new OpQueueQueryBulk(callBack, ns, query, fieldsToReturn, queryOptions));
            }

            std::deque<mongo::BSONObj>& data() { return _data; }
            long long size() { return _data.size(); }

            const std::string _ns;
            const mongo::Query _query;
            const mongo::BSONObj* const _fieldsToReturn;
            const int _queryOptions;

            mongo::Cursor _cursor;
            mongo::Connection _connection;
            std::deque<mongo::BSONObj> _data;

        protected:
            OpReturnCode run(Connection* const conn);

        private:
            void enqueue(const mongo::BSONObj &obj) { _data.emplace_back(obj.getOwned()); }
        };

    }  //namespace mtools
}  //namespace tools
