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
            OpReturnCode opCheckError(Connection* conn) {
                //TODO: mongo::BSONObj le = GetLastErrorDetailed
                std::string error = conn->getLastError();
                return error.empty() ? OpReturnCode::ok : OpReturnCode::error;
            }
            OpReturnCode opCheckError(const mongo::WriteResult& result) {
                if (!result.hasErrors())
                    return OpReturnCode::ok;
                if (result.hasWriteConcernErrors()) {
                    std::cerr << "Write concern errors:\n";
                    for (auto&& ist : result.writeConcernErrors())
                        std::cerr << tojson(ist) << std::endl;
                    std::cerr << std::endl;
                }
                if (result.hasWriteErrors()) {
                    std::cerr << "Write concern errors:\n";
                    for (auto&& ist : result.writeErrors())
                        std::cerr << tojson(ist) << std::endl;
                    std::cerr << std::endl;                }
                return OpReturnCode::error;
            }
        }

        OpQueueBulkInsertUnorderedv24_0::OpQueueBulkInsertUnorderedv24_0(std::string ns,
                                                               DataQueue* data,
                                                               int flags,
                                                               const WriteConcern* wc) :
                _ns(std::move(ns)), _data(std::move(*data)), _flags(flags), _wc(wc)
        {
        }

        OpReturnCode OpQueueBulkInsertUnorderedv24_0::run(Connection* const conn) {
            conn->insert(_ns, _data, _flags, _wc);
            if (!_wc || !_wc->requiresConfirmation()) return OpReturnCode::ok;
            return opCheckError(conn);
        }

        OpQueueBulkInsertUnorderedv26_0::OpQueueBulkInsertUnorderedv26_0(std::string ns,
                                                                       DataQueue* data,
                                                                       int flags,
                                                                       const WriteConcern* wc) :
                _ns(std::move(ns)), _data(std::move(*data)), _flags(flags), _wc(wc)
        {
        }

        //TODO: move this further up the stack if possible
        OpReturnCode OpQueueBulkInsertUnorderedv26_0::run(Connection* const conn) {
            auto bulker = conn->initializeUnorderedBulkOp(_ns);
            for (auto&& itr: _data)
                bulker.insert(itr);
            bulker.execute(_wc, &_writeResult);
            return opCheckError(_writeResult);
        }

        OpReturnCode OpQueueQueryBulk::run(Connection* const conn) {
            //todo: check out errors more (probably this is as good at it gets with the driver though)
            OpReturnCode result = OpReturnCode::ok;
            //std::cout << _ns << ":" << _query << std::endl;
            auto c = conn->query(_ns, _query, 0, 0, _fieldsToReturn, _queryOptions); // | mongo::QueryOption_Exhaust); //for some reason exhaust generates errors, need to investigate, not sure it's fixable (driver seems to not use exhaust either)
            while (c->more()) {
                mongo::BSONObj obj = c->next();
                if( strcmp(obj.firstElementFieldName(), "$err") == 0 ) {
                    std::string s = "error reading document: " + obj.toString();
                    std::cerr << s << std::endl;
                    result = OpReturnCode::error;
                    break;
                }
                _data.emplace_back(obj.getOwned());
                _size += obj.objsize();
                //Check to see if the batch size is exceeded and there are more docs from the db
                //c-more() prevents any callbacks with zero docs when there were results
                if (_callbackBatchSizeBytes && _data.size() >= _callbackBatchSizeBytes && c->more()) {
                    callback(OpReturnCode::getMore);
                    _data.clear();
                    _size = 0;
                }
            }
            return result;
        }
    } //namespace mtools
}  //namespace tools
