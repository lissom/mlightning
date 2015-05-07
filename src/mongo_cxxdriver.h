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

#include <memory>
#include <mongo/client/dbclient.h>

namespace mongo {

//TODO: if defs to support linking to different drivers
/*
 * Contains definitions for the C++ driver.  Insulation against changes.
 * We have effectively 3 and soon 4 C++ drivers.  Indirection is a very good thing.
 */
const std::string uriStart = "mongodb://";
using Cursor = std::unique_ptr<mongo::DBClientCursor>;
using Connection = std::unique_ptr<mongo::DBClientConnection>;
inline ConnectionString parseConnectionOrThrow(const std::string &connStr) {
    std::string error;
    ConnectionString mongoConnStr = mongo::ConnectionString::parse(connStr, error);
    if (!error.empty()) {
        std::logic_error("Unable to parse connection string");
    }
    if (!mongoConnStr.isValid())
        throw std::logic_error("Invalid mongo connection string");
    return mongoConnStr;
}

inline std::unique_ptr<DBClientBase> connectOrThrow(const ConnectionString &connStr,
        double socketTimeout = 0) {
    std::string error;

    std::unique_ptr<DBClientBase> dbConn(connStr.connect(error, socketTimeout));
    if (!error.empty()) {
        std::cerr << "Unable to connect to " << connStr.toString() << ": " << error << std::endl;
        throw std::logic_error("Unable to connect to mongodb");
    }
    return dbConn;
}
}  //namespace mongo
