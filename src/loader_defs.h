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

#include <deque>
#include <vector>
#include "mongo_cxxdriver.h"

namespace loader {
    using Bson = mongo::BSONObj;
    using BsonV = std::vector<mongo::BSONObj>;
    using BsonQ = std::deque<mongo::BSONObj>;
    using BsonPairDeque = std::deque<std::pair<mongo::BSONObj, mongo::BSONObj>>;
    using LoadQueues = std::vector<std::string>;

    /*
     * These enums are written to the file system, the value/ type cannot be safely changed
     */
    enum class Compression : int8_t { none = 0, snappy = 1 };
    enum class FileChunkHeader : int8_t { data = 0 };
    /*
     * end file system enums
     */

    /*
     * Output Types
     */
    const char OUTPUT_FILE[] = "file";
    const char OUTPUT_MONGO[] = "mongo";
    /*
     * Input types
     */
    /*extern const char MONGO_CLUSTER_INPUT[];
    extern const char JSON_INPUT[];
    extern const char BSON_INPUT[];*/
    const char INPUT_MONGO[] = "mongo";
    const char INPUT_JSON[] = "json";
    const char INPUT_BSON[] = "bson";
    //covers all formats that mLightning creates
    const char INPUT_MLTN[] = "mlgtn";
}  //namespace loader


