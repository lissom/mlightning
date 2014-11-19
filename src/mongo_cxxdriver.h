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
     * Changes auto_ptr to unique_ptr so mongo plays well with stl
     */
    const std::string uriStart = "mongodb://";
    using Cursor = std::unique_ptr<mongo::DBClientCursor>;
}  //namespace mongo

