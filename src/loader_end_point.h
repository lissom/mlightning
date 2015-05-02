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

#include "mongo_end_point.h"

namespace loader {

/*
 * Defines the endpoints so they no longer need to be called as templates and are ready for
 * use by the loader
 */
using EndPointHolder = mtools::MongoEndPointHolder<>;
using EndPoint = EndPointHolder::MongoEndPoint;

}  //namespace loader
