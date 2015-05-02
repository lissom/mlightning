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
#include "mongo_cxxdriver.h"

namespace tools {

//Depends on the same in mongo.  Ordering.h lines 67 (no constant defined...)
constexpr int MAX_ORDER_KEYS = 31;

/**
 * Prints out the ordering object information
 */
inline std::ostream& operator<<(std::ostream& o, mongo::Ordering& rhs) {
    mongo::StringBuilder buf;
    for (unsigned i = 0; i < MAX_ORDER_KEYS; i++)
        buf.append(rhs.get(i) > 0 ? "+" : "-");
    o << buf.str();
    return o;
}

/**
 * wraps mongo::BSONObjCmp to insulate against changes
 * Also makes it work for indexing used in index.h.
 * The functions for std::sort are defined.
 */
class BsonCompare {
public:
    explicit BsonCompare(const mongo::BSONObj& obj) :
            _order(mongo::Ordering::make(obj)) {
    }
    explicit BsonCompare(mongo::Ordering order) :
            _order(std::move(order)) {
    }
    bool operator()(const mongo::BSONObj& l, const mongo::BSONObj& r) const {
        return l.woCompare(r, _order, false) < 0;
    }

    mongo::Ordering ordering() const {
        return _order;
    }

    operator std::string() const {
        mongo::StringBuilder buf;
        for (unsigned i = 0; i < MAX_ORDER_KEYS; i++)
            buf.append(_order.get(i) > 0 ? "+" : "-");
        return buf.str();
    }

    friend std::ostream& operator<<(std::ostream& o, BsonCompare& rhs) {
        o << std::string(rhs);
        return o;
    }

private:
    const mongo::Ordering _order;
};

class BsonCompareDbg {
public:
    using KeyType = mongo::BSONObj;

    BsonCompareDbg(mongo::BSONObj obj) :
            _ordering(mongo::Ordering::make(obj)) {
    }
    BsonCompareDbg(mongo::Ordering o) :
            _ordering(std::move(o)) {
    }
    bool operator()(const mongo::BSONObj& l, const mongo::BSONObj& r) {
        std::cerr << l.jsonString(mongo::JsonStringFormat::TenGen, true, false) << "::"
                << r.jsonString(mongo::JsonStringFormat::TenGen, true, false) << std::endl;
        bool j = l.firstElement().Long() < r.firstElement().Long();
        bool v = l.woCompare(r, _ordering, false) < 0;
        if (j != v)
            assert(j == v);
        return v;
    }

    mongo::Ordering ordering() const {
        return _ordering;
    }

    friend std::ostream& operator<<(std::ostream& o, BsonCompareDbg& rhs) {
        //o << rhs.ordering();
        return o;
    }

private:
    const mongo::Ordering _ordering;
};

}  //namespace tools
