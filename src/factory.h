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

#include <assert.h>
#include <unordered_map>
#include <type_traits>
#include <memory>
#include <string>

namespace tools {
/*
 * Factory template
 * This factor template is never meant to be concrete
 */
template<typename ObjectPtr, typename Factory, typename Key, typename Map>
class RegisterFactoryImpl {
    using Container = Map;

    static Container& getMap() {
        static Container container;
        return container;
    }

public:
    /**
     * Registers the function to create the object.
     * If that behavior is desired, create an unregister function
     *
     * @return to ensure that static bools can be used with this function to setup the factory
     */
    static bool registerCreator(Key&& key, Factory&& factory) {
        assert(!key.empty());
        bool result = getMap().insert(std::make_pair(key, std::forward<Factory>(factory))).second;
        //ensure it doesn't already exist to avoid double inserts
        assert(result);
        return true;
    }

    /**
     * Returns a nearly created object
     * Throws if the key cannot be found
     */
    template<typename ... Args>
    static ObjectPtr createObject(const Key& key, Args ... args) {
        return getMap().at(key)(args...);
    }

    static std::string getKeysPretty() {
        std::string keys;
        bool first = true;
        for (auto& i : getMap()) {
            if (!first)
                keys += ", \"" + std::string(i.first) + "\"";
            else {
                keys = "\"" + std::string(i.first) + "\"";
                first = false;
            }
        }
        return keys;
    }

    static bool verifyKey(const Key& key) {
        return (getMap().find(key) != getMap().end());
    }
};

template<typename ObjectPtr, typename Factory, typename Key = std::string,
        typename Map = std::unordered_map<Key, Factory>> using
RegisterFactory = RegisterFactoryImpl<ObjectPtr, Factory, Key, Map>;

}  //namespace tools

