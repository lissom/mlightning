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

#include <algorithm>
#include <chrono>
#include <deque>
#include <memory>
#include <string>
#ifdef __linux__
#include <unistd.h>
#endif
#ifdef __WIN32__
#include <windows.h>
#endif

namespace tools {

#ifdef __linux__
inline size_t getTotalSystemMemory() {
    long pages = sysconf( _SC_PHYS_PAGES);
    long page_size = sysconf( _SC_PAGE_SIZE);
    return pages * page_size;
}
#endif

//TODO: rest of windows support
#ifdef __WIN32__
#include <windows.h>
inline size_t getTotalSystemMemory()
{
    MEMORYSTATUSEX status;
    status.dwLength = sizeof(status);
    GlobalMemoryStatusEx(&status);
    return status.ullTotalPhys;
}
#endif

template <typename Key, typename Value>
struct SortType {
    Key key;
    Value value;

    bool operator<(const SortType<Key, Value>& rhs) {
        return key < rhs.key;;
    }
};

/**
 * Sfinae true/false
 */

/*
 * Checks if a class tree has a shift left stream operator, e.g. <<
 */
/*
 * Good for detecting functions that should be void, but have default arguments...
 */
#define OBJECT_HAS_FUNCTION(traitName, funcName) \
template<typename T> \
class traitName { \
    template<typename U> static std::true_type check(decltype(&U::funcName)); \
    template<typename U> static std::false_type check(...); \
public: \
    static constexpr bool value = decltype(check<T>(0))::value; \
};

OBJECT_HAS_FUNCTION(HasAnyFooFunc, foo)
struct HasFoo { void foo(int, int); };
struct StaticHasFoo { static void foo(int, int); };
struct NoFoo { };
static_assert(HasAnyFooFunc<HasFoo>::value, "Helper failed to detect foo() exists");
static_assert(HasAnyFooFunc<StaticHasFoo>::value, "Helper failed to detect static foo() exist");
static_assert(!HasAnyFooFunc<NoFoo>::value, "Helper failed to detect foo() doesn't exist");

/*
 * Detects if an object has a signature.
 * Only visible functions can be detected
 * For instance, to detect if an object has a toString function:
 * class MyObj { public: std::string toString(); };
 * OBJECT_HAS_FUNCTION_SIGNATURE(HasToString, toString, std::string, void)
 */
#define OBJECT_HAS_FUNCTION_SIGNATURE(traitName, funcName, funcRet, args...) \
template<typename T> \
class traitName { \
    template<typename U, U> struct helper; \
    template<typename U> static std::true_type check(helper<funcRet(U::*)(args), &U::funcName>*); \
    template<typename U> static std::false_type check(...); \
public: \
    static constexpr bool value = decltype(check<T>(0))::value; \
};


OBJECT_HAS_FUNCTION_SIGNATURE(HasToStringFunc, toString, std::string, void)
struct HasToString { std::string toString(); };
struct StaticHasToString { static std::string toString(); };
struct NoHasToString { };

static_assert(HasToStringFunc<HasToString>::value, "Helper failed to detect HasToString() exists");
static_assert(!HasToStringFunc<StaticHasToString>::value, "Helper failed to detect non-static HasToString() doesn't exist");
static_assert(!HasToStringFunc<NoHasToString>::value, "Helper failed to detect HasToString() doesn't exist");

OBJECT_HAS_FUNCTION_SIGNATURE(HasFooFunc, foo, void, int, int)

static_assert(HasFooFunc<HasFoo>::value, "Helper failed to detect foo() exists");
static_assert(!HasFooFunc<StaticHasFoo>::value, "Helper failed to detect non-static foo() doesn't exist");
static_assert(!HasFooFunc<NoFoo>::value, "Helper failed to detect foo() doesn't exist");

/**
 * Simple timer
 */
template<typename T = std::chrono::high_resolution_clock>
class SimpleTimer {
public:
    using TimePoint = typename T::time_point;

    TimePoint startTime, endTime;

    SimpleTimer() {
        start();
    }

    void start() {
        startTime = T::now();
    }
    void stop() {
        endTime = T::now();
    }
    long seconds() {
        return std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime).count();
    }

    long nanos() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
    }

    long millis() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
    }

    template<typename Td = std::chrono::nanoseconds>
    Td duration() {
        return std::chrono::duration_cast<Td>(endTime - startTime);
    }
};

/**
 * Lap timer
 */
template<typename T = std::chrono::high_resolution_clock,
        template<typename, typename > class Tc = std::deque>
class LapTimer {
public:
    using TimePoint = typename T::time_point;
    using ContainerType = Tc<T, std::allocator<T>>;

    TimePoint checkTime;
    ContainerType laps;

    void start() {
        checkTime = T::now();
    }

    void lap() {
        TimePoint end = T::now();
        laps.emplace_back(end - checkTime);
        checkTime = end;
    }

    void reset() {
        laps.clear();
        start();
    }

    void reserver(size_t size) {
        laps.reserve(size);
    }
    //template < typename U = std::chrono::nanoseconds >
    long duration() {
        TimePoint total = std::accumulate(std::begin(laps), std::end(laps), TimePoint { });
        return std::chrono::duration_cast<std::chrono::nanoseconds>(total).count();
    }

    //template < typename U = std::chrono::nanoseconds>
    unsigned long long avg() {
        if (laps.empty())
            return 0;
        return duration() / laps.size();
        //return duration<U>() / laps.size();
    }

};

/**
 * Event timer.  Stores an event along with the time it occured
 */
template<typename E>
class EventTimer {
public:
    using EventTime = std::tuple<std::chrono::high_resolution_clock::time_point, E>;
    std::deque<EventTime> events;

    void insertEvent(E event) {
        EventTime inter = EventTime(std::chrono::high_resolution_clock::now(), event);
        events.push_back(inter);
    }
};

/*
 * Arg 2 is the ceiling.  If Arg1 exceeds the ceiling the ceiling is returned using Arg1's type
 */
template<typename T, typename U>
T SetCeiling(const T& t, const U& u) {
    return t > u ? T(u) : t;
}

/*
 * Disk reference types
 */
using LogicalDiskMapping = std::vector<std::string>;

/*
 * Logical segments
 * end of 0 means run to the end
 */
struct LocSegment {
    std::string file;
    long long begin;
    long long end;

    LocSegment(std::string file_ = "", size_t begin_ = 0, size_t end_ = 0) :
            file(std::move(file_)), begin(begin_), end(end_) {
    }

    /**
     * Compare two segment objects
     * return < 0, 0, > 0
     * < 0 if ordered before
     * 0 if ordered equally
     * > 0 if ordered after
     */
    int compare(const LocSegment& rhs) const {
        int order = file.compare(rhs.file);
        //begin/end are 64 bit so we need to do comparisons
        if (!order) {
            if (begin < rhs.begin)
                order = -1;
            else if (begin > rhs.begin)
                order = 1;
            else {
                if (end < rhs.end)
                    order = -1;
                else if (end > rhs.end)
                    order = 1;
                else
                    order = 0;
            }
        }
        return order;
    }

    bool operator==(const LocSegment& rhs) const {
        return compare(rhs) == 0;
    }

    bool operator<(const LocSegment rhs) const {
        return compare(rhs) < 0;
    }
};

struct fileinfo {
    std::string name;
    size_t size;

    fileinfo(std::string name_, size_t size_) :
            name(std::move(name_)), size(size_) {
    }

    bool compare(const fileinfo &rhs) const {
        return name.compare(rhs.name);
    }

    bool operator<(const fileinfo &rhs) const {
        return compare(rhs) < 0;
    }
};

using LocSegMapping = std::vector<tools::LocSegment>;
using LogicalLoc = size_t;

/*
 * DocLoc
 * location = logical location
 * start = start byte, inclusive
 * end = end byte, exclusive
 * [start,end)
 *
 *
 */
struct DocLoc {
    static_assert(sizeof(std::streamsize) >= sizeof(std::int64_t), "mload assumes that std::streamsize is at least 64 bits so it can handle any files size");
    LogicalLoc location;
    std::streamsize start;
    //Max bson size is currently 16Megs so int covers this easily.
    int length;
};

template<typename Tp, typename ... Args>
inline std::unique_ptr<Tp> make_unique(Args ...args) {
    return std::unique_ptr<Tp>(new Tp(std::forward<Args>(args)...));
}
}  //namespace tools
