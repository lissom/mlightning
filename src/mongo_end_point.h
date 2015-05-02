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
#include <memory>
#include <unordered_map>
#include "mongo_cxxdriver.h"
#include "mongo_cluster.h"
#include "mongo_operations.h"
#include "threading.h"

namespace mtools {
//todo: pull this out of tools so it is only the mtools namespace

/*
 * The settings for an MongoEndPoint.  The settings aren't a template so they are a separate.
 */
struct MongoEndPointSettings {
    bool startImmediate;bool directLoad;
    size_t maxQueueSize;
    size_t threadCount;
    size_t sleepTime = 10;
};

/**
 * MongoDB end point (i.e. a mongoD or mongoD).
 * There is no associated queue.  That is a template parameter
 * This class handles getting operations from a queue and then using N threads to send the
 * operations to mongo as quickly as possible.
 */
template<typename TOpQueue>
class BasicMongoEndPoint {
public:
    BasicMongoEndPoint(MongoEndPointSettings settings, std::string connStr) :
            _threadPool(settings.threadCount), _opQueue(settings.maxQueueSize), _sleepTime(
                    settings.sleepTime) {
        std::string error;
        _connStr = mongo::ConnectionString::parse(connStr, error);
        if (!error.empty()) {
            std::cerr << "Unable to parse: " << connStr << "\nExiting" << std::endl;
            exit(EXIT_FAILURE);
        }
        assert(settings.threadCount);
        if (settings.startImmediate)
            start();
    }

    ~BasicMongoEndPoint() {
        shutdown();
    }

    /**
     * @return the connection string to this end point
     */
    std::string connection() const {
        return _connStr.toString();
    }

    /**
     * @return are threads active
     */
    bool isRunning() {
        return _threadPool.queueSize();
    }

    /**
     * Starts the threads running for inserts
     * It is desirable to delay this until there is data in the queue if the threads are spinning
     */
    void start() {
        assert(!isRunning());
        _threadPool.queueForEach([this] () {this->run();});
    }

    /**
     * Shutdown the queue after it is cleared
     */
    void gracefulShutdown() {
        _threadPool.endWaitInitiate();
    }

    /**
     * Shutdown the queue after it is cleared and waits for the threads to be joined
     */
    void gracefulShutdownJoin() {
        _opQueue.endWait();
        _threadPool.endWaitInitiate();
        joinAll();
    }

    /**
     * Immediately shutdown the queue
     */
    void shutdown() {
        _threadPool.terminateInitiate();
    }

    /**
     * Wait for all threads to be joined
     * Should NOT be called on it's own, this will NOT stop the threads
     */
    void joinAll() {
        _threadPool.joinAll();
    }

    /**
     * Push onto the thread queue
     */
    bool push(DbOpPointer dbOp) {
        return _opQueue.push(dbOp);
    }

    /**
     * thread work loop
     */
    //todo: change this for locked operations, move out and use specializations for locks
    void run() {
        //dbConn used in exception catching to see what db is connected to
        mongo::DBClientBase* dbConn = nullptr;
        try {
            DbOpPointer currentOp;
            std::string error;
            int retries = 3;
            for (;;) {
                dbConn = _connStr.connect(error);
                if (dbConn || !retries)
                    break;
                --retries;
                sleep(1);
            }
            if (!dbConn) {
                std::cerr << "Unable to connect to: " << _connStr.toString() << "\nError: " << error
                        << "\nExiting" << std::endl;
                exit(EXIT_FAILURE);
            }

            /*
             //Discount the first miss as the loop is probably starting dry
             bool miss = false;
             bool firstmiss = true;
             size_t missCount {};
             */
            while (!_threadPool.terminate()) {
                if (pop(currentOp)) {
                    /* For lockless
                     if (miss) {
                     miss = false;
                     firstmiss = false;
                     std::cout << dbConn->toString() << ": Hitting" << std::endl;
                     }
                     */
                    if (currentOp->execute(dbConn) != OpReturnCode::ok)
                        throw std::logic_error("Insert failed, exiting");
                } else {
                    if (_threadPool.endWait())
                        break;
                    /*
                     //TODO: log levels.  If you are seeing misses std::cout is cheap
                     if (!miss && !firstmiss) {
                     std::cout << dbConn->toString() << ": Missing" << std::endl;
                     }
                     std::this_thread::sleep_for(std::chrono::milliseconds(_sleepTime));
                     miss = true;
                     if (!firstmiss) ++missCount;
                     */
                }
            }
            /*
             if (missCount) std::cout << "Endpoint misses: " << missCount << ".  Slept: "
             << missCount * _sleepTime / 1000 << " seconds"
             << std::endl;
             */
        } catch (mongo::DBException& e) {
            std::cerr << "End point failed: " << dbConn->toString() << ": DBException: " << e.what()
                    << std::endl;
            exit(EXIT_FAILURE);
        } catch (std::exception &e) {
            std::cerr << "End point failed: " << dbConn->toString() << ": std::exception: "
                    << e.what() << std::endl;
            exit(EXIT_FAILURE);
        } catch (...) {
            std::cerr << "End point failed: " << dbConn->toString() << ": Error unknown"
                    << std::endl;
        }

    }

private:
    bool pop(DbOpPointer& dbOp) {
        return _opQueue.pop(dbOp);
    }

    tools::ThreadPool _threadPool;
    mongo::ConnectionString _connStr;
    TOpQueue _opQueue;
    size_t _sleepTime;
};

/**
 * Holds end points to a cluster.
 * All endpoints should either be monogS or mongoD
 *
 * MongoEndPointHolder will get the mongoS from the cluster connection string if direct
 * connections aren't specified.
 */
//template<typename TOpQueue = mtools::OpQueueNoLock>
template<typename TOpQueue = mtools::OpQueueLocking1>
class MongoEndPointHolder {
public:
    using MongoEndPoint = BasicMongoEndPoint<TOpQueue>;
    using MongoEndPointPtr = std::unique_ptr<MongoEndPoint>;
    //Note that ShardName can also be a mongoS, but in that case it doesn't much matter
    using MongoEndPointMap = std::unordered_map<mtools::ShardName,
    MongoEndPointPtr>;

    MongoEndPointHolder(const MongoEndPointSettings &settings, const MongoCluster& mCluster) :
            _started( false), _directLoad(settings.directLoad) {
        if (settings.directLoad) {
            for (auto& shard : mCluster.shards())
                _epm.emplace(
                        std::make_pair(shard.first,
                                MongoEndPointPtr(new MongoEndPoint(settings, shard.second))));
        } else {
            auto servers = mCluster.connStr().getServers();
            for (auto& mongoS : servers) {
                std::string mongosConn = std::string("mongodb://").append(mongoS.toString());
                //todo: error checking to make sure *ip addresses* in the connection string match mongoS in the cluster
                _epm.emplace(
                        std::make_pair(mongosConn,
                                MongoEndPointPtr(new MongoEndPoint(settings, mongosConn))));
            }
        }
        if (!_epm.size()) {
            std::cerr << "No end points were created\nExiting" << std::endl;
            exit(EXIT_FAILURE);
        }
        _cycleItr = _epm.begin();
    }

    /**
     * @return MongoEndPoint for a specific shard/mongoS (though shouldn't need to be called
     * in the mongoS case)
     */
    MongoEndPoint* const at(const mtools::ShardName& shard) {
        assert(_directLoad);
        return _epm.at(shard).get();
    }

    /**
     * Return the number of queues
     */
    size_t size() const {
        return _epm.size();
    }

    /**
     * Hand out end points in a round robin, use for mongoS
     */
    MongoEndPoint* const getMongoSCycle() {
        assert(!_directLoad);
        tools::MutexLockGuard lock(_cycleMutex);
        ++_cycleItr;
        if (_cycleItr == _epm.end())
            _cycleItr = _epm.begin();
        return _cycleItr->second.get();
    }

    /**
     * Are the end points active?
     */
    bool running() const {
        return _started;
    }

    /**
     * Start up all end points
     * It is desirable to delay this while there is no data if the end points don't wait
     */
    void start() {
        _started = true;
        for (auto&& i : _epm)
            i.second->start();
    }

    /**
     * Have all of the end points shutdown when their queues are cleared, join those threads.
     */
    void gracefulShutdownJoin() {
        for (auto&& ep : _epm)
            ep.second->gracefulShutdownJoin();
    }

    const bool directLoad() const {
        return _directLoad;
    }

private:
    tools::Mutex _cycleMutex;
    typename MongoEndPointMap::iterator _cycleItr;
    MongoEndPointMap _epm;bool _started;bool _directLoad;
};
}  //namespace mtools
