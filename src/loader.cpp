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

#include "loader.h"
#include <algorithm>
#include <exception>
#include <iostream>
#include <tuple>
#include "input_processor.h"
#include "input_types.h"
#include "mongo_cxxdriver.h"

/*
 * Strategy ideas:
 * Testing so far has shown that direct load is great when mongoD contains 15M docs or less.
 * The disks throughput appears to be completely occupied reading (spiking to 100% util with 2 RAID-0 SSD
 * Cycle sort looks like it might be an option (I think merge sort is going to be too costly in terms of disk use)
 * Also looking for back pressure on the end point queues (atomic size_t?) so we only load the load the lower chunk ranges first
 */
//TODO: Allow for secondary sort key outside of the shard key
//TODO: Support replicas as single member shards
namespace loader {

    void Loader::Settings::process() {
        try {
            output.validate();
        }
        catch (std::exception &e) {
            std::cerr << "Unable to validate output cluster: " << e.what() << std::endl;
            exit(EXIT_FAILURE);
        }

        //If the input type is a cluster there is no directory given, validate cluster input
        if (inputType == MONGO_CLUSTER_INPUT || loadPath.empty()) {
            try {
                input.validate();
                if (loadPath.empty())
                    inputType = MONGO_CLUSTER_INPUT;
            }
            catch (std::exception &e) {
                if (loadPath.empty())
                    std::cerr << "No load path\n";
                std::cerr << "Unable to validate input cluster: " << e.what() << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        output.endPoints.startImmediate = false;
        input.endPoints.startImmediate = true;
        indexHas_id = false;
        indexPos_id = size_t(-1);
        size_t count {};
        if(sharded) {
            if (shardKeyJson.empty()) {
                std::cerr << "No shard key for sharded setup" << std::endl;
                exit(EXIT_FAILURE);
            }
            shardKeysBson = mongo::fromjson(shardKeyJson);
            for (mongo::BSONObj::iterator i(shardKeysBson); i.more();) {
                mongo::BSONElement key = i.next();
                if (key.valueStringData() == std::string("hashed")) hashed = true;
                else if (key.Int() != 1 && key.Int() != -1) {
                    std::cerr << "Unknown value for key: " << key << "\nValues are 1, -1, hashed"
                              << std::endl;
                    exit(EXIT_FAILURE);
                }
                shardKeyFields.push_back(key.fieldName());
                if (!indexHas_id && key.fieldNameStringData().toString() == "_id") {
                    indexHas_id = true;
                    indexPos_id = count;
                }
                ++count;
            }
            if (hashed && count > 1) {
                std::cerr << "MongoDB currently only supports hashing of a single field"
                          << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        if (!indexHas_id) add_id = false;
        dispatchSettings.sortIndex = shardKeysBson;
        batcherSettings.sortIndex = shardKeysBson;

        dispatchSettings.workPath = workPath;
        dispatchSettings.directLoad = output.endPoints.directLoad;

        loadQueueBson = mongo::fromjson(loadQueueJson);
        for (mongo::BSONObj::iterator i(loadQueueBson); i.more();) {
            mongo::BSONElement load = i.next();
            if (!loader::docbuilder::ChunkBatchFactory::verifyKey(load.fieldName())) {
                std::cerr << "No such queue type: " << load.fieldName() << std::endl;
                exit(EXIT_FAILURE);
            }
            if(!load.isNumber()) {
                std::cerr << load.fieldName() << " is not a number: " << load.String() << std::endl;
                exit(EXIT_FAILURE);
            }
            for (int queueCount = 0; queueCount < load.Int(); ++queueCount)
                loadQueues.push_back(load.fieldName());
        }
        if (loadQueues.size() < 1) {
            std::cerr << "No load queues were created from: " << loadQueueJson << std::endl;
            exit(EXIT_FAILURE);
        }
        chunksPerShard = loadQueues.size();
        batcherSettings.loadQueues = &loadQueues;
        dispatchSettings.loadQueues = &loadQueues;

        int originalThreads = threads;
        if (threads == 0) threads = std::thread::hardware_concurrency() * 2;
        else if (threads < 0) {
            threads = std::thread::hardware_concurrency() + threads;
            if (threads < 1) {
                std::cerr << "Request hardware threads(" << std::thread::hardware_concurrency()
                << ") minus " << std::abs(originalThreads) << ".  That is less than 1.  Exiting"
                << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        if (output.endPoints.directLoad) output.stopBalancer = true;
        if (input.endPoints.directLoad) output.stopBalancer = true;

    }

    Loader::Loader(Settings settings) :
            _settings(std::move(settings)),
            _mCluster (_settings.output.uri),
            _ramMax (tools::getTotalSystemMemory()),
            _threadsMax (static_cast<size_t>(_settings.threads))
    {
        _writeOps = 0;
        setupLoad();
        _mCluster.loadCluster();
        _endPoints.reset(new EndPointHolder(settings.output.endPoints, cluster()));
        _chunkDispatch.reset(new dispatch::ChunkDispatcher(_settings.dispatchSettings,
                                                           cluster(),
                                                           _endPoints.get(),
                                                           _settings.output.ns()));
    }

    void Loader::setupLoad() {
        if (_settings.sharded) {
            if (!_mCluster.isSharded()) {
                std::cerr << "Unable to load sharded cluster metadata, this is required for a"
                        " sharded cluster load" << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        if (_mCluster.isSharded()) {
            if (_settings.output.stopBalancer) _mCluster.stopBalancer();
        }
        else {
            //Need to create fake shard info here
            throw std::logic_error("Currently only supports sharded setups");
        }

        std::unique_ptr<mongo::DBClientBase> conn;
        std::string error;
        conn.reset(_settings.output.cs.connect(error));
        if (!error.empty()) {
            std::cerr << "Unable to connect to database: " << error << std::endl;
            exit(EXIT_FAILURE);
        }

        if (_settings.dropDb) {
            conn->dropDatabase(_settings.output.database);
        }
        else if (_settings.dropColl) {
            conn->dropCollection(_settings.output.ns());
        }
        else if (_settings.dropIndexes) {
            conn->dropIndexes(_settings.output.ns());
        }

        if (_mCluster.isSharded() && _settings.output.stopBalancer)
            if(_mCluster.stopBalancerWait(std::chrono::seconds(120))) {
                std::cerr << "Unable to stop the balancer" << std::endl;
                exit(EXIT_FAILURE);
            }

        if (_settings.sharded) {
            //TODO: make these checks more sophisticated (i.e. conditions already true? success!)
            mongo::BSONObj info;
            if (!_mCluster.enableSharding(_settings.output.database, &info)) {
                if (info.getIntField("ok") != 0)
                    std::cerr << "Sharding db failed: " << info << std::endl;
                info = mongo::BSONObj().getOwned();
            }
            assert(_settings.chunksPerShard > 0);
            if (_settings.hashed) {
                int totalChunks = _settings.chunksPerShard * _mCluster.shards().size();
                if (!_mCluster.shardCollection(_settings.output.ns(), _settings.shardKeysBson,
                                               _settings.shardKeyUnique, totalChunks, &info)) {
                    //The collection already being sharded is only an error if it was supposed to be dropped
                    std::string strerror = info.getStringField("errmsg");
                    if ((strerror != "already sharded") || _settings.dropDb || _settings.dropColl) {
                        std::cerr << "Sharding collection failed: " << strerror << "\nExiting" << std::endl;
                        exit(EXIT_FAILURE);
                    }
                }
                _mCluster.waitForChunksPerShard(_settings.output.ns(),_settings.chunksPerShard);
            }
            else {
                //Don't do presplits for non-hashed here, no idea what the data is yet
                if (!_mCluster.shardCollection(_settings.output.ns(), _settings.shardKeysBson,
                                               _settings.shardKeyUnique,  &info)) {
                    //The collection already being sharded is only an error if it was supposed to be dropped
                    std::string strerror = info.getStringField("errmsg");
                    if ((strerror != "already sharded") || _settings.dropDb || _settings.dropColl) {
                        std::cerr << "Sharding collection failed: " << info << "\nExiting" << std::endl;
                        exit(EXIT_FAILURE);
                    }
                }
            }
        }
    }

    void Loader::setEndPoints() {
        _endPoints->start();
    }

    dispatch::ChunkDispatchInterface* Loader::getNextPrep() {
        tools::MutexLockGuard lg(_prepSetMutex);
        if (_wf.empty()) return nullptr;
        dispatch::ChunkDispatchInterface* ret;
        ret = _wf.front();
        _wf.pop_front();
        return ret;
    }

    void Loader::threadPrepQueue() {
        for (;;) {
            dispatch::ChunkDispatchInterface* prep = getNextPrep();
            if (prep == nullptr) break;
            prep->prep();
            prep->doLoad();
        }
    }

    void Loader::run() {
        //Total time
        tools::SimpleTimer<> timerLoad;
        tools::SimpleTimer<> timerRead;
        /*
         * The hardware parameters we are working with. Note that ram is free RAM when this program
         * started.  i.e. the working ram available.
         */
        std::cout << "Threads: " << _settings.threads << " RAM(Mb): "
                  << _ramMax / 1024 / 1024
                  << "\nStarting read of data"
                  << std::endl;

        if (!_mCluster.disableBalancing(_settings.output.ns()))
            exit(EXIT_FAILURE);
        std::cout << "WARNING: Balancing has been disabled on name space \""
                << _settings.output.ns() << "\".  It will only be enabled on a successful load,"
                " otherwise it must be done manually." << std::endl;


        std::unique_ptr<InputProcessorInterface> inputProcessor(
                InputProcessorFactory::createObject(_settings.inputType, this));
        inputProcessor->run();

        std::this_thread::sleep_for(std::chrono::seconds(1));
        setEndPoints();

        /*
         * After the load is complete hit all queues and call any additional actions.
         * For instance, sort RAM queues.
         * Waterfall means that finalize is called in shard chunk order to minimize possible
         * waiting.  The general assumption is that there are more chunks than threads available
         */
        size_t finalizeThreads = _threadsMax;
        tools::ThreadPool tpFinalize(finalizeThreads);
        _wf = _chunkDispatch->getWaterFall();
        //Wait for all threads to finish processing segments
        inputProcessor->waitEnd();
        timerRead.stop();

        std::cout << "Entering finalize phase" << std::endl;

        for (size_t i = 0; i < finalizeThreads; i++)
            tpFinalize.queue([this] {this->threadPrepQueue();});

        /*
        if (!enabledEndPoints()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            setEndPoints();
        }
        */

        //Make sure all threads are kicked off
        std::this_thread::sleep_for(std::chrono::seconds(2));

        /*
         *  Wait for all threads to shutdown prior to exit.
         */
        tpFinalize.endWaitInitiate();
        tpFinalize.joinAll();

        _endPoints->gracefulShutdownJoin();

        timerLoad.stop();
        long loadSeconds = timerLoad.seconds();
        long readSeconds = timerRead.seconds();
        std::cout << "\nLoad time: " << loadSeconds / 60 << "m" << loadSeconds % 60 << "s"
            << "\nRead time: " << readSeconds / 60 << "m" << readSeconds % 60 << "s" << std::endl;

        /*
         * Output the stats if requested
         */
        if (!_settings.statsFile.empty()) {
            try {
            std::ofstream statsfile(_settings.statsFile, std::ios_base::out | std::ios_base::app);
            //If the file is empty, print the header
            if (statsfile.tellp() == 0) {
                statsfile << "\"time(s)\","
                        << "\"time\","
                        << "\"bypass\","
                        << "\"type\","
                        << "\"input time(s)\","
                        << "\"key\","
                        << "\"queuing\","
                        << "\"queue size\","
                        << "\"threads\","
                        << "\"endpoint conns\","
                        << "\"wc\","
                        << "\"note\""
                << std::endl;
            }
            statsfile << "\"" << timerLoad.seconds() << "\", "
                    << "\"" << loadSeconds / 60 << "m" << loadSeconds % 60 << "s" << "\", "
                    << "\"" << _settings.output.endPoints.directLoad << "\", "
                    << "\"" << _settings.inputType << "\", "
                    << "\"" << timerRead.seconds() << "\", "
                    << "\"" << _settings.shardKeyJson << "\", "
                    << "\"" << _settings.loadQueueJson << "\", "
                    << "\"" << _settings.batcherSettings.queueSize << "\", "
                    << "\"" << _settings.threads << "\", "
                    << "\"" << _settings.output.endPoints.threadCount << "\", "
                    << "\"" << _settings.dispatchSettings.writeConcern << "\", "
                    << "\"" << _settings.statsFileNote << "\""
                    << std::endl;
            }
            catch (const std::exception &e) {
                std::cerr << "Exception writing stats: " << e.what() << std::endl;
            }
            catch (...) {
                std::cerr << "Unknown exception writing stats. " << std::endl;
            }
            _mCluster.enableBalancing(_settings.output.ns());
        }

    }

}  //namespace loader
