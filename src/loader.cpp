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

#include <algorithm>
#include <boost/filesystem.hpp>
#include <exception>
#include "input_processor.h"
#include <iostream>
#include "loader.h"
#include "mongo_cxxdriver.h"
#include <tuple>

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

        if (!workPath.empty()) {
            boost::filesystem::path work(workPath);
            workPath = work.make_preferred().native();
            if (workPath.back() != boost::filesystem::path::preferred_separator)
                workPath += boost::filesystem::path::preferred_separator;
            if (boost::filesystem::exists(boost::filesystem::path(workPath))) {
                std::cerr << "workPath already exists, this can be dangerous so mLightning is exiting. "
                        "Remove this path and all of it's contents and start again if the path is "
                        "correct. workPath: " << workPath
                      << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        if (outputType == OUTPUT_MONGO)
            try {
                output.validate();
            }
            catch (std::exception &e) {
                std::cerr << "Unable to validate output cluster: " << e.what() << std::endl;
                exit(EXIT_FAILURE);
            }
        else {
            //For all file output formats use hashed _id if the user hasn't given a different key
            output.database = "mlightning";
            output.collection = "mlightning_synth";
            loadQueueJson = OUTPUT_FILE;
            if (shardKeyJson.empty())
                shardKeyJson = R"({ "_id" : "hashed" })";
            //Create queues equal to the number of threads
            loadQueueJson = R"({"ml1": )" + std::to_string(threads) + R"(})";
            if (outputType == OUTPUT_FILE) {
                if (workPath.empty()) {
                    std::cerr << "Output of \"file\" specified.  No workPath given to output to" << std::endl;
                    exit(EXIT_FAILURE);
                }
            }
            else {
                std::cerr << "Invalid output type given: " << outputType << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        //If the input type is a cluster there is no directory given, validate cluster input
        if (inputType == INPUT_MONGO || loadPath.empty()) {
            try {
                input.validate();
                //The input type is INPUT_MONGO by deduction
                inputType = INPUT_MONGO;
            }
            catch (std::exception &e) {
                if (loadPath.empty())
                    std::cerr << "No load path, assuming cluster \n";
                std::cerr << "Unable to validate input cluster: " << e.what() << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        else {
            //Ensure the directory exists
            if (!is_directory(boost::filesystem::path(loadPath))) {
                std::cerr << "loadPath is required to be a directory. loadPath: " << loadPath
                          << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        parseShardKey();

        output.endPoints.startImmediate = false;
        input.endPoints.startImmediate = true;

        dispatchSettings.workPath = workPath;
        dispatchSettings.directLoad = output.endPoints.directLoad;

        loadQueueBson = mongo::fromjson(loadQueueJson);
        for (mongo::BSONObj::iterator i(loadQueueBson); i.more();) {
            auto load = i.next();
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

        if (output.endPoints.directLoad) output.stopBalancer = true;
        if (input.endPoints.directLoad) input.stopBalancer = true;

    }

    void Loader::Settings::parseShardKey() {
        indexHas_id = false;
        indexPos_id = size_t(-1);
        size_t count {};
        if (shardKeyJson.empty()) {
            std::cerr << "No shard key for sharded setup" << std::endl;
            exit(EXIT_FAILURE);
        }
        shardKeyBson = mongo::fromjson(shardKeyJson);
        for (mongo::BSONObj::iterator i(shardKeyBson); i.more();) {
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

        if (!indexHas_id) add_id = false;
        dispatchSettings.sortIndex = shardKeyBson;
        batcherSettings.sortIndex = shardKeyBson;
    }

    Loader::Loader(Settings settings) :
            _settings (std::move(settings)),
            _mCluster (_settings.output.uri),
            _ramMax (tools::getTotalSystemMemory())
    {
    }

    void Loader::setupOutputCluster() {
        //The only valid shardkey without a sharded cluster is _id
        if (_mCluster.sharded()) {
            if (_settings.shardKeyJson.empty()) {
                std::cerr << "Unable to load sharded cluster metadata, this is required for a"
                        " sharded cluster load" << std::endl;
                exit(EXIT_FAILURE);
            }
            if (_settings.output.stopBalancer) _mCluster.stopBalancer();
            _disableCollectionBalancing = _mCluster.isBalancingEnabled(_settings.output.ns());
            if (_disableCollectionBalancing) {
                if (!_mCluster.disableBalancing(_settings.output.ns())) {
                    std::cerr << "Unable to disable balancing for ns: " << _settings.output.ns() << std::endl;
                    exit(EXIT_FAILURE);
                }
                std::cout << "WARNING: Balancing has been disabled on name space \""
                        << _settings.output.ns() << "\".  It will only be enabled on a successful load,"
                        " otherwise it must be done manually." << std::endl;
            }
        } else {
            //If this isn't a sharded cluster, "shard" on _id by default
            //There is no good way to do this as MongoCluster isn't brought up until now
            if (_settings.shardKeyJson.empty()) {
                Settings& newSettings = const_cast<Settings&>(_settings);
                newSettings.shardKeyJson = "{_id:1}";
                newSettings.parseShardKey();
            }
        }

        std::unique_ptr<mongo::DBClientBase> conn;
        std::string error;
        conn.reset(_settings.output.cs.connect(error));
        if (!error.empty()) {
            std::cerr << "Unable to connect to database: " << error << std::endl;
            exit(EXIT_FAILURE);
        }

        //Mongo failes on "doens't exist", whichwe don't care about, so save any errors
        mongo::BSONObj dropFailure;
        if (_settings.dropDb) {
            conn->dropDatabase(_settings.output.database, &dropFailure);
        }
        else if (_settings.dropColl) {
            conn->dropCollection(_settings.output.ns(), &dropFailure);
        }
        else if (_settings.dropIndexes) {
            conn->dropIndexes(_settings.output.ns());
        }

        if (_mCluster.sharded()) {
            if (_settings.output.stopBalancer)
                if(_mCluster.stopBalancerWait(std::chrono::seconds(120))) {
                    std::cerr << "Unable to stop the balancer" << std::endl;
                    exit(EXIT_FAILURE);
                }

            //TODO: make these checks more sophisticated (i.e. conditions already true? success!)
            mongo::BSONObj info;
            if (!_mCluster.enableSharding(_settings.output.database, info)) {
                if (info.getIntField("ok") != 1)
                    std::cerr << "Sharding db failed: " << info << std::endl;
                info = mongo::BSONObj().getOwned();
            }
            assert(_settings.chunksPerShard > 0);
            if (_settings.hashed) {
                int totalChunks = _settings.chunksPerShard * _mCluster.shards().size();
                if (!_mCluster.shardCollection(_settings.output.ns(), _settings.shardKeyBson,
                                               _settings.shardKeyUnique, totalChunks, info)) {
                    //The collection already being sharded is only an error if it was supposed to be dropped
                    std::string strerror = info.getStringField("errmsg");
                    if ((strerror != "already sharded") || _settings.dropDb || _settings.dropColl) {
                        if (dropFailure.getIntField("ok") != 1)
                            std::cerr << "Dropping " << (_settings.dropDb ? "database " : "collection ")
                                << "failed: " << dropFailure.getStringField("errmsg") << "\n";
                        std::cerr << "Sharding collection failed: " << strerror << "\nExiting" << std::endl;
                        exit(EXIT_FAILURE);
                    }
                }
                _mCluster.waitForChunksPerShard(_settings.output.ns(),_settings.chunksPerShard);
            }
            else {
                //Don't do presplits for non-hashed here, no idea what the data is yet
                if (!_mCluster.shardCollection(_settings.output.ns(), _settings.shardKeyBson,
                                               _settings.shardKeyUnique,  info)) {
                    //The collection already being sharded is only an error if it was supposed to be dropped
                    std::string strerror = info.getStringField("errmsg");
                    if ((strerror != "already sharded") || _settings.dropDb || _settings.dropColl) {
                        std::cerr << "Sharding collection failed: " << info << "\nExiting" << std::endl;
                        exit(EXIT_FAILURE);
                    }
                }
            }
            //Capture sharded changes if we hashed, etc
            _mCluster.loadCluster();
        } else {
            _mCluster.shardCollection(_settings.output.ns(), _settings.shardKeyBson, true, 1);
        }

        _endPoints.reset(new EndPointHolder(_settings.output.endPoints, cluster()));
    }

    dispatch::ChunkDispatchInterface* Loader::getNextFinalize() {
        tools::MutexLockGuard lg(_prepSetMutex);
        if (_wf.empty()) return nullptr;
        dispatch::ChunkDispatchInterface* ret;
        ret = _wf.front();
        _wf.pop_front();
        return ret;
    }

    void Loader::threadFinalizeQueue() {
        for (;;) {
            dispatch::ChunkDispatchInterface* prep = getNextFinalize();
            if (prep == nullptr) break;
            prep->finalize();
        }
    }

    /*
     * Setups up the splits points via a synthetic split
     * Generates the dump output based on those split points
     * Starts the input to the synthetic splits and dumps to disk
     */
    void Loader::dump() {
        if (!boost::filesystem::create_directories(_settings.workPath.substr(0,
                _settings.workPath.size() - 1))) {
            std::cerr << "Unable to create workPath directory: " << _settings.workPath
                  << std::endl;
            exit(EXIT_FAILURE);
        }
        //Create a single fake shard
        _mCluster.shards().insert(std::make_pair("mlSynth", "mlSynth"));
        mongo::BSONObj info;
        //Create the splits to setup the file write by creating a synthetic output namespace
        _mCluster.shardCollection(_settings.output.ns(), _settings.shardKeyBson, false,
                _settings.threads);
        //Setup the file write
        _chunkDispatch.reset(new dispatch::ChunkDispatcher(_settings.dispatchSettings,
                                                                   cluster(),
                                                                   nullptr,
                                                                   _settings.output.ns()));
        _timerSetup.stop();
        _timerRead.start();
        //Setup the input processor and run it
        std::unique_ptr<InputProcessorInterface> inputProcessor(
            InputProcessorFactory::createObject(_settings.inputType, this));
        inputProcessor->run();

        tools::ThreadPool tpFinalize(_settings.threads);
        _wf = _chunkDispatch->getWaterFall();
        //Wait for all threads to finish processing segments
        inputProcessor->waitEnd();
        _timerRead.stop();

        std::cout << "Entering finalize phase" << std::endl;

        tpFinalize.threadForEach([this] {this->threadFinalizeQueue();});

        //Make sure all threads are kicked off
        std::this_thread::sleep_for(std::chrono::seconds(2));

        /*
         *  Wait for all threads to shutdown prior to exit.
         */
        tpFinalize.endWaitInitiate();
        tpFinalize.joinAll();
    }

    void Loader::load() {
        //TODO: See if there are timing implications with the cleanup of objects
        setupOutputCluster();
        _chunkDispatch.reset(new dispatch::ChunkDispatcher(_settings.dispatchSettings,
                                                           cluster(),
                                                           _endPoints.get(),
                                                           _settings.output.ns()));
        _timerSetup.stop();
        _timerRead.start();

        _endPoints->start();

        std::unique_ptr<InputProcessorInterface> inputProcessor(
            InputProcessorFactory::createObject(_settings.inputType, this));
        inputProcessor->run();

        std::this_thread::sleep_for(std::chrono::seconds(1));

        /*
         * After the load is complete hit all queues and call any additional actions.
         * For instance, sort RAM queues.
         * Waterfall means that finalize is called in shard chunk order to minimize possible
         * waiting.  The general assumption is that there are more chunks than threads available
         */
        tools::ThreadPool tpFinalize(_settings.threads);
        _wf = _chunkDispatch->getWaterFall();
        //Wait for all threads to finish processing segments
        inputProcessor->waitEnd();
        _timerRead.stop();

        std::cout << "Entering finalize phase" << std::endl;

        tpFinalize.threadForEach([this] {this->threadFinalizeQueue();});

        //Make sure all threads are kicked off
        std::this_thread::sleep_for(std::chrono::seconds(2));

        /*
         *  Wait for all threads to shutdown prior to exit.
         */
        tpFinalize.endWaitInitiate();
        tpFinalize.joinAll();

        _endPoints->gracefulShutdownJoin();
    }

    void Loader::run() {
        tools::SimpleTimer<> timerLoad;
        /*
         * The hardware parameters we are working with. Note that ram is free RAM when this program
         * started.  i.e. the working ram available.
         */
        std::cout << "Threads: " << _settings.threads << " RAM(Mb): "
                  << _ramMax / 1024 / 1024
                  << std::endl;

        if (_settings.outputType == OUTPUT_MONGO)
            load();
        else
            dump();

        timerLoad.stop();
        long loadSeconds = timerLoad.seconds();
        long readSeconds = _timerRead.seconds();
        long setupSeconds = _timerSetup.seconds();
        std::cout << "\nLoad time: " << loadSeconds / 60 << "m" << loadSeconds % 60 << "s"
            << "\nSetup time: " << setupSeconds / 60 << "m" << setupSeconds % 60 << "s"
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
                    << "\"" << _timerRead.seconds() << "\", "
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
            //Re-enable collection balancing if we disabled it
        }

    }

}  //namespace loader
