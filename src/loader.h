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

#include <fstream>
#include <list>
#include <memory>
#include <utility>
#include <vector>
#include "bson_tools.h"
#include "concurrent_container.h"
#include "input_processor.h"
#include "loader_defs.h"
#include "mongo_cxxdriver.h"
#include "mongo_end_point.h"
#include "batch_dispatch.h"
#include "tools.h"
#include "threading.h"

namespace loader {

    /**
     *  Loader does all the work actually loading the mongoDs.
     *  The main function in this class is run() which kicks the load off.
     *  _mCluster must be accessed read only after initialization.
     *  Loader assumes the balancer is turned off (we don't want the wasted efficient of chunk
     *  moves while loading so this is reasonable and saves some work.
     *
     *  NOTE: We do NOT turn the balancer back on.  If multiple simultaneous loads take place it's
     *  too dangerous for little benefit.
     */

    class Loader {
    public:
        using MissTime = std::chrono::milliseconds;

        /**
         * Values required to setup the loader
         */
        class Settings {
        public:
            static const std::string MONGO_CLUSTER_INPUT;
            class ClusterSettings {
            public:
                std::string uri;
                mongo::ConnectionString cs;
                bool stopBalancer;
                std::string database;
                std::string collection;
                std::string ns() const {
                    return database + "." + collection;
                }

                void validate() {
                    if (database.empty())
                        throw std::logic_error("Database is empty");
                    if (collection.empty())
                        throw std::logic_error("Collection is empty");

                    if (uri.substr(0,mongo::uriStart.size()) != mongo::uriStart) {
                        uri = mongo::uriStart + uri;
                    }
                    std::string error;
                    cs = mongo::ConnectionString::parse(uri, error);
                    if (!error.empty()) {
                        std::cerr << "Unable to parse connection string: " << error << std::endl;
                        exit(EXIT_FAILURE);
                    }
                }
            };
            using FieldKeys = std::vector<std::string>;
            std::string statsFile;
            std::string statsFileNote;
            std::string loadPath;
            std::string fileRegex;
            std::string inputType;
            std::string workPath;
            std::string loadQueueJson;
            mongo::BSONObj loadQueueBson;
            LoadQueues loadQueues;
            int syncDelay;
            int threads;
            size_t mongoLocklessMissWait;
            bool add_id;
            bool indexHas_id;
            size_t indexPos_id;
            bool hashed;
            size_t chunksPerShard;
            bool shardKeyUnique;
            std::string shardKeyJson;
            mongo::BSONObj shardKeysBson;
            FieldKeys shardKeyFields;
            bool dropDb;
            bool dropColl;
            bool sharded;
            bool dropIndexes;

            ClusterSettings input;
            ClusterSettings output;
            docbuilder::InputNameSpaceContainer::Settings batcherSettings;
            dispatch::ChunkDispatcher::Settings dispatchSettings;
            tools::mtools::MongoEndPointSettings inputEndPointSettings;
            tools::mtools::MongoEndPointSettings outputEndPointSettings;

            /**
             * Check invariants and sets dependent settings
             * Needs to be called once all the user input is read in
             */
            void process();

            static std::string inputTypesPretty() {
                return InputFormatFactory::getKeysPretty();
            }
        };

        /**
         * LoaderStats is currently "dead".  It is being kept around for the next round of optimizations.
         */
        struct LoaderStats {
            std::atomic<size_t> feederMisses;
            MissTime feederMissTime;
            size_t docFails;

            LoaderStats() :
                    feederMisses(), feederMissTime(), docFails()
            {
            }

        };

        explicit Loader(Settings settings);

        /**
         * Gets stats
         */
        const LoaderStats& stats() const {
            return _stats;
        }

        /**
         * Get cluster
         * Must be read only in multithreaded mode
         */
        tools::mtools::MongoCluster& cluster() {
            return _mCluster;
        }

        /**
         * Returns the ChunkDispatcher queues
         */
        dispatch::ChunkDispatcher& chunkDispatcher() {
            return *_chunkDispatch.get();
        }

        docbuilder::InputNameSpaceContainer& inputAggregator() {
            return * _inputAggregator.get();
        }

        /**
         * Returns the settings.
         */
        const Settings& settings() const {
            return _settings;
        }

        /**
         * run() kicks off the loading process.
         */
        void run();

        /**
         * Returns the settings for loader queues.
         */
        const docbuilder::InputNameSpaceContainer::Settings& queueSettings() const {
            return _settings.batcherSettings;
        }

    private:
        using IndexObj = mongo::BSONObj;

        LoaderStats _stats;
        const Settings _settings;
        tools::mtools::MongoCluster _mCluster;
        std::unique_ptr<EndPointHolder> _endPoints;
        std::unique_ptr<dispatch::ChunkDispatcher> _chunkDispatch;
        std::unique_ptr<docbuilder::InputNameSpaceContainer> _inputAggregator;

        size_t _ramMax;
        size_t _threadsMax;
        std::atomic<unsigned long long> _writeOps;
        dispatch::ChunkDispatcher::OrderedWaterFall _wf;
        tools::Mutex _prepSetMutex;


        bool enabledEndPoints() {
            return _endPoints->isRunning();
        }

        /**
         * Setup the environment for loading
         * 1)Check to make sure we are sharded or not as user indicates
         * 2)Stop the balancer if needed
         * 3)Drop what is needed
         * 4)Create database if needed
         * 5)Create unhashed collection if needed
         * 6)Ensure the balancer is stopped
         * 7)Do splits (create hashed collection at this point)
         */
        void setupLoad();

        /**
         * Start end points up
         */
        void setEndPoints();

        /**
         * Creates objects and runs the notifications to the queues that the load process has
         * completed reading in all the files.
         * Thread safe
         */
        void threadPrepQueue();

        /**
         * Get the next chunk to notify of input file completion in shard chunk order.
         * Thread safe
         */
        dispatch::ChunkDispatchInterface* getNextPrep();

        /**
         * Resolves a connection for a shard
         */
        const std::string& getConn(const std::string& shard) {
            return this->_mCluster.getConn(shard);
        }
    };
}  //namespace loader
