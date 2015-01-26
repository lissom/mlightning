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

#include "input_batcher.h"
#include "input_format_file.h"
#include "mongo_cluster.h"
#include "mongo_cxxdriver.h"

namespace loader {

    class Loader;
    /*
     * Runs the loading
     */
    class InputProcessorInterface {
    public:
        virtual ~InputProcessorInterface() {};
        virtual void run() = 0;
        virtual void waitEnd() = 0;
    };

    /**
     * Pointer returned by factory functions
     */
    using InputProcessorInterfacePtr = std::unique_ptr<InputProcessorInterface>;

    /**
     * Factory function signature
     */
    using CreateInputProcessorFunction =
            std::function<InputProcessorInterfacePtr(Loader* const)>;

    /*
     * Factory
     */
    using InputProcessorFactory = tools::RegisterFactory<InputProcessorInterfacePtr,
            CreateInputProcessorFunction>;


    class MongoInputProcessor : public InputProcessorInterface {
    public:
        MongoInputProcessor(Loader* const owner);
        /**
         * Pulls the data from the target intput database and puts it into the queues for processing
         */
        void run() override;
        /**
         * Returns when all processing is completed
         */
        void waitEnd() override;

        static InputProcessorInterfacePtr create(Loader* const loader) {
            return InputProcessorInterfacePtr(new MongoInputProcessor(loader));
        }

    private:
        using BsonContainer = tools::mtools::OpQueueQueryBulk::BsonContainer;

        /*
         * Print out the shard chunk stats left to process (totals before the run starts)
         */
        void displayChunkStats();
        /**
         * Function the process threads call to process to batches
         */
        void threadProcessLoop();
        /**
         * Setup the threads to process the query results into the batcher
         */
        void setupProcessLoops();
        /**
         * Sets up reading from the shards by chunk
         */
        void dispatchChunksForRead();
        /**
         * Pushes the data from the database operation onto the input queue
         */
        void inputQueryCallBack(tools::mtools::DbOp*, tools::mtools::OpReturnCode);

        Loader* const _owner;
        //Number of chunks that have not had their results queued, must be set before processing starts
        std::atomic<size_t> _chunksRemaining{};
        //Target input cluster
        tools::mtools::MongoCluster _mCluster;
        //Ends points to target input cluster
        EndPointHolder _endPoints;
        //Input namespace
        const std::string _ns;
        //Input shard key
        const mongo::BSONObj _shardKey;
        //Input chunks by shard, is drained eventually
        tools::mtools::MongoCluster::ShardChunks _inputShardChunks;
        //Query results are stored here while waiting for a thread to process them
        tools::ConcurrentQueue<BsonContainer> _inputQueue;
        std::unique_ptr<tools::ThreadPool> _tpBatcher;

        static const bool _registerFactory;
    };

    /*
     * Processes files
     */
    //TODO: If splits are possible is tied into the format, not the processing, move
    class FileInputProcessor : public InputProcessorInterface {
    public:
        //Minimum average size that needs to be exceeded for a split
        static constexpr unsigned long long OVERAGE_SIZE = 100 * 1024 * 1024;

        FileInputProcessor(Loader* owner);
        /**
         * Do the work
         */
        void run() override;

        /**
         * Returns when all input is finished
         * Calling this function before calling run is undefined
         */
        void waitEnd() override;

        static std::unique_ptr<InputProcessorInterface> create(Loader* const loader) {
            return std::unique_ptr<InputProcessorInterface>(new FileInputProcessor(loader));
        }


    private:
        /**
         * This is where the threads do the actual work
         */
        void threadProcessLoop();

        const bool allowInputSplits() const { return _inputType == "json"; }

        using LocSegmentQueue = tools::ConcurrentQueue<tools::LocSegment>;
        LocSegmentQueue _locSegmentQueue;
        tools::LocSegMapping _locSegMapping;
        std::atomic<std::size_t> _processedSegments{};
        std::unique_ptr<tools::ThreadPool> _tpBatcher;

        Loader* const _owner;
        size_t _threads;
        const std::string _inputType;
        const std::string _loadDir;
        const std::string _fileRegex;
        const tools::mtools::MongoCluster::NameSpace _ns;

        static const bool _registerFactoryJson;
        static const bool _registerFactoryBson;
    };

    /*
     * Performs any needed transformations on a BSON document so it can be inserted
     * To use, load a document into the public "doc" variable and then run process() and push()
     * Given that BSONObj's cannot currently be moved, leaving doc public is a performance decision
     */
    class DocumentProcessor : public docbuilder::DocumentBuilderInterface {
    public:
        DocumentProcessor(Loader* const owner);
        /**
         * A document is put in _doc and then it is processed
         * Done due to BSON ownership model
         */
        void process();
        void push() { _inputAggregator.targetStage(_docShardKey)->push(this); }

        Bson getFinalDoc() override;
        Bson getIndex() override;
        Bson getAdd() override;
        tools::DocLoc getLoc() { return tools::DocLoc(); }

        Loader* owner() { return _owner; }

        mongo::BSONObj doc;

    private:
        Loader* const _owner;
        const bool _add_id;
        const mongo::BSONObj _keys;
        int _keyFieldsCount;
        docbuilder::InputNameSpaceContainer _inputAggregator;
        mongo::BSONObjBuilder *_extra = NULL;
        mongo::BSONObj _docShardKey;
        bool _added_id{};

    };

    class FileSegmentProcessor : public DocumentProcessor {
    public:
        FileSegmentProcessor(Loader* owner, const std::string& fileType);

        /**
         * Takes a segment and it's logical location (i.e. the mapping to something real)
         * It then puts all the documents into the right queues
         */
        void processSegmentToBatch(tools::LocSegment segment, tools::LogicalLoc logicalLoc);

        tools::DocLoc getLoc() override;

    private:
        tools::LogicalLoc _docLogicalLoc;
        tools::DocLoc _docLoc;
        FileInputInterfacePtr _input;

    };

}  //namespace loader

