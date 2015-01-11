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

    class MongoInputProcessor : public InputProcessorInterface {
    public:
        MongoInputProcessor(std::string connStr) : _mCluster(connStr) {
        }
        void run() override;
        void waitEnd() override;

    private:
        tools::mtools::MongoCluster _mCluster;
    };



    /*
     * Processes files
     */
    //TODO: If splits are possible is tied into the format, not the processing, move
    class FileInputProcessor : public InputProcessorInterface {
    public:
        //Minimum average size that needs to be exceeded for a split
        static constexpr unsigned long long OVERAGE_SIZE = 100 * 1024 * 1024;
        FileInputProcessor(Loader* owner, size_t threads, std::string inputType,
                           std::string loadDir, std::string fileRegex,
                           tools::mtools::MongoCluster::NameSpace ns) :
            _owner(owner), _threads(threads), _inputType(inputType), _loadDir(std::move(loadDir)),
            _fileRegex(std::move(fileRegex)), _ns(std::move(ns))
        { }

        /**
         * Do the work
         */
        void run() override;

        /**
         * Returns when all input is finished
         * Calling this function before calling run is undefined
         */
        void waitEnd() override;

    private:
        const bool allowInputSplits() const { return _inputType == "json"; }

        using LocSegmentQueue = tools::ConcurrentQueue<tools::LocSegment>;
        LocSegmentQueue _locSegmentQueue;
        tools::LocSegMapping _locSegMapping;
        std::atomic<std::size_t> _processedSegments{};
        std::unique_ptr<tools::ThreadPool> _tpInput;

        Loader * const _owner;
        size_t _threads;
        const std::string _inputType;
        const std::string _loadDir;
        const std::string _fileRegex;
        const tools::mtools::MongoCluster::NameSpace _ns;

        /**
         * This is where the threads do the actual work
         */
        void threadProcessSegment();
    };

    /*
     * Current assumption is that a single LoadSegmentProcessor handles a single namespace.
     * This could change in the future but to keep lookups down it's probably better to
     * make sure that load segments are handed off by namespace if possible.
     */
    class SegmentProcessor : public docbuilder::DocumentBuilder {
    public:
        SegmentProcessor(Loader* owner, std::string ns, const std::string& fileType);

        /**
         * Takes a segment and it's logical location (i.e. the mapping to something real)
         * It then puts all the documents into the right queues
         */
        void processSegmentToBatch(tools::LocSegment segment, tools::LogicalLoc logicalLoc);

        virtual Bson getFinalDoc();
        virtual Bson getIndex();
        virtual Bson getAdd();
        virtual tools::DocLoc getLoc();

    private:
        Loader *_owner;
        const std::string _ns;
        const bool _add_id;
        const mongo::BSONObj _keys;
        int _keyFieldsCount;
        docbuilder::InputNameSpaceContainer& _inputAggregator;
        tools::LogicalLoc _docLogicalLoc;
        tools::DocLoc _docLoc;
        std::string _docJson;
        Bson _doc;
        mongo::BSONObjBuilder *_extra = NULL;
        mongo::BSONObj _docShardKey;
        bool _added_id{};
        FileInputInterfacePtr _input;

    };

}  //namespace loader

