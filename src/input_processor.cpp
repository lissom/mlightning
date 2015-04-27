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

#include <boost/filesystem.hpp>
#include "loader.h"
#include "loader_defs.h"
#include "input_processor.h"
#include <regex>
#include <string.h>
#include "util/hasher.h"

namespace loader {

    const size_t MONGOINPUT_DEFAULT_MAX_SIZE = 100;

    const bool MongoInputProcessor::_registerFactory = InputProcessorFactory::registerCreator(
            INPUT_MONGO, MongoInputProcessor::create);
    const bool FileInputProcessor::_registerFactoryJson = InputProcessorFactory::registerCreator(
            INPUT_JSON, FileInputProcessor::create);
    const bool FileInputProcessor::_registerFactoryBson = InputProcessorFactory::registerCreator(
            INPUT_BSON, FileInputProcessor::create);
    const bool FileInputProcessor::_registerFactoryMltn = InputProcessorFactory::registerCreator(
            INPUT_MLTN, FileInputProcessor::create);


    MongoInputProcessor::MongoInputProcessor(Loader* const owner) :
        _owner(owner),
        _mCluster(_owner->settings().input.uri),
        _endPoints(_owner->settings().input.endPoints, _mCluster),
        _ns(_owner->settings().input.ns()),
        _tpBatcher(new tools::ThreadPool(_owner->settings().threads)),
        _didDisableBalancerForNS( _mCluster.isBalancingEnabled(_owner->settings().input.ns())) {
        if (_didDisableBalancerForNS) {
           if (!_mCluster.disableBalancing(_owner->settings().input.ns()))
             exit(EXIT_FAILURE);
        }
        _inputQueue.setSizeMax(MONGOINPUT_DEFAULT_MAX_SIZE);
    }

    MongoInputProcessor::~MongoInputProcessor() {
        //Renable balancing on the input namespace if we disabled it
        if (_didDisableBalancerForNS)
            (void)_mCluster.enableBalancing(_owner->settings().input.ns());
    }

    void MongoInputProcessor::run() {
        if (_mCluster.sharded() && _mCluster.balancerIsRunning()) {
            std::cout << "Waiting for balancer to stop.  Conn: " << _mCluster.connStr().toString() << std::endl;
            _mCluster.waitForBalancerToStop();
        }
        _inputShardChunks = _mCluster.getShardChunks(_ns);
        //If the ns has no chunks, check to see if it's not sharded, if not synth shard it
        if (_inputShardChunks.empty()) {
            mongo::BSONObj splits;
            //Set max chunks size to 64 megs
            if (!_mCluster.splitVector(&splits, _ns, _owner->settings().shardKeyBson, 65 * 1024 * 1024)) {
                std::cerr << "Error calling splitVector(no chunks found, assumed unsharded) for " << _ns
                        << ": " << splits.getStringField("errmsg") << "\nExiting" << std::endl;
                //Assuming there were supposed to be chunks this is an error
                exit(EXIT_FAILURE);
            }
            _mCluster.shardCollection(_ns, _owner->settings().shardKeyBson, false,
                   splits, true);
            _inputShardChunks = _mCluster.getShardChunks(_ns);
            assert(_inputShardChunks.size() > 0);
        }
        //displayChunkStats();
        dispatchChunksForRead();
        setupProcessLoops();
        _tpBatcher->endWait();
    }

    void MongoInputProcessor::displayChunkStats() {
        //Output size of chunks
        std::cout << "Shard name: Chunk count";
        for (auto&& shardChunks: _inputShardChunks)
            std::cout << "\n" << shardChunks.first << ": " << shardChunks.second.size();
        std::cout << std::endl;
    }

    void MongoInputProcessor::threadProcessLoop() {
        DocumentProcessor dp(_owner);
        BsonContainer data;
        for (;;) {
            //Check if there is work to do, if not and there is future work, sleep 1s
            if (!_inputQueue.pop(data)) {
                if (_chunksRemaining == 0)
                  break;
                sleep(1);
                continue;
            }
            --_chunksRemaining;
            for(auto&& itr: data) {
                dp.doc = itr;
                dp.process();
                dp.push();
            }
            data.clear();
        }
    }

    void MongoInputProcessor::setupProcessLoops() {
        //If chunksRemaining isn't > 0 the processing can terminate prematurely
        assert(_chunksRemaining > 0);
        _tpBatcher->threadForEach([this]() {this->threadProcessLoop();});
    }

    void MongoInputProcessor::dispatchChunksForRead() {
        //todo: should change this to iterate by shard (i.e. while (shardChunks.size() .. for(.. if !size remove))
        //todo: may need to run in a different context for extremely large chunk counts so things kick off immediately
        //Shardkey must be added so that hashed shard keys are properly accounted for
        mongo::BSONObj shardKey = _mCluster.getNs(_ns).key;
        if (shardKey.isEmpty()) throw std::logic_error("MongoInputProcessor::dispatchChunksForRead - shardKey to hint on cannot be empty.");
        if (_inputShardChunks.empty()) throw std::logic_error("MongoInputProcessor::dispatchChunksForRead - no chunks found for reading");
        for (auto&& shardChunks: _inputShardChunks) {
            EndPointHolder::MongoEndPoint* endPoint;
            if (_endPoints.directLoad())
                endPoint = _endPoints.at(shardChunks.first);
            else
                endPoint = _endPoints.getMongoSCycle();
            for (const auto& chunks: shardChunks.second) {
                endPoint->push(tools::mtools::OpQueueQueryBulk::make(
                        [this](tools::mtools::DbOp* op, tools::mtools::OpReturnCode status) {
                                                    this->inputQueryCallBack(op, status);
                                                },
                    _ns,
                    mongo::Query().minKey(chunks.min).maxKey(chunks.max).hint(shardKey)));
                ++_chunksRemaining;
            }
        }
    }

    void MongoInputProcessor::inputQueryCallBack(tools::mtools::DbOp* dbOp__,
            tools::mtools::OpReturnCode status__) {
        auto dbOp = dynamic_cast<tools::mtools::OpQueueQueryBulk*>(dbOp__);
        //todo: handle fails gracefully
        //status should currently terminate in the results object
        assert(status__);
        _inputQueue.pushCheckMaxSize(std::move(dbOp->_data));
    }

    void MongoInputProcessor::waitEnd() {
        _endPoints.gracefulShutdownJoin();
        _tpBatcher->endWaitInitiate();
        _tpBatcher->joinAll();
        if (_chunksRemaining) {
            std::cerr << "Not all chunks were processed (remaining = " << _chunksRemaining
                    << ")\nExiting" << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    FileInputProcessor::FileInputProcessor(Loader* owner) :
        _owner(owner),
        _threads(owner->settings().threads),
        _inputType(owner->settings().inputType),
        _loadDir(owner->settings().loadPath),
        _fileRegex(owner->settings().fileRegex),
        _ns(owner->settings().output.ns())
    { }

    void FileInputProcessor::run() {
        /*
         * Initial setup.  Getting all the files that are going to put into the mognoDs.
         * If we files that are larger than bytes per thread, break them down and into smaller
         * segments
         */
        using namespace boost::filesystem;
        std::deque<tools::fileinfo> files;
        path loadDir(_loadDir);
        std::regex fileRegex(_fileRegex);
        unsigned long long totalSize{};
        for (directory_iterator ditr {loadDir}; ditr != directory_iterator {}; ditr++) {
            std::string filename = ditr->path().string();
            if (!is_regular_file(ditr->path())
                || (_fileRegex.length() && !std::regex_match(filename, fileRegex))) continue;
            size_t filesize = boost::filesystem::file_size(filename);
            files.emplace_back(filename, filesize);
            totalSize += filesize;
        }
        //Ensure that there are files for us to process
        if (!files.size()) {
            std::cerr << "No files to load at: " << loadDir;
            if (_fileRegex.size())
                std::cerr << ". Using regex: " << _fileRegex;
            std::cerr << std::endl;
            exit(EXIT_SUCCESS);
        }
        /*
         * Crucial _locSegMapping is sorted and not changed past this point.  _locSetMapping is used
         * as an index, it can have std::sort called against it to find the index of a file name.
         *
         * The various queue stages that need to look up a file index by name or name by index use
         * this and expect to use this as the source of truth for file->index mapping.
         */
        std::sort(files.begin(), files.end());
        _locSegMapping.reserve(files.size());
        if (allowInputSplits()) {
            unsigned long long sizePerThread = totalSize / _threads;
            for (auto&& filerec : files) {
                /**
                 * If the size per thread is greater than an overage, split the file up into segments
                 */
                if (filerec.size > sizePerThread + OVERAGE_SIZE) {
                    size_t pos{};
                    for (pos = 0; pos < filerec.size; pos += sizePerThread)
                        _locSegMapping.emplace_back(filerec.name, pos, pos + sizePerThread);
                    //always have a "to end" for every file for consistancy
                    _locSegMapping.back().end = 0;
                }
                else
                    _locSegMapping.emplace_back(filerec.name, 0, 0);
            }
        } else {
            for (auto&& i : files)
                _locSegMapping.emplace_back(i.name, 0, 0);
        }

        //Insert the segments into the queue for the threads to consume
        LocSegmentQueue::ContainerType fileQ(_locSegMapping.begin(), _locSegMapping.end());
        _locSegmentQueue.swap(fileQ);
        std::cout << "Dir: " << loadDir << "\nSegments: " << _locSegmentQueue.size()
                  << "\nKicking off run" << std::endl;

        /*
         * Start up the threads to read in the files
         */
        size_t inputThreads = _threads > _locSegmentQueue.size() ? _locSegmentQueue.size()
                : _threads;
        _tpBatcher.reset(new tools::ThreadPool(inputThreads));
        _tpBatcher->threadForEach([this]() {this->threadProcessLoop();});
        _tpBatcher->endWaitInitiate();

    }

    void FileInputProcessor::threadProcessLoop() {
        FileSegmentProcessor lsp(_owner, _owner->settings().inputType);
        tools::LocSegment segment;
        for (;;) {
            if (!_locSegmentQueue.pop(segment)) break;
            auto itr = std::find(_locSegMapping.begin(), _locSegMapping.end(), segment);
            assert(itr != _locSegMapping.end());
            lsp.processSegmentToBatch(segment, std::distance(_locSegMapping.begin(), itr));
            ++_processedSegments;
        }
    }

    void FileInputProcessor::waitEnd() {
        _tpBatcher->joinAll();
        //Make sure that all segments have been processed, invariant
        if (_processedSegments != _locSegMapping.size()) {
            std::cerr << "Error: not all segments processed. Total segments: "
                    << _locSegMapping.size() << "; Processed: " << _processedSegments
                    << "\nExiting" << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    //TODO::clean this up and not pass owner
    DocumentProcessor::DocumentProcessor(Loader* const owner) :
            _owner(owner),
            _add_id(_owner->settings().indexHas_id && _owner->settings().add_id),
            _keys(_owner->settings().shardKeyBson),
            _keyFieldsCount(_keys.nFields()),
            _inputAggregator(_owner->queueSettings(),
                    _owner->cluster(),
                    &_owner->chunkDispatcher(),
                    _owner->settings().output.ns())
    {}

    void DocumentProcessor::process() {
        mongo::BSONObjBuilder extra;
        //TODO: Make sure that this extra field keys works with multikey indexes, sparse, etc
        //fillWithNull is set to false, not sure that works with mulitfield keys
        //May need to switch to void getFields(unsigned n, const char **fieldNames, BSONElement* fields) const;
        _docShardKey = doc.extractFields(_keys, false);
        _added_id = false;
        //Check to see if the document has a complete shard key
        if (_docShardKey.nFields() != _keyFieldsCount) {
            //If we can add the _id and _id is the only missing field, add it, else error
            if (_add_id && (_keyFieldsCount - _docShardKey.nFields()) == 1 &&
                    !_docShardKey.hasField("_id")) {
                //If the shard key is only short by _id and we are willing to add it, do so
                //The shard key must be complete at this stage so all sorting is correct
                _added_id = true;
                auto oid = mongo::OID::gen();
                //Update the added fields
                extra.append("_id", oid);
                //Add the _id field
                if (_keyFieldsCount == 1)
                    _docShardKey = BSON("_id" << oid);
                else {
                    auto itr = _docShardKey.begin();
                    size_t pos = 0;
                    mongo::BSONObjBuilder index;
                    do {
                        if (pos != _owner->settings().indexPos_id) index.append(itr.next());
                        else index.append("_id", oid);
                        ++pos;
                    }
                    while (itr.more());
                    _docShardKey = index.obj();
                }
            }
            //TODO: continue on error: convert to log message if continue
            else {
                std::cerr << "No shard key in final doc for insert: " << doc << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        //If hashing is required, do it
        if (_owner->settings().hashed) _docShardKey =
                BSON("_id-hash" << mongo::BSONElementHasher::hash64(_docShardKey.firstElement(),
                                       mongo::BSONElementHasher::DEFAULT_HASH_SEED));
        _extra = &extra;
    }

    Bson DocumentProcessor::getFinalDoc() {
        return std::move(doc);
    }

    Bson DocumentProcessor::getIndex() {
        return std::move(_docShardKey);
    }

    Bson DocumentProcessor::getAdd() {
        return std::move(_extra->obj());
    }

    FileSegmentProcessor::FileSegmentProcessor(Loader* owner, const std::string& fileType) :
        DocumentProcessor(owner),
        _docLogicalLoc{}
    {
        _input = InputFormatFactory::createObject(fileType);
    }

    tools::DocLoc FileSegmentProcessor::getLoc() {
        _docLoc.length = _input->pos() - _docLoc.start;
        _docLoc.length--;
        assert(_docLoc.length > 0);
        return std::move(_docLoc);
    }

    void FileSegmentProcessor::processSegmentToBatch(tools::LocSegment segment,
                                               tools::LogicalLoc logicalLoc)
    {
        _docLogicalLoc = logicalLoc;
        _input->reset(std::move(segment));
        _docLoc.location = _docLogicalLoc;
        _docLoc.start = _input->pos();
        //Reads in documents until the segment comes back with no more docs
        while (_input->next(&doc)) {
            process();
            push();
            _docLoc.start = _input->pos();
        }
    }
}  //namespace loader
