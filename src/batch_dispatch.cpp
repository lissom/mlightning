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

#include <snappy.h>
#include "batch_dispatch.h"

namespace loader {
    namespace dispatch {

        const bool DirectDispatch::factoryRegisterCreator = ChunkDispatcherFactory::registerCreator(
                "direct",
                &DirectDispatch::create);
        const bool RAMQueueDispatch::factoryRegisterCreator = ChunkDispatcherFactory::registerCreator(
                "ram",
                &RAMQueueDispatch::create);
        const bool DiskQueueBoundedFileDispatch::factoryRegisterCreator = ChunkDispatcherFactory::registerCreator(
                "ml1",
                &DiskQueueBoundedFileDispatch::create);

        ChunkDispatchInterface::ChunkDispatchInterface(Settings settings) :
                _settings(std::move(settings)),
                _bulkWriteVersion(_settings.owner->bulkWriteVersion())
        {
            //If we are outputting to an endpoint, get the connection info
            if (_settings.eph) {
                if (_settings.owner->directLoad()) _ep = _settings.owner->getEndPointForChunk(_settings
                        .chunkUB);
                else _ep = _settings.owner->getMongoSCycle();
            }
        }

        ChunkDispatcher::ChunkDispatcher(Settings settings,
                                               tools::mtools::MongoCluster& mCluster,
                                               EndPointHolder* eph,
                                               tools::mtools::MongoCluster::NameSpace ns) :
                _settings(std::move(settings)),
                _tp(_settings.workThreads),
                _mCluster(mCluster),
                _eph(eph),
                _ns(std::move(ns)),
                _loadPlan(_settings.sortIndex)
        {
            if (_settings.writeConcern == -1)
                _wc = mongo::WriteConcern::majority;
            else
                _wc.nodes(_settings.writeConcern);
            init();
        }

        void ChunkDispatcher::init() {
            //shardChunkCounters keeps track of the number of chunk depth per shard
            //Assumes the chunks are in sorted order so that the queues are correct per shard
            std::unordered_map<tools::mtools::MongoCluster::ShardName, size_t> shardChunkCounters;
            for (auto& iCm : _mCluster.nsChunks(ns())) {
                _loadPlan.insertUnordered(std::get<0>(iCm), ChunkDispatchPointer {});
                size_t depth = ++(shardChunkCounters[std::get<1>(iCm)->first]);
                _loadPlan.back() = ChunkDispatcherFactory::createObject(_settings.loadQueues->at(depth - 1), this, _eph, std::get<0>(iCm));
            }
        }

        ChunkDispatcher::OrderedWaterFall ChunkDispatcher::getWaterFall() {
            std::unordered_map<tools::mtools::MongoCluster::ShardName, std::deque<ChunkDispatchInterface*>> chunksort;
            for (auto& i : _mCluster.nsChunks(_ns))
                chunksort[std::get<1>(i)->first].emplace_back(getDispatchForChunk(std::get<0>(i)));
            OrderedWaterFall wf;
            for (;;) {
                bool added = false;
                for (auto& i : chunksort) {
                    auto& q = i.second;
                    if (q.size()) {
                        added = true;
                        wf.push_back(q.back());
                        q.pop_back();
                    }
                }
                if (!added) break;
            }
            return wf;
        }

        void RAMQueueDispatch::finalize() {
            _queue.sort(Compare(tools::BsonCompare(owner()->sortIndex())));
            tools::mtools::DataQueue sendQueue;
            size_t queueSize = owner()->queueSize();
            for (auto& i : _queue.unSafeAccess()) {
                sendQueue.emplace_back(i.second);
                if (sendQueue.size() >= queueSize) {
                    send(&sendQueue);
                    sendQueue.clear();
                    sendQueue.reserve(queueSize);
                }
            }
            if (sendQueue.size())
                send(&sendQueue);
        }

        void DiskQueueBoundedFileDispatch::spill() {
            assert(settings().maxSize);
            bool doBreak = false;
            size_t size{};
            Queue toCompress;

            //Grab the current buffer, will return extra at the end
            tools::MutexUniqueLock lock(_mutex);
            Queue localHolder;
            localHolder.swap(_queue);
            _size = 0;
            lock.release();


            std::string fileCountName = std::to_string(_fileCount++);
            if (fileCountName.size() < 4)
                fileCountName.insert(0, "0", fileCountName.size());
            while (localHolder.size()) {
                auto& container = localHolder.front();
                size_t count{};
                for (const auto& doc: container)
                {
                    //If we have exceeded the max size, end the writes to disk
                    size += doc.objsize();
                    if (size > settings().maxSize) {
                        size -= doc.objsize();
                        doBreak = true;
                        break;
                    }
                    count ++;
                }
                if (doBreak) {
                    //If the first doc checked (i.e. last doc) didn't over size, move the good ones
                    if (count != container.size()) {
                        toCompress.push_back(BsonQ());
                        auto end = container.begin();
                        std::advance(end, count);
                        std::move(container.begin(), end, toCompress.back().begin());
                        //The container shouldn't be drained if we are breaking
                        assert(container.size());
                    }
                    break;
                }
                //If the whole container is good to move, queue it all
                toCompress.emplace_back(std::move(container));
                localHolder.pop_front();

            }

            //Return any elements to the object's queue, we don't check to deque again
            //This thread already has done so once
            //First calc the size
            size_t plusSize{};
            for (const auto& q: localHolder)
                for (const auto& doc: q)
                    plusSize += doc.objsize();
            lock.lock();
            _size += plusSize;
            while (localHolder.size()) {
                _queue.emplace_back(localHolder.back());
                localHolder.pop_back();
            }
            lock.release();

            //Write the file to disk
            mongo::BufBuilder raw(size);
            for (const auto& vec: toCompress)
                for (const auto& doc: vec)
                    raw.appendBuf(doc.objdata(), doc.objsize());

            std::ofstream diskQueue(owner()->workPath() + "chunk" +
                    this->settings().chunkUB.toString() + fileCountName + ".mls1b");

            std::string compressed;
            //TODO: Consider moving to RawCompress and keeping a static buffer
            uint64_t diskSize = snappy::Compress(raw.buf(), raw.len(), &compressed);
            //Structure: Reserve the first byte, format byte, size uint64, payload
            diskQueue << char(0) << static_cast<char>(Compression::snappy) << diskSize << compressed;
        }
    } //namespace dispatch
}  //namespace loader
