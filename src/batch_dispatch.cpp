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

#include "batch_dispatch.h"

namespace loader {
    namespace dispatch {

        const bool DirectDispatch::factoryRegisterCreator = ChunkDispatcherFactory::registerCreator(
                "direct",
                &DirectDispatch::create);
        const bool RAMQueueDispatch::factoryRegisterCreator = ChunkDispatcherFactory::registerCreator(
                "ram",
                &RAMQueueDispatch::create);

        ChunkDispatchInterface::ChunkDispatchInterface(Settings settings) :
                _settings(std::move(settings)),
                _bulkWriteVersion(_settings.owner->bulkWriteVersion())
        {
            if (_settings.owner->directLoad()) _ep = _settings.owner->getEndPointForChunk(_settings
                    .chunkUB);
            else _ep = _settings.owner->getMongoSCycle();
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

        void RAMQueueDispatch::doLoad() {
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
    }
}  //namespace loader
