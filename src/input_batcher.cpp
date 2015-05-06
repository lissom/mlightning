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

#include "input_batcher.h"

namespace loader {
namespace docbuilder {

const bool DirectQueue::factoryRegisterCreator = ChunkBatchFactory::registerCreator("direct",
        &DirectQueue::create);
const bool RAMQueue::factoryRegisterCreator = ChunkBatchFactory::registerCreator("ram",
        &RAMQueue::create);
const bool DiskQueue::factoryRegisterCreator0 = ChunkBatchFactory::registerCreator("mltn",
        &DiskQueue::create);
const bool DiskQueue::factoryRegisterCreator1 = ChunkBatchFactory::registerCreator("presplit",
        &DiskQueue::create);


ChunkBatcherInterface::ChunkBatcherInterface(InputChunkBatcherHolder* owner, Bson UBIndex) :
        _owner(owner), _queueSize(_owner->settings().queueSize), _dispatcher(
                _owner->getDispatchForChunk(UBIndex)), _UBIndex(std::move(UBIndex)) {

}

void InputChunkBatcherHolder::init(const mtools::NameSpace& ns) {
    //shardChunkCounters keeps track of the number of chunk depth per shard
    //Assumes the chunks are in sorted order so that the queues are correct per shard
    std::unordered_map<mtools::ShardName, size_t> shardChunkCounters;
    for (auto&& iCm : _mCluster.nsChunks(ns)) {
        _inputPlan.insertUnordered(std::get<0>(iCm), ChunkBatcherPointer { });
        size_t depth = ++(shardChunkCounters[std::get<1>(iCm)->first]);
        //If there aren't enough queues given, then use the first one given for all next
        try {
            _inputPlan.back() = ChunkBatchFactory::createObject(_settings.loadQueues->at(depth - 1),
                    this, std::get<0>(iCm));
        } catch (std::out_of_range &e) {
            _inputPlan.back() = ChunkBatchFactory::createObject(_settings.loadQueues->back(), this,
                    std::get<0>(iCm));
        }
    }
    assert(_inputPlan.size());
}

void InputChunkBatcherHolder::cleanUpAllQueues() {
    for (auto& i : _inputPlan)
        i.second->cleanUpQueue();
}
}  //namespace queue
}  //namespace loader
