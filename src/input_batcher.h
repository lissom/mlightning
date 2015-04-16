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

#include <memory>
#include <unordered_map>
#include "batch_dispatch.h"
#include "factory.h"
#include "index.h"
#include "loader_defs.h"
#include "mongo_cxxdriver.h"
#include "mongo_cluster.h"

namespace loader {
    namespace docbuilder {

        class InputChunkBatcherHolder;
        class ChunkBatcherInterface;

        using ChunkBatcherPointer = std::unique_ptr<ChunkBatcherInterface>;

        using CreateBatcherFunction =
                std::function<ChunkBatcherPointer(InputChunkBatcherHolder* owner,
                const Bson& UBIndex)>;
        /*
         * This factory is used to create chunks for settings file
         * NOTE: the keys for batch_dispact and input_batcher must use the same value
         * to build a valid pipeline across both.
         */
        using ChunkBatchFactory = tools::RegisterFactory<ChunkBatcherPointer,
                CreateBatcherFunction
                >;

        //Insert data needs to hold the index, location, and any generated information
        using Key = Bson;

        /**
         * Document builder creates insertable BSON documents
         * It doesn't guarantee anything about call order of the functions
         * Therefore the functions should be completely independent
         *
         */
        class DocumentBuilderInterface {
        public:
            DocumentBuilderInterface() { }
            virtual ~DocumentBuilderInterface() { }
            /**
             * Returns the BSON to the completely built doc
             */
            virtual Bson getFinalDoc() = 0;
            /**
             * Returns the index of the document
             * We currently only support a single index which is the sort/shard key
             */
            virtual Bson getIndex() = 0;
            /**
             * Returns any fields that the builder added to the document
             */
            virtual Bson getAdd() = 0;
            /**
             * Returns the document location on in the original data set
             */
            virtual tools::DocLoc getLoc() = 0;
        };

        /*
         * Public interface for getting bson documents into large batches by chunk
         * Documents should be pushed into.
         */
        class ChunkBatcherInterface {
        public:
            /**
             * Push is called when the LoadBuilder is ready to have any values required read
             */
            virtual void push(DocumentBuilderInterface* stage) = 0;

            /**
             *  Makes a final push to clear the queue
             */
            virtual void cleanUpQueue() = 0;

            virtual ~ChunkBatcherInterface() {
            }

            /**
             * @return the opAggregator that the queue should post to
             */
            dispatch::ChunkDispatchInterface* postTo() {
                return _dispatcher;
            }

            /**
             * @return the holder
             */
            InputChunkBatcherHolder* owner() {
                return _owner;
            }

            const size_t queueSize() const {
                return _queueSize;
            }

            /**
             * @return the index upper bound being used
             */
            const Bson& UBIndex() const {
                return _UBIndex;
            }

        protected:
            ChunkBatcherInterface(InputChunkBatcherHolder* owner, Bson UBIndex);

        private:
            InputChunkBatcherHolder *_owner;
            size_t _queueSize;
            dispatch::ChunkDispatchInterface *_dispatcher;
            const Bson _UBIndex;
        };

        /**
         * InputNameSpaceContainer is not thread safe.  It aggregates documents into batches for passing onto
         * an operation dispatcher.  This is only valid for a single namespace.
         */
        class InputChunkBatcherHolder {
        public:
            struct Settings {
                LoadQueues *loadQueues;
                mongo::BSONObj sortIndex;
                size_t queueSize;
            };

            InputChunkBatcherHolder(Settings settings,
                            tools::mtools::MongoCluster& mCluster,
                            dispatch::ChunkDispatcher* out,
                            tools::mtools::MongoCluster::NameSpace ns) :
                    _settings(std::move(settings)),
                    _mCluster(mCluster),
                    _out(out),
                    _inputPlan(tools::mtools::MongoCluster::CHUNK_SORT),
                    _ns(ns)
            {
                init(_ns);
            }

            ~InputChunkBatcherHolder() {
                cleanUpAllQueues();
            }

            /**
             * @return the stage for a single bson value.
             */
            ChunkBatcherInterface* targetStage(const Bson& indexValue) {
                return _inputPlan.upperBound(indexValue).get();
            }

            /**
             * returns the opAggregator for that upper bound chunk key
             */
            dispatch::ChunkDispatchInterface* getDispatchForChunk(Key key) {
                return out()->at(key).get();
            }

            /**
             * @return the settings this InputNameSpaceContainer is using
             */
            const Settings& settings() const {
                return _settings;
            }

            /**
             * Force all queues to push data upstream
             * This class does *not* push on d'tor, this function must be called
             */
            void cleanUpAllQueues();

        private:
            using InputPlan = tools::Index<tools::mtools::MongoCluster::ChunkIndexKey, ChunkBatcherPointer, tools::BsonCompare>;

            /**
             * Sets up a single name space
             */
            void init(const tools::mtools::MongoCluster::NameSpace& ns);

            const tools::mtools::MongoCluster& cluster() const {
                return _mCluster;
            }

            dispatch::ChunkDispatcher* out() {
                return _out;
            }

            Settings _settings;
            tools::mtools::MongoCluster &_mCluster;
            dispatch::ChunkDispatcher *_out;
            InputPlan _inputPlan;
            tools::mtools::MongoCluster::NameSpace _ns;

        };

        class DirectQueue : public ChunkBatcherInterface {
        public:
            DirectQueue(InputChunkBatcherHolder* owner, Bson UBIndex) :
                    ChunkBatcherInterface(owner, std::move(UBIndex))
            {
                _bsonHolder.reserve(queueSize());
            }

            void push(DocumentBuilderInterface* stage) {
                _bsonHolder.emplace_back(stage->getFinalDoc());
                if (_bsonHolder.size() >= queueSize()) doPush();
            }

            void cleanUpQueue() {
                if (!_bsonHolder.empty()) postTo()->push(&_bsonHolder);
            }

            static ChunkBatcherPointer create(InputChunkBatcherHolder* owner, const Bson& UBIndex) {
                return ChunkBatcherPointer(new DirectQueue(owner, UBIndex));
            }

        private:
            BsonV _bsonHolder;

            static const bool factoryRegisterCreator;

            void doPush() {
                postTo()->push(&_bsonHolder);
                _bsonHolder.reserve(queueSize());
            }
        };

        class RAMQueue : public ChunkBatcherInterface {
        public:
            RAMQueue(InputChunkBatcherHolder* owner, Bson UBIndex) :
                ChunkBatcherInterface(owner, std::move(UBIndex))
            {
            }

            void push(DocumentBuilderInterface* stage) {
                _bsonHolder.emplace_back(std::make_pair(stage->getIndex(), stage->getFinalDoc()));
                if (_bsonHolder.size() > queueSize()) doPush();
            }

            void cleanUpQueue() {
                if (!_bsonHolder.empty()) postTo()->pushSort(&_bsonHolder);
            }

            static ChunkBatcherPointer create(InputChunkBatcherHolder* owner, Bson UBIndex)
            {
                return ChunkBatcherPointer(new RAMQueue(owner, std::move(UBIndex)));
            }

        private:
            static const bool factoryRegisterCreator;
            BsonPairDeque _bsonHolder;

            void doPush() {
                postTo()->pushSort(&_bsonHolder);
            }

        };

        class DiskQueue : public ChunkBatcherInterface {
        public:
            DiskQueue(InputChunkBatcherHolder* owner, Bson UBIndex) :
                ChunkBatcherInterface(owner, std::move(UBIndex))
            {
            }

            void push(DocumentBuilderInterface* stage) {
                _bsonHolder.emplace_back(stage->getFinalDoc());
                if (_bsonHolder.size() > queueSize()) doPush();
            }

            void cleanUpQueue() {
                if (!_bsonHolder.empty()) doPush();
            }

            static ChunkBatcherPointer create(InputChunkBatcherHolder* owner, Bson UBIndex)
            {
                return ChunkBatcherPointer(new RAMQueue(owner, std::move(UBIndex)));
            }

        private:
            static const bool factoryRegisterCreator;
            BsonQ _bsonHolder;

            void doPush() {
                postTo()->pushQ(&_bsonHolder);
            }

        };

        /*
         * work in progress, ignore
         * being use to examine different disk queues, currently all of them are too disk intensive
         */
        class IndexedBucketQueue : public DirectQueue {
        public:
            IndexedBucketQueue(InputChunkBatcherHolder* owner, const Bson& UBIndex) :
                    DirectQueue(owner, UBIndex)
            {
            }

            static ChunkBatcherPointer create(InputChunkBatcherHolder* owner,
                                           const Bson& UBIndex,
                                           const Bson& index)
            {
                return ChunkBatcherPointer(new IndexedBucketQueue(owner, UBIndex));
            }
        };
    }  //namespace queue
} //namespace loader

