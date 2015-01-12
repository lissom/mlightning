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

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <string>
#include <mongo/client/dbclient.h>
#include <thread>
#include <vector>
#include "bson_tools.h"
#include "index.h"
#include "tools.h"

namespace tools {
    namespace mtools {
        //TODO: replace asserts with exceptions
        /*
         * Represents a mongo cluster
         * All required cluster information can be taken from here.  (i.e. this is the config database)
         * IT IS CRITICAL THAT CHUNKS ARE SORTED IN ASCENDING ORDER
         * All other operations rely on upper_bound/sort being correct and they load the chunks
         * from here.
         *
         * Mongo cluster isn't designed for cluster modification activity to be multi-threaded
         */
        class MongoCluster {
        public:
            //The sort for chunks.  "max": 1
            static const mongo::BSONObj CHUNK_SORT;
            using DatabaseName = std::string;
            using NameSpace = std::string;
            using ShardName = std::string;
            using ShardConn = std::string;
            using ShardTag = std::string;
            using ChunkIndexKey = mongo::BSONObj;
            struct MetaNameSpace {
                NameSpace ns;
                bool dropped;
                mongo::BSONObj key;
                bool unique;

                MetaNameSpace(NameSpace ns__, const bool dropped__, mongo::BSONObj key__,
                              const bool unique__) :
                                  ns(std::move(ns__)), dropped(dropped__), key(std::move(key__)),
                                  unique(unique__)
                                    { }



            };
            struct MetaDatabase {
                DatabaseName name;
                bool partitioned;
                ShardName primary;
                MetaDatabase(DatabaseName name__, const bool partitioned__, ShardName primary__) :
                    name(std::move(name__)), partitioned(partitioned__),
                    primary(std::move(primary__))
                { }
            };
            //ShardMap is a map of mongo connection string to shards
            using ShardMap = std::unordered_map<ShardName, ShardConn>;
            //Shard chunk range upper bound map
            using ShardBsonIndex = tools::Index<ChunkIndexKey, ShardMap::iterator, tools::BSONObjCmp>;
            //Chunks in a namespace map
            using NsUBIndex = std::unordered_map<NameSpace, ShardBsonIndex>;
            //Tagged sharding tag->shards
            using TagShards = std::unordered_map<ShardTag, std::vector<ShardMap::iterator>>;
            //Tagged sharding ranges
            struct TagRange {
                TagShards::iterator tagShards;
                mongo::BSONObj max;
                mongo::BSONObj min;

                TagRange(TagShards::iterator tagShards__, mongo::BSONObj max__, mongo::BSONObj min__) :
                    tagShards(tagShards__), max(max__), min(min__) {}

                friend std::ostream& operator<<(std::ostream& ostream, const TagRange& range) {
                    ostream << "max: " << range.max << " min: " << range.min << " shards: ";
                    bool first = true;
                    for (auto&& shards: range.tagShards->second) {
                        if (!first)
                            ostream << ", ";
                        else
                            first = false;
                        ostream << shards->first;
                    }
                    return ostream;
                }
            };
            using TagBsonIndex = tools::Index<ChunkIndexKey, TagRange, tools::BSONObjCmp>;
            using NsTagUBIndex = std::unordered_map<NameSpace, TagBsonIndex>;
            using Mongos = std::vector<std::string>;
            MongoCluster() = delete;
            explicit MongoCluster(const std::string& conn);
            virtual ~MongoCluster();

            /**
             * Did the load detect a shard
             */
            bool isSharded() {
                return _sharded;
            }

            bool exists(const NameSpace &ns) {
                return _colls.count(ns) > 0;
            }

            mongo::ConnectionString& connStr() {
                return _connStr;
            }

            const mongo::ConnectionString& connStr() const {
                return _connStr;
            }

            /**
             * Returns that shards as in the config.shards namespace
             */
            ShardMap getShardList();

            /**
             * @return access to shards and their connection strings
             */
            ShardMap& shards() {
                return _shards;
            }

            const ShardMap& shards() const {
                return _shards;
            }

            /**
             * @return access to namespace chunks
             */
            NsUBIndex& nsChunks() {
                return _nsChunks;
            }

            const NsUBIndex& nsChunks() const {
                return _nsChunks;
            }

            TagShards& shardTags() {
                return _shardTags;
            }

            const TagShards& shardTags() const {
                return _shardTags;
            }

            NsTagUBIndex& nsTagRanges() {
                return _nsTagRanges;
            }

            const NsTagUBIndex& nsTagRanges() const {
                return _nsTagRanges;
            }


            /**
             * All chunks for a single namespace
             */
            ShardBsonIndex& nsChunks(const std::string& ns) {
                return _nsChunks.at(ns);
            }

            const ShardBsonIndex& nsChunks(const std::string& ns) const {
                return _nsChunks.at(ns);
            }
            /**
             * access to mongos.
             */
            Mongos& mongos() {
                return _mongos;
            }

            const Mongos& mongos() const {
                return _mongos;
            }

            /**
             * MongoDB commands.  These operate exactly like the manual stats
             */
            bool enableSharding(const DatabaseName& dbName, mongo::BSONObj* info);

            bool shardCollection(const NameSpace& ns, const mongo::BSONObj& shardKey,
                                 const bool unique, mongo::BSONObj *info);
            //Presharding for a hashed shard key
            bool shardCollection(const NameSpace& ns, const mongo::BSONObj& shardKey,
                                 const bool unique, int chunk, mongo::BSONObj *info);

            //Flush all router configs
            void flushRouterConfigs();

            //End MongoDB commands

            /**
             * Queries for active locks
             * @return is the balancer running (are there active locks?)
             */
            bool balancerIsRunning();

            /**
             * Tell mongoS to stop the balancer
             */
            void balancerStop();
            /**
             * Stops the balancer and then checks to see if it is running
             * @return true if running, false if not
             */
            bool stopBalancerWait(std::chrono::seconds wait);

            /**
             * Wait for all shards in a namespace to have X or more chunks
             */
            void waitForChunksPerShard(std::string ns, int chunksPerShard);

            /**
             * @return count of chunks for a single namespace
             */
            size_t chunksCount(const std::string& ns) const {
                auto i = _nsChunks.find(ns);
                if (i == _nsChunks.end()) return 0;
                return i->second.size();
            }

            /**
             * @return connection string for a shard
             */
            const std::string& getConn(const std::string& shard) {
                return _shards.at(shard);
            }

            /**
             * @return given a namespace and chunk give back the shard it resides on
             */
            ShardName getShardForChunk(const std::string& ns, const ChunkIndexKey& key) {
                return _nsChunks.at(ns).at(key)->first;
            }

            /**
             * loads values from the cluster from the _connStr string
             */
            void loadCluster();

            /**
             * Appends to a container a list of the shards.
             * The container is NOT cleared.
             */
            template<typename T>
            void getShardList(T* queue) const {
                for (auto& i : _shards)
                    queue->push_back(i.first);
            }

            /**
             * Writes the chunk config to the ostream
             */
            friend std::ostream& operator<<(std::ostream& o, const MongoCluster& c);

        private:
            mongo::ConnectionString _connStr;
            Mongos _mongos;
            ShardMap _shards;
            NsUBIndex _nsChunks;
            TagShards _shardTags;
            NsTagUBIndex _nsTagRanges;
            std::unordered_map<DatabaseName, MetaDatabase> _dbs;
            std::unordered_map<NameSpace, MetaNameSpace> _colls;
            std::unique_ptr<mongo::DBClientBase> _dbConn;
            /*
             * Stores if sharding info could be loaded
             */
            bool _sharded;

            /**
             * clears all values for the loaded cluster
             */
            void clear();

            //These private use templates are defined in the .cpp file
            /**
             * Loads an index from a config collection.  ns and max are always true at this time
             * It then links to the mapping type
             * Indextype is assumed to be constructible from a <string, tools::Index>
             */
            template<typename IndexType, typename MappingType>
            void loadIndex(IndexType* index, const std::string& queryNs, MappingType* linkmap,
                 const std::string& mappingName, const std::string group = "ns",
                 const mongo::BSONObj& key = CHUNK_SORT);
            /**
             * Specialization for more complex mapping
             */
            template<typename MappingType>
            void loadIndex(NsTagUBIndex* index, const std::string& queryNs, MappingType* linkmap,
                             const std::string& mappingName, const std::string group = "ns",
                             const mongo::BSONObj& key = CHUNK_SORT);
        };

        std::ostream& operator<<(std::ostream& ostream, MongoCluster& cluster);

    }  //namespace mtools
}  //namespace tools
