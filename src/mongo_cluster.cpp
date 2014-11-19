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
#include <cstddef>
#include "mongo_cluster.h"
#include "mongo_cxxdriver.h"

namespace tools {
    namespace mtools {

        const mongo::BSONObj MongoCluster::CHUNK_SORT = BSON("max" << 1);

        MongoCluster::MongoCluster(const std::string& connStr) :
                _sharded(false)
        {
            std::string error;
            _connStr = mongo::ConnectionString::parse(connStr, error);
            if (!error.empty()) {
                std::cerr << "Unable to parse: " << connStr << "\nExiting" << std::endl;
                exit(EXIT_FAILURE);
            }
            if (!_connStr.isValid())
                throw std::logic_error("Invalid mongo connection string");
            _dbConn.reset(_connStr.connect(error));
            //TODO: make this error more helpful as the reason isn't included
            if (error.size()) {
                std::cerr << "Unable to connect: " << error << std::endl;
                throw std::logic_error("Unable to connect to the mongo cluster");
            }
            loadCluster();
        }

        MongoCluster::~MongoCluster() {
        }

        void MongoCluster::clear() {
            _shards.clear();
            _nsChunks.clear();
            _mongos.clear();
            _shardTags.clear();
            _nsTagRanges.clear();
            _dbs.clear();
        }

        template<typename IndexType, typename MappingType>
        void MongoCluster::loadIndex(IndexType* index, const std::string& queryNs, MappingType* linkmap,
                                     const std::string& mappingName, const std::string group,
                                     const mongo::BSONObj& key) {
            //The indexes held type
            using index_mapped_type = typename IndexType::mapped_type;
            index_mapped_type* idx = nullptr;
            mongo::Cursor cur = _dbConn->query(queryNs, mongo::Query().sort(BSON(group << 1)));
            std::string prevNs;
            while (cur->more()) {
                mongo::BSONObj obj = cur->next();
                std::string mappingValue = obj.getStringField(mappingName);
                assert(!mappingValue.empty());
                std::string ns = obj.getStringField(group);
                assert(!ns.empty());
                if (ns != prevNs) {
                    if (idx) idx->finalize();
                    prevNs = ns;
                    idx = &index->emplace(ns, index_mapped_type(tools::BSONObjCmp(key))).first->second;
                }
                idx->insertUnordered(obj.getField("max").Obj().getOwned(), linkmap->find(mappingValue));
            }
            //Sort chunks here.
            if (idx) idx->finalize();
        }

        template<typename MappingType>
        void MongoCluster::loadIndex(NsTagUBIndex* index, const std::string& queryNs, MappingType* linkmap,
                                     const std::string& mappingName, const std::string group,
                                     const mongo::BSONObj& key) {
            //The indexes held type
            using index_mapped_type = typename NsTagUBIndex::mapped_type;
            index_mapped_type* idx = nullptr;
            mongo::Cursor cur = _dbConn->query(queryNs, mongo::Query().sort(BSON(group << 1)));
            std::string prevNs;
            while (cur->more()) {
                mongo::BSONObj obj = cur->next();
                std::string mappingValue = obj.getStringField(mappingName);
                assert(!mappingValue.empty());
                std::string ns = obj.getStringField(group);
                assert(!ns.empty());
                if (ns != prevNs) {
                    if (idx) idx->finalize();
                    prevNs = ns;
                    idx = &index->emplace(ns, index_mapped_type(tools::BSONObjCmp(key))).first->second;
                }
                idx->insertUnordered(obj.getField("max").Obj().getOwned(),
                                     TagRange(linkmap->find(mappingValue),
                                              obj.getField("max").Obj().getOwned(),
                                              obj.getField("min").Obj().getOwned()));
            }
            //Sort chunks here.
            if (idx) idx->finalize();
        }

        void MongoCluster::loadCluster() {
            clear();
            //TODO: Add a sanity check this is actually a mongoS/ config server
            //Load shards && tag map
            mongo::Cursor cur = _dbConn->query("config.shards", mongo::BSONObj());
            while (cur->more()) {
                mongo::BSONObj obj = cur->next();
                std::string shard = obj.getStringField("_id");
                std::string connect = obj.getStringField("host");
                size_t shardnamepos = connect.find_first_of('/');
                //If this is a replica the name starts: replicaName/<host list>, standalone: <host>
                if (shardnamepos != std::string::npos)
                    connect = "mongodb://" + connect.substr(shardnamepos + 1) + "/?replicaSet=" + connect.substr(0, shardnamepos);
                else
                    connect = "mongodb://" + connect;
                if (shard.empty() || connect.empty())
                    throw std::logic_error("Couldn't load shards, empty values, is this a "
                            "sharded cluster?");
                std::string tag = obj.getStringField("tag");
                auto sharditr = _shards.emplace(std::move(shard), std::move(connect)).first;
                if (!tag.empty())
                    _shardTags[tag].push_back(sharditr);
            }
            _sharded = _shards.size();

            //Load shard chunk ranges
            loadIndex(&_nsChunks, "config.chunks", &_shards, "shard");
            //Load tag bound ranges
            loadIndex(&_nsTagRanges, "config.tags", &_shardTags, "tag");

            //Get all the mongoS
            cur = _dbConn->query("config.mongos", mongo::BSONObj());
            while (cur->more()) {
                mongo::BSONObj o = cur->next();
                _mongos.emplace_back(std::string("mongodb://") + o.getStringField("_id"));
            }

            cur = _dbConn->query("config.databases", mongo::BSONObj());
            while (cur->more()) {
                mongo::BSONObj obj = cur->next();
                std::string dbName = obj.getStringField("_id");
                _dbs.emplace(std::make_pair(dbName,
                        MetaDatabase(dbName, obj.getBoolField("partitioned"), obj.getStringField("primary"))));
            }

            cur = _dbConn->query("config.collections", mongo::BSONObj());
            DatabaseName prevDb;
            while (cur->more()) {
                mongo::BSONObj obj = cur->next();
                NameSpace currNs = obj.getStringField("_id");
                _colls.emplace(std::make_pair(currNs, MetaNameSpace(currNs, obj.getBoolField("dropped"),
                               obj.getObjectField("key").getOwned(), obj.getBoolField("unique"))));
            }

        }

        MongoCluster::ShardMap MongoCluster::getShardList() {
            ShardMap shardMap(shards().begin(), shards().end());
            return std::move(shardMap);
        }

        bool MongoCluster::balancerIsRunning() {
            mongo::Cursor cursor = _dbConn->query("config.locks", BSON("state" << BSON("$gt" << 0)));
            if (!cursor->more())
                return false;
            return true;
        }

        void MongoCluster::balancerStop() {
            mongo::BSONObj query = BSON("_id" << "balancer");
            mongo::BSONObj update = BSON("$set" << BSON("stopped" << true));
            mongo::BSONObj info;

            _dbConn->update("config.settings", query, update, true);
        }

        bool MongoCluster::stopBalancerWait(std::chrono::seconds wait) {
            balancerStop();
            if (wait > std::chrono::seconds(0)) {
                using time = std::chrono::high_resolution_clock;
                time::time_point start = time::now();
                while (balancerIsRunning() && (time::now() - start < wait)) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                return balancerIsRunning();
            }
            else {
                while (balancerIsRunning())
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                return false;
            }

        }

        void MongoCluster::waitForChunksPerShard(std::string ns, int chunksPerShard) {
            mongo::BSONObj aggOpts = mongo::BSONObjBuilder().append("allowDiskUse", true)
                        .append("cursor", BSON("batchSize" << 10000)).obj();

            mongo::BSONObj agg = BSON_ARRAY(BSON("$match" << BSON("ns" << ns))
                    << BSON("$group" << BSON("_id" << "$shard" << "chunkCount" << BSON("$sum" << 1))));

            bool done;
            do {
                done = true;
                mongo::Cursor cursor = _dbConn->aggregate("config.chunks", agg, &aggOpts);
                if(!cursor->more()) {
                    std::cerr << "Unable to find chunks for namespace: " << ns << std::endl;
                    exit(EXIT_FAILURE);
                }
                while(cursor->more()) {
                    mongo::BSONObj obj = cursor->next();
                    mongo::BSONElement count = obj.getField("chunkCount");
                    if (count.Int() < chunksPerShard) {
                        done = false;
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        continue;
                    }
                }
            } while (!done);
        }

        bool MongoCluster::enableSharding(const DatabaseName &dbName, mongo::BSONObj* info) {
            mongo::BSONObj cmd = BSON("enableSharding" << dbName);
            return _dbConn->runCommand("admin", cmd, *info);
        }

        bool MongoCluster::shardCollection(const NameSpace &ns, const mongo::BSONObj &shardKey,
                                           bool unique, mongo::BSONObj *info) {
            mongo::BSONObj shardCmd = BSON("shardCollection" << ns
                                       << "key" << shardKey);
            return _dbConn->runCommand("admin", shardCmd, *info);
        }

        bool MongoCluster::shardCollection(const NameSpace& ns, const mongo::BSONObj &shardKey,
                                           const bool unique, const int chunks,
                                           mongo::BSONObj *info) {
            mongo::BSONObj shardCmd = BSON("shardCollection" << ns
                                       << "key" << shardKey
                                       << "numInitialChunks" << chunks);
            return _dbConn->runCommand("admin", shardCmd, *info);
        }

        void MongoCluster::flushRouterConfigs() {
            std::unique_ptr<mongo::DBClientBase> conn;
            for (auto&& itr : _mongos) {
                std::string error;
                conn.reset(mongo::ConnectionString(itr, mongo::ConnectionString::ConnectionType::SET).connect(error));
                if (!error.empty()) {
                    //continue on error, mongoS may be down which is fine
                    std::cerr << "Unable to connect to router (for flush): " << itr << std::endl;
                    continue;
                }
                mongo::BSONObj info;
                if(!conn->simpleCommand("", &info, "flushRouterConfig"))
                    std::cout << "Failed to flush router config (" << itr << "): " << info << std::endl;
            }
        }

        std::ostream& operator<<(std::ostream& ostream, const MongoCluster& cluster) {
            ostream << "Shards:" << "\n";
            for (auto&& i : cluster.shards())
                ostream << i.first << " : " << i.second << "\n";

            ostream << "\nChunks:" << "\n";
            for (auto&& i : cluster.nsChunks()) {
                ostream << i.first << "\n";
                for (auto&& s : i.second.container())
                    ostream << "\tUpper Bound: " << s.first << " Shard: " << s.second->first << "\n";
            }
            ostream << "\nMongoS:" << "\n";
            for (auto&& i : cluster.mongos())
                ostream << i << "\n";

            ostream << "\nTags" << "\n";
            for (auto&& i : cluster.shardTags()) {
                ostream << "Tag: " << i.first << ".  Shards: ";
                for (auto&& shard : i.second)
                    ostream << shard->first;
                ostream << "\n";
            }

            ostream << "\nTag Ranges:" << "\n";
            for (auto&& i : cluster.nsTagRanges()) {
                ostream << i.first << "\n";
                for (auto&& s : i.second.container())
                    ostream << s.second << "\n";
            }

            return ostream;
        }

    }  //mespace mtools
}  //namespace tools
