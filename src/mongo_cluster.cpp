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
#include <cstring>
#include "mongo_cluster.h"
#include "mongo_cxxdriver.h"

namespace mtools {

const mongo::BSONObj& MongoCluster::ConfigChunkSort() {
    static const mongo::BSONObj ChunkSort = BSON("max" << 1);
    return ChunkSort;
}

MongoCluster::MongoCluster(const std::string& connStr) {
    _connStr = mongo::parseConnectionOrThrow(connStr);
    _dbConn = mongo::connectOrThrow(_connStr);
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
        const std::string& mappingName, const std::string group, const mongo::BSONObj& key) {
    //The indexes held type
    using index_mapped_type = typename IndexType::mapped_type;
    index_mapped_type* idx = nullptr;
    auto cur = _dbConn->query(queryNs, mongo::Query().sort(BSON(group << 1)));
    std::string prevNs;
    while (cur->more()) {
        mongo::BSONObj obj = cur->next();
        const std::string mappingValue = obj.getStringField(mappingName);
        assert(!mappingValue.empty());
        const std::string ns = obj.getStringField(group);
        assert(!ns.empty());
        if (ns != prevNs) {
            if (idx)
                idx->finalize();
            prevNs = ns;
            idx = &index->emplace(ns, index_mapped_type(mtools::BsonCompare(key))).first->second;
        }
        idx->insertUnordered(obj.getField("max").Obj().getOwned(), linkmap->find(mappingValue));
    }
    //Sort chunks here
    if (idx)
        idx->finalize();
}

MongoCluster::ShardsChunks MongoCluster::getShardChunks(const NameSpace &ns) {
    MongoCluster::ShardsChunks shardChunks;
    //Use the internal getShardChunks here so synth shards are honored
    //The range of upper bounds is transformed into a range per chunk
    auto&& chunks = _nsChunks.find(ns);
    if (chunks != _nsChunks.end() && chunks->second.size()) {
        auto chunk = chunks->second.begin();
        mongo::BSONObj minKey = generateMinKey(chunk->first);
        mongo::BSONObj* prevMinKey = &minKey;
        for (; chunk != chunks->second.end(); ++chunk) {
            assert(chunk->first > *prevMinKey);
            shardChunks[chunk->second->first].emplace_back(ChunkRange(chunk->first, *prevMinKey));
            prevMinKey = &chunk->first;
        }
    }
    return std::move(shardChunks);
}

template<typename MappingType>
void MongoCluster::loadIndex(NsTagUBIndex* index, const std::string& queryNs, MappingType* linkmap,
        const std::string& mappingName, const std::string group, const mongo::BSONObj& key) {
    //The indexes held type
    using index_mapped_type = typename NsTagUBIndex::mapped_type;
    index_mapped_type* idx = nullptr;
    auto cur = _dbConn->query(queryNs, mongo::Query().sort(BSON(group << 1)));
    std::string prevNs;
    while (cur->more()) {
        mongo::BSONObj obj = cur->next();
        std::string mappingValue = obj.getStringField(mappingName);
        assert(!mappingValue.empty());
        std::string ns = obj.getStringField(group);
        assert(!ns.empty());
        if (ns != prevNs) {
            if (idx)
                idx->finalize();
            prevNs = ns;
            idx = &index->emplace(ns, index_mapped_type(mtools::BsonCompare(key))).first->second;
        }
        idx->insertUnordered(obj.getField("max").Obj().getOwned(),
                TagRange(linkmap->find(mappingValue), obj.getField("max").Obj().getOwned(),
                        obj.getField("min").Obj().getOwned()));
    }
    //Sort tags here
    if (idx)
        idx->finalize();
}

std::string MongoCluster::generateShardConnection(const std::string& shardDbConn) {
    std::string connectionString;
    size_t shardnamepos = shardDbConn.find_first_of('/');
    //If this is a replica the name starts: replicaName/<host list>, standalone: <host>
    if (shardnamepos != std::string::npos)
        connectionString = "mongodb://" + shardDbConn.substr(shardnamepos + 1) + "/?replicaSet="
                + shardDbConn.substr(0, shardnamepos);
    else
        connectionString = "mongodb://" + shardDbConn;
    return std::move(connectionString);
}

void MongoCluster::loadCluster() {
    clear();
    //TODO: Add a sanity check this is actually a mongoS/ config server
    //Load shards && tag map
    auto cur = _dbConn->query("config.shards", mongo::BSONObj());
    //If there are no shards, assume connection to a standalone/replica
    if (!cur->more()) {
        std::string shardName = "SYNTHShard1";
        std::string shardConn = generateShardConnection(_connStr.toString());
        (void) _shards.emplace(shardName, std::move(shardConn));
        auto dbNames = _dbConn->getDatabaseNames();
        for (auto&& i : dbNames) {
            //If a config database exists, bail, to be on the safe side
            if (i == "config")
                throw std::logic_error("Found a config database, but no shards");
            _dbs.emplace(std::make_pair(i, MetaDatabase(i, false, shardName, true)));
        }
        _sharded = false;
        return;
    }
    while (cur->more()) {
        auto obj = cur->next();
        const std::string shard = obj.getStringField("_id");
        std::string connect = generateShardConnection(obj.getStringField("host"));
        if (shard.empty() || connect.empty())
            throw std::logic_error("Couldn't load shards, empty required values for a shard"
                    " name and/or connection");
        std::string tag = obj.getStringField("tag");
        auto sharditr = _shards.emplace(std::move(shard), std::move(connect)).first;
        if (!tag.empty())
            _shardTags[tag].push_back(sharditr);
    }
    _sharded = true;

    //Load shard chunk ranges
    loadIndex(&_nsChunks, "config.chunks", &_shards, "shard");
    //Load tag bound ranges
    loadIndex(&_nsTagRanges, "config.tags", &_shardTags, "tag");

    //Get all the mongoS
    cur = _dbConn->query("config.mongos", mongo::BSONObj());
    while (cur->more()) {
        auto o = cur->next();
        _mongos.emplace_back(std::string("mongodb://") + o.getStringField("_id"));
    }

    cur = _dbConn->query("config.databases", mongo::BSONObj());
    while (cur->more()) {
        auto obj = cur->next();
        const std::string dbName = obj.getStringField("_id");
        _dbs.emplace(
                std::make_pair(dbName,
                        MetaDatabase(dbName, obj.getBoolField("partitioned"),
                                obj.getStringField("primary"))));
    }

    cur = _dbConn->query("config.collections", mongo::BSONObj());
    DatabaseName prevDb;
    while (cur->more()) {
        auto obj = cur->next();
        const NameSpace currNs = obj.getStringField("_id");
        _colls.emplace(
                std::make_pair(currNs,
                        MetaNameSpace(currNs, obj.getBoolField("dropped"),
                                obj.getObjectField("key").getOwned(), obj.getBoolField("unique"))));
    }

}

MongoCluster::ShardMap MongoCluster::getShardList() const {
    ShardMap shardMap(shards().begin(), shards().end());
    return std::move(shardMap);
}

mongo::BSONObj MongoCluster::getShardKeyAsBson(NameSpace ns) {
    const auto projection = BSON("key" << 1);
    auto keyField = _dbConn->findOne("config.collections", BSON("_id" << ns), &projection);
    if (keyField.isEmpty())
        return BSON("_id" << 1);
    return keyField.getObjectField("key").getOwned();
}

bool MongoCluster::splitVector(mongo::BSONObj* result, const NameSpace& ns,
        const mongo::BSONObj& shardKey, const long long maxChunkSizeBytes) {
    mongo::BSONObjBuilder bob;
    bob.append("splitVector", ns).append("keyPattern", shardKey).append("maxChunkSizeBytes",
            maxChunkSizeBytes);
    return _dbConn->runCommand("admin", bob.obj(), *result);
}

bool MongoCluster::balancerIsRunning() {
    mongo::Cursor cursor = _dbConn->query("config.locks",
            BSON("_id" << "balancer" << "state" << BSON("$gt" << 0)));
    if (!cursor->more())
        return false;
    return true;
}

void MongoCluster::stopBalancer() {
    auto query = BSON("_id" << "balancer");
    auto update = BSON("$set" << BSON("stopped" << true));
    _dbConn->update("config.settings", query, update, true);
    std::string lastError = _dbConn->getLastError();
    if (!lastError.empty())
        std::cerr << "Failed to stop balancer. Error: " << lastError << std::endl;
}

bool MongoCluster::waitForBalancerToStop(std::chrono::seconds wait) {
    if (wait > std::chrono::seconds(0)) {
        using time = std::chrono::high_resolution_clock;
        time::time_point start = time::now();
        while (balancerIsRunning() && (time::now() - start < wait)) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        return balancerIsRunning();
    } else {
        while (balancerIsRunning())
            std::this_thread::sleep_for(std::chrono::seconds(1));
        return false;
    }
}

bool MongoCluster::stopBalancerWait(std::chrono::seconds wait) {
    stopBalancer();
    return waitForBalancerToStop(wait);
}

bool MongoCluster::isBalancerEnabled() {
    auto result = _dbConn->findOne("config.settings",
            BSON("_id" << "balancer" << "stopped" << true));
    if (strcmp(result.firstElementFieldName(), "$err") == 0) {
        throw std::logic_error(
                "Unable to get query for balancer state in bool MongoCluster::isBalancerEnabled()");
    }
    return !strcmp("stopped", result.getStringField("balancer"));
}

bool MongoCluster::isBalancingEnabled(const NameSpace &ns) {
    auto result = _dbConn->findOne("config.collections", BSON("_id" << ns << "noBalance" << true));
    //If there are no results, then it's no disabled
    if (result.isEmpty())
        return false;
    if (strcmp(result.firstElementFieldName(), "$err") == 0) {
        throw std::logic_error(
                "Unable to get query for balancer state in bool MongoCluster::isBalancingEnabled(const NameSpace &ns)");
    }
    return true;
}

bool MongoCluster::disableBalancing(const NameSpace &ns) {
    _dbConn->update("config.collections", BSON("_id" << ns),
            BSON("$set" << BSON("noBalance" << true)));
    std::string lastError = _dbConn->getLastError();
    if (lastError.empty())
        return true;
    std::cerr << "Failed to disable balancing for name space \"" << ns << "\". Error: " << lastError
            << std::endl;
    return false;
}

bool MongoCluster::enableBalancing(const NameSpace &ns) {
    _dbConn->update("config.collections", BSON("_id" << ns),
            BSON("$set" << BSON("noBalance" << false)));
    std::string lastError = _dbConn->getLastError();
    if (lastError.empty())
        return true;
    std::cerr << "Failed to enable balancing for name space \"" << ns << "\". Error: " << lastError
            << std::endl;
    return false;
}

void MongoCluster::waitForChunksPerShard(std::string ns, int chunksPerShard) {
    auto aggOpts = mongo::BSONObjBuilder().append("allowDiskUse", true).append("cursor",
            BSON("batchSize" << 10000)).obj();
    //Get the numbers returned in case we ever want to output
    mongo::BSONObj agg =
            BSON_ARRAY(
                    BSON("$match" << BSON("ns" << ns)) << BSON("$group" << BSON("_id" << "$shard" << "chunkCount" << BSON("$sum" << 1))));
    bool done;
    do {
        done = true;
        mongo::Cursor cursor = _dbConn->aggregate("config.chunks", agg, &aggOpts);
        if (!cursor->more()) {
            std::cerr << "Unable to find chunks for namespace: " << ns << std::endl;
            exit(EXIT_FAILURE);
        }
        while (cursor->more()) {
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

bool MongoCluster::enableSharding(const DatabaseName& dbName, mongo::BSONObj& info) {
    auto cmd = BSON("enableSharding" << dbName);
    return _dbConn->runCommand("admin", cmd, info);
}

bool MongoCluster::shardCollection(const NameSpace& ns, const mongo::BSONObj& shardKey,
bool unique, mongo::BSONObj& info) {
    mongo::BSONObjBuilder bob;
    bob.append("shardCollection", ns).append("key", shardKey);
    if (unique)
        bob.append("unique", true);
    return _dbConn->runCommand("admin", bob.obj(), info);
}

bool MongoCluster::shardCollection(const NameSpace& ns, const mongo::BSONObj& shardKey,
        const bool unique, const uint initialChunks, mongo::BSONObj& info) {
    assert(!ns.empty());
    assert(!shardKey.isEmpty());
    //ensure the key is a hashed shard key
    std::string key = shardKey.toString();
    (void) key;
    assert(key.find("hashed", key.find(":")) != std::string::npos);
    mongo::BSONObjBuilder bob;
    bob.append("shardCollection", ns).append("key", shardKey).append("numInitialChunks",
            initialChunks);
    if (unique)
        bob.append("unique", true);
    return _dbConn->runCommand("admin", bob.obj(), info);
}

bool MongoCluster::shardCollection(const NameSpace& ns, const mongo::BSONObj& shardKey,
        const bool unique, const uint initialChunks) {
    assert(!ns.empty());
    assert(!shardKey.isEmpty());
    //With only 1 chunk there are no preslits, so who cares
    if (!validHashedShardKey(shardKey) && initialChunks > 1)
        throw std::logic_error("Cannot auto generate presplits > 1 without a hashed shard key");
    if (initialChunks <= 0)
        throw std::logic_error("Cannot set initial chunks less than 1 for synthetic sharding");
    if (_colls.end() != _colls.find(ns))
        throw std::logic_error("Namespace already exists, cannot synthetic shard it");
    //insert the database if it doesn't exist
    std::string dbName = ns.substr(0, ns.find('.'));
    auto shardItr = _shards.begin();
    if (_dbs.find(dbName) == _dbs.end())
        _dbs.insert(std::make_pair(dbName, MetaDatabase(dbName, true, shardItr->first, true)));
    //insert the collection
    _colls.insert(std::make_pair(ns, MetaNameSpace(ns, false, shardKey, unique, true)));
    //insert the shards
    //long long is used in the driver/server code.  int64_t is ambiguous
    ShardBsonIndex* shardKeyMap = &_nsChunks.emplace(ns,
            ShardBsonIndex(mtools::BsonCompare(shardKey))).first->second;
    std::string shardKeyName = shardKey.firstElement().fieldName();
    shardKeyMap->insertUnordered(
            std::make_pair(BSON(shardKeyName << BSON("$maxkey" << 1)), shardItr));
    if (initialChunks > 1) {
        //As max signed long is only half the range, double the chunk size for one "step"
        long long chunkSize = std::numeric_limits<long long>::max() / initialChunks * 2;
        long long currentUB = std::numeric_limits<long long>::max() - chunkSize;
        for (int64_t count = initialChunks - 1; count;
                --count, ++shardItr, currentUB -= chunkSize) {
            if (shardItr == _shards.end())
                shardItr = _shards.begin();
            shardKeyMap->insertUnordered(std::make_pair(BSON(shardKeyName << currentUB), shardItr));
        }
    }
    shardKeyMap->finalize();
    return true;
}

bool MongoCluster::shardCollection(const NameSpace& ns, const mongo::BSONObj& shardKey,
        const bool unique, const mongo::BSONObj& splits, const bool synthetic) {
    assert(synthetic);
    assert(!ns.empty());
    assert(!shardKey.isEmpty());
    if (_colls.end() != _colls.find(ns))
        throw std::logic_error("Namespace already exists, cannot synthetic shard it");
    //insert the database if it doesn't exist
    std::string dbName = ns.substr(0, ns.find('.'));
    auto shardItr = _shards.begin();
    if (_dbs.find(dbName) == _dbs.end())
        _dbs.insert(std::make_pair(dbName, MetaDatabase(dbName, true, shardItr->first, true)));
    //insert the collection
    _colls.insert(std::make_pair(ns, MetaNameSpace(ns, false, shardKey, unique, true)));
    //insert the shards
    //long long is used in the driver/server code.  int64_t is ambiguous
    ShardBsonIndex* shardKeyMap = &_nsChunks.emplace(ns,
            ShardBsonIndex(mtools::BsonCompare(shardKey))).first->second;
    std::string shardKeyName = shardKey.firstElement().fieldName();
    shardKeyMap->insertUnordered(
            std::make_pair(BSON(shardKeyName << BSON("$maxkey" << 1)), shardItr));
    //Sort order is irrelevant, but it the only intr available, plus it means array order
    mongo::BSONObjIterator itr(splits["splitKeys"].Obj());
    while (itr.more())
        shardKeyMap->insertUnordered(std::make_pair(itr.next().Obj().copy(), shardItr));
    //Sort happens for sharding
    shardKeyMap->finalize();
    return true;
}

void MongoCluster::flushRouterConfigs() {
    std::unique_ptr<mongo::DBClientBase> conn;
    for (auto&& itr : _mongos) {
        std::string error;
        conn.reset(
                mongo::ConnectionString(itr, mongo::ConnectionString::ConnectionType::SET).connect(
                        error));
        if (!error.empty()) {
            //continue on error, mongoS may be down which is fine
            std::cerr << "Unable to connect to router (for flush): " << itr << std::endl;
            continue;
        }
        mongo::BSONObj info;
        if (!conn->simpleCommand("", &info, "flushRouterConfig"))
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
}  //namespace mtools
