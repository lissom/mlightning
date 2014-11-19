/*
 * clone.cpp
 *
 *  Created on: Oct 12, 2014
 *      Author: charlie
 */

#include "clone.h"

namespace cloner {

    Clone::Clone(const std::string &target, const std::string &source) :
        _target(target),
        _source(source) {
        std::string error;
        _targetConn.reset(_target.connStr().connect(error));
        assert(error.empty());
        _sourceConn.reset(_source.connStr().connect(error));
        assert(error.empty());

        if (!_target.isSharded() || !_source.isSharded()) {
            std::cerr << "Both clusters must be sharded for the cloner" << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    /**
     * Stop all balancers
     * Make sure that none of the databases exist, if they do drop them
     * Insert the new databases/collections/chunks
     * Mark the oplog position on each replica set
     * Clone the chunks
     * Clone the unsharded collections in sharded databases
     * Clone the non-sharded databases
     * Start updates from the oplog position
     */
    //TODO: Persist state information in the new cluster
    //TODO: Options: drop databases, balancer wait
    void Clone::run() {
        if (!_target.stopBalancerWait(std::chrono::seconds(60))) {
            std::cerr << "Unable to stop target balancer" << std::endl;
            exit(EXIT_FAILURE);
        }
        if (!_source.stopBalancerWait(std::chrono::seconds(60))) {
            std::cerr << "Unable to stop target balancer" << std::endl;
            exit(EXIT_FAILURE);
        }


        //Move tag ranges if they exist
        //TODO: do we want to add sanity checks/shard range removals?
        mongo::Cursor cur = _sourceConn->query("config.tags", mongo::Query());
        while (cur->more()) {
            _sourceConn->insert("config.tags", cur->next());
            std::string error = _sourceConn->getLastError();
            if (!error.empty()) {
                std::cerr << "Unable to insert source tag ranges into target config.tags" << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        cur = _sourceConn->query("config.chunks", mongo::Query().sort(BSON("ns" << 1 << "max" << 1)));
        _currShard = _target.shards().cbegin();

        std::string prevNs;
        tools::mtools::MongoCluster::TagBsonIndex* nsTags;
        TagShardPointer currentTag;
        while (cur->more()) {
            mongo::BSONObj obj = cur->next();
            std::string ns = obj.getStringField("ns");
            //If it's a new namespace, shard it, clear the chunks, check for tag ranges
            if (ns != prevNs) {
                prevNs = ns;
                //Unconditional call, it may fail because sharding is already enabled
                mongo::BSONObj shardingError;
                _target.enableSharding(mongo::nsGetDB(ns), &shardingError);
                mongo::BSONObj oldColl = _sourceConn->findOne("config.collections", BSON("_id" << ns));
                //This should always be found as we found chunks
                assert(oldColl.isValid());
                mongo::BSONObj key = oldColl.getObjectField("key");
                assert(key.isValid());
                //If the shard key is hashed, the first element should has a string value
                mongo::BSONObj info;
                if (key.firstElement().type() == mongo::BSONType::String) {
                    //Make sure there is only a single chunk to prevent having to wait for balancing
                    _target.shardCollection(ns, key, oldColl.getBoolField("unique"), 1, &info);
                }
                else
                    _target.shardCollection(ns, key, oldColl.getBoolField("unique"), &info);
                if (!info.isEmpty()) {
                    std::cerr << "Unable to shard(" << ns << "):" << info << std::endl;
                    exit(EXIT_FAILURE);
                }
                //remove all chunks from our freshy hashed collection
                _targetConn->remove("config.chunks", MONGO_QUERY("ns" << ns), false, nullptr);
                auto itr = _target.nsTagRanges().find(ns);
                if (itr != _target.nsTagRanges().end()) {
                    nsTags = &itr->second;
                    currentTag.clear();
                }
                else
                    nsTags = nullptr;
            }
            mongo::BSONObjBuilder bob;
            mongo::BSONObjIterator itr(obj);
            while(itr.more()) {
                mongo::BSONElement elem = itr.next();
                if (strcmp("shard", elem.fieldName()) == 0)
                    continue;
                bob.append(elem);
            }
            //Check to see if this is a defined tag set, if not, use the next available chunk
            //TODO: Use chunk count rather than RR to balance non-tagged chunks after tagging has taken place
            if (nsTags) {
                mongo::BSONObj maxKey = obj.getField("max").Obj();
                mongo::BSONObj minKey = obj.getField("min").Obj();
                //auto upper = std::upper_bound(nsTags->begin(), nsTags->end(), maxKey, nsTags->compare());
                //auto lower = std::lower_bound(nsTags->begin(), nsTags->end(), minKey, nsTags->compare());
            }
            bob.append("shard", _currShard->first);
            if (++_currShard == _target.shards().cend())
                _currShard = _target.shards().cbegin();

            //TODO: write or die, cleanup on fail first?
            _targetConn->insert("config.chunks", bob.obj());
        }

        //Ensure that all routers see the new sharding info
        _target.flushRouterConfigs();


        //TODO: replace this with stopping mongoS to ensure it won't try to balance any collections
        //i.e. db.shutdownServer()
        std::this_thread::sleep_for(std::chrono::seconds(3));

    }

} /* namespace Clone */
