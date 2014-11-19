/*
 * clone.h
 *
 *  Created on: Oct 12, 2014
 *      Author: charlie
 */

#pragma once

#include "mongo_cxxdriver.h"
#include "mongo_cluster.h"

namespace cloner {

    /*
     * M sized sharded cluster to N sized sharded cluster
     * Clones the sharding config data from one cluster another
     * Assumes that the config.shards collection w/ any tagging information is created
     * Copies tag ranges
     * Copies config.chunks (so everything will in theory be balanced), but will round robin them
     * This makes going from M to N easy, will honor tag ranges
     */
    class Clone {
    public:
        Clone(const std::string &target, const std::string &source);
        void run();

    private:
        tools::mtools::MongoCluster _target;
        tools::mtools::MongoCluster _source;
        std::unique_ptr<mongo::DBClientBase> _targetConn;
        std::unique_ptr<mongo::DBClientBase> _sourceConn;
        tools::mtools::MongoCluster::ShardMap::const_iterator _currShard;

        //iterators for different tag ranges
        using TagShardPointer = std::unordered_map<tools::mtools::MongoCluster::ShardTag,
                tools::mtools::MongoCluster::ShardMap::const_iterator>;
    };

} /* namespace Clone */

