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

#include "program_options.h"
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <thread>

namespace loader {

    namespace {

    void wrapJson(std::string* json) {
        if (!json->empty() && !boost::starts_with(*json, "{"))
            json->insert(0, "{").append("}");
    }

    }  //namespace

//TODO: convert to YAML setup file
//TODO: config file
//TODO: logging queue and output file
//TODO: Option to discard fields (i.e. _id for fast right inserts)
//TODO: option to change field names
    /*
     * Takes argc and argv, uses boost to transform them into program options
     */
    void setProgramOptions(Loader::Settings& settings, int argc, char* argv[]) {
        namespace po = boost::program_options;
        po::options_description generic("Generic");
        po::options_description cmdline("Command Line");
        const std::string supportedLoadStrategies = "BSON doc of queuing *per shard* to use: '{\"direct\": 98, "
                "\"ram\": 2}'"
                "\nNote that presplits for very large numbers can be lengthy(i.e. over 100)!  Total splits = sum of queues * number of shards"
                "\nCurrently supported: "
                + loader::docbuilder::ChunkBatchFactory::getKeysPretty() + "\nDirect between 10 and 100 is recommended";
        const std::string supportedInputTypes = "Input types: " +
                loader::Loader::Settings::inputTypesPretty();
        generic.add_options()
            ("help,h", "print this help message")
            ("record.statFile,S", po::value<std::string>(&settings.statsFile),
                    "will output csv run summary to this file")
            ("record.note", po::value<std::string>(&settings.statsFileNote), "note in final stats file column")
            //TODO:log file
            /*("logFile,l", po::value<std::string>(),
                    "logFile - NOT YET IMPLEMENTED")*/
            ("inputType,T", po::value<std::string>(&settings.inputType)->default_value("json"),
                    supportedInputTypes.c_str())
            ("loadPath,p", po::value<std::string>(&settings.loadDir)->required(),
                    "directory to load files from")
            ("fileRegex,r", po::value<std::string>(&settings.fileRegex),
                    "regular expression to match files on: (.*)(json)")
            ("workPath", po::value<std::string>(&settings.workPath),
                    "directory to save temporary work in")
            ("uri,u", po::value<std::string>(&settings.connstr)
                    ->default_value("mongodb://127.0.0.1:27017"), "mongodb connection URI")
            ("db,d", po::value<std::string>(&settings.database)->required(), "database")
            ("coll,c", po::value<std::string>(&settings.collection)->required(), "collection")
            ("directLoad,D", po::value<bool>(&settings.endPointSettings.directLoad)
                    , "Directly load into mongoD, bypass mongoS")
            ("dropDb", po::value<bool>(&settings.dropDb)->default_value(false),
                    "DANGER: Drop the database")
            ("dropColl", po::value<bool>(&settings.dropColl)->default_value(false),
                    "DANGER: Drop the collection")
            ("stopBalancer", po::value<bool>(&settings.stopBalancer)->default_value(true),
                    "stop the balancer")
            ("shardKey,k", po::value<std::string>(&settings.shardKeyJson)->required(),
                    "Dotted fields not supported (i.e. subdoc.field) must quote fields "
                    "'(\"_id\":\"hashed\"'")
            ("shardKeyUnique", po::value<bool>(&settings.shardKeyUnique)->default_value(false),
                    "Is the shard key unique")
            ("add_id", po::value<bool>(&settings.add_id)->default_value(true),
                    "Add _id if it doesn't exist, operations will error if _id is required")
            ("queuing,q", po::value<std::string>(&settings.loadQueueJson)->default_value("\"direct\":10"),
                    supportedLoadStrategies.c_str())
            ("load.batchSize", po::value<long unsigned int>(&settings.batcherSettings.queueSize)
                    ->default_value(1000), "Read queue size")
            ("load.inputThreads,t", po::value<int>(&settings.threads)
                    ->default_value(0), "threads, 0 for auto limit, "
                    "-x for a limit from the max hardware threads(default: 0)")
            ("dispatch.threads", po::value<size_t>(&settings.dispatchSettings.workThreads)
                    ->default_value(10), "Threads available to the dispatcher to do work (i.e. spill to disk)")
            ("dispatch.ramQueueBatchSize,B",
                    po::value<size_t>(&settings.dispatchSettings.ramQueueBatchSize)
                    ->default_value(10000), "load queue size to pass on to dispatcher")
            ("mongo.bulkWriteVersion", po::value<int>(&settings.dispatchSettings.bulkWriteVersion)
                    ->default_value(1), "Write protocol to use: 0 = 2.4; 1 = 2.6")
            ("mongo.writeConcern,w", po::value<int>(&settings.dispatchSettings.writeConcern)
                    ->default_value(0), "write concern, # of nodes")
            ("mongo.batchMaxQueue", po::value<size_t>(&settings.endPointSettings.maxQueueSize)
                    ->default_value(100), "Maximum queue size for an endpoint before halting read threads")
            ("mongo.syncDelay", po::value<int>(&settings.syncDelay)
                    ->default_value(-1), "NOT YET IMPLEMENTED") //reserving S
            ("mongo.threads,e", po::value<size_t>(&settings.endPointSettings.threadCount)
                    ->default_value(2), "threads per end point")
            ("mongo.LocklessMissWait", po::value<size_t>(&settings.endPointSettings.sleepTime)
                    ->default_value(10), "Wait time for mongo connections with a lockless miss method")
            ("mongo.sharded,s", po::value<bool>(&settings.sharded)->default_value(true), "Used a sharded setup")
            ;
        cmdline.add_options()
            ("config", po::value<std::string>(), "config file - NOT YET IMPLEMENTED")
            ;
        cmdline.add(generic);
        po::variables_map vm;
        std::string errormsg;
        try {
            po::store(po::command_line_parser(argc, argv).options(cmdline).run(), vm);
            /*if(vm.count("config"))
             po::store(po::parse_config_file(vm["config"].as<std::string>().c_str()), vm));*/
            po::notify(vm);
        }
        catch (std::exception& e) {
            errormsg = e.what();
        }
        if (vm.count("help") || !errormsg.empty()) {
            cmdline.print(std::cout);
            if (!errormsg.empty()) std::cerr << "Unable to parse options: " + errormsg << std::endl;
            exit(0);
        }
        //Set to -1 to signify the default, which is to
        //drop and recreate if the collection is empty
        wrapJson(&settings.shardKeyJson);
        wrapJson(&settings.loadQueueJson);
    }
}  //namespace loader
