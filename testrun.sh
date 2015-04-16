#The hash test requires --dropDB 1 is required for verificatio to work as hashing is database wide
#The hash test requires that a mongoS which can access the md5s is on localhost:27017 on the machine this script is ran on
OPTIONS=
ML_PATH=/home/charlie/git/mlightning/Debug/
MONGO1=mongodb://127.0.0.1:27017
MONGO2=mongodb://127.0.0.1:27017
DATA_DIR=/home/charlie/serialshort/
DIRECT_IN=1
DIRECT_OUT=1
DIRECT_FINAL_IN=0
DIRECT_FINAL_OUT=0
echo ***Importing Data
runtest() {
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "error with mlightning test" >&2
	exit $status
    fi
}
goto end
runtest ${ML_PATH}mlightning ${OPTIONS} --shardKey '{"_id":"hashed"}' --output.uri ${MONGO1} --output.writeConcern 1 --output.direct ${DIRECT_OUT} --output.db import --output.coll original --loadPath ${DATA_DIR} --dropDb 1
echo .
echo .
echo ***Changing shard key
runtest ${ML_PATH}mlightning ${OPTIONS} --shardKey '{"org":"hashed"}' --output.uri ${MONGO2} --output.writeConcern 1 --output.direct ${DIRECT_OUT} --output.db trans --output.coll trans --input.uri ${MONGO1} --input.db import --input.coll original --input.direct ${DIRECT_IN} --dropDb 1
echo .
echo .
echo ***Reverting back to original shard key
#Direct isn't used here so routing is verified too
runtest ${ML_PATH}mlightning ${OPTIONS} --shardKey '{"_id":"hashed"}' --output.uri ${MONGO1} --output.writeConcern 1 --output.direct ${DIRECT_FINAL_OUT} --output.db mirror --output.coll mirror --input.uri ${MONGO2} --input.db trans --input.coll trans --input.direct ${DIRECT_FINAL_IN} --dropDb 1
echo .
echo .
echo ***Verifing
echo The verify is only valid if all operations have taken place on the same cluster
:end
runtest mongo --nodb --norc << EOF
var mongos=new Mongo()
var shardHost=mongos.getDB("config").getCollection("shards").findOne().host.toString()
print(shardHost)
var shard=new Mongo(shardHost)
//Ensure that there are documents on the shard being tested
var statsimport=shard.getDB("import").getCollection("original").stats()
var statsmirror=shard.getDB("mirror").getCollection("mirror").stats()
if(statsimport.count < 1) {
    print("No documents to test on: " + shard)
    quit(1)
}
//In theory this shouldn't be an issue if anything other than w:0 is used for the final write
if (statsimport.count != statsmirror.count) {
    print("Waiting for count of records in mirror.mirror to stablize")
    do {
        sleep(1)
        var oldstats=statsmirror;
        statsmirror=shard.getDB("mirror").getCollection("mirror").stats()
    } while(oldstats.count != statsmirror.count)
    print("mirror.mirror stablized over 1s, if this test still fails manual verification is suggested")
    if (statsimport.count != statsmirror.count) {
        print("Counts are not equal")
        printjson(statsimport.count)
        printjson(statsmirror.count)
        quit(2)
    }
}
var md5import=shard.getDB("import").runCommand({dbHash:1})
var md5mirror=shard.getDB("mirror").runCommand({dbHash:1})
printjson(md5import)
printjson(md5mirror)
if (md5import.md5 != md5mirror.md5) {
    print("MD5 check failed")
    if (md5import.numCollections != md5mirror.numCollections)
	print("Collection size for the databases being hashed aren't the same, is something else running?")
    quit(1)
}
print("SUCCESS")
//quit() prevents this from being saved in the history
quit(0)
EOF

