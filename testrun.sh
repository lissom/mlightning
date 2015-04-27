#The hash test requires --dropDB 1 is required for verificatio to work as hashing is database wide
#The hash test requires that a mongoS which can access the md5s is on localhost:27017 on the machine this script is ran on
OPTIONS=
DO_DROP=1
ML_PATH=/home/charlie/git/mlightning/Debug/
MONGO1=mongodb://127.0.0.1:27018
MONGO2=${MONGO1}
#It is recommended to use MONGO1 === MONGO2
#MONGO1=mongodb://127.0.0.1:27018
#MONGO2=mongodb://127.0.0.1:27019
DATA_DIR=/home/charlie/serialshort/
DUMP_PATH=/tmp/mlightning_test/
DIRECT_IN=1
DIRECT_OUT=1
DIRECT_FINAL_IN=0
DIRECT_FINAL_OUT=0
runtest() {
echo    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "Error with mlightning testing, manual cleanup for the failed test is required (this is intentional, it is kept for debug purposes)" >&2
	exit $status
    fi
}

dropdatabases() {
if [ "${MONGO1}" = "${MONGO2}" ]; then
echo "Dropping databases this test created."
mongo --nodb --norc << EOF
var toDrop = ["import", "mirror", "mltnimport", "trans"]
var mongos=new Mongo()
for (i = toDrop.length - 1; i >= 0; --i) {
  print("Dropping database " + toDrop[i])
  mongos.getDB(toDrop[i]).dropDatabase()
}
EOF
else
echo "Different clusters used in testing, not removing databases"
fi
}

runloadingtest() {
echo ***Importing Data
#runtest ${ML_PATH}mlightning ${OPTIONS} --shardKey '{"_id":"hashed"}' --output.uri ${MONGO1} --output.writeConcern 1 --output.direct ${DIRECT_OUT} --output.db import --output.coll original --loadPath ${DATA_DIR} --dropDb 1
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
#TODO: pump this into /tmp and use sed to set the variables
runtest mongo --nodb --norc << EOF
var sourcedb="import"
var sourcecoll="original"
var targetdb="mirror"
var targetcoll="mirror"
var mongos=new Mongo()
var shardHost=mongos.getDB("config").getCollection("shards").findOne().host.toString()
print(shardHost)
var shard=new Mongo(shardHost)
//Ensure that there are documents on the shard being tested
var statsimport=shard.getDB(sourcedb).getCollection(sourcecoll).stats()
var statsmirror=shard.getDB(targetdb).getCollection(targetcoll).stats()
if(statsimport.count < 1) {
    print("No documents to test on: " + shard)
    quit(1)
}
//In theory this shouldn't be an issue if anything other than w:0 is used for the final write
if (statsimport.count != statsmirror.count) {
    print("Waiting for count of records in new collection to stablize (in case w:0 was used)")
    do {
        sleep(1)
        var oldstats=statsmirror;
        statsmirror=shard.getDB(targetdb).getCollection(targetcoll).stats()
    } while(oldstats.count != statsmirror.count)
    print("New collection stablized over 1s, if this test still fails manual verification is suggested")
    if (statsimport.count != statsmirror.count) {
        print("Counts are not equal")
        printjson(statsimport.count)
        printjson(statsmirror.count)
        quit(2)
    }
}
var md5import=shard.getDB(sourcedb).runCommand({dbHash:1})
var md5mirror=shard.getDB(targetdb).runCommand({dbHash:1})
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
} #runloadingtest

runfiletest() {
echo .
echo .
echo ***Dumping the database
runtest ${ML_PATH}mlightning ${OPTIONS} --input.uri ${MONGO1} --input.direct ${DIRECT_IN} --input.db mirror --input.coll mirror --outputType mltn --workPath ${DUMP_PATH}
echo .
echo .
echo ***Restoring the database
runtest ${ML_PATH}mlightning ${OPTIONS} --shardKey '{"_id":"hashed"}' --output.uri ${MONGO1} --output.direct ${DIRECT_OUT} --output.db mltnimport --output.coll mirror --inputType mltn --loadPath ${DUMP_PATH}
rm -rf mkdir ${DUMP_PATH}
if [ $? -eq 0 ]; then
  echo "Dump path removed (it did not exist before this test was run)"
else
  echo "Failure to remove the dump path: ${DUMP_PATH}"
fi
echo .
echo .
echo ***Verifing
echo The verify is only valid if all operations have taken place on the same cluster
runtest mongo --nodb --norc << EOF
var sourcedb="import"
var sourcecoll="original"
var targetdb="mltnimport"
var targetcoll="mirror"
var mongos=new Mongo()
var shardHost=mongos.getDB("config").getCollection("shards").findOne().host.toString()
print(shardHost)
var shard=new Mongo(shardHost)
//Ensure that there are documents on the shard being tested
var statsimport=shard.getDB(sourcedb).getCollection(sourcecoll).stats()
var statsmirror=shard.getDB(targetdb).getCollection(targetcoll).stats()
if(statsimport.count < 1) {
    print("No documents to test on: " + shard)
    quit(1)
}
//In theory this shouldn't be an issue if anything other than w:0 is used for the final write
if (statsimport.count != statsmirror.count) {
    print("Waiting for count of records in new collection to stablize (in case w:0 was used)")
    do {
        sleep(1)
        var oldstats=statsmirror;
        statsmirror=shard.getDB(targetdb).getCollection(targetcoll).stats()
    } while(oldstats.count != statsmirror.count)
    print(targetdb + "." + targetcoll + " stablized over 1s, if this test still fails manual verification is suggested")
    statsmirror=shard.getDB(targetdb).getCollection(targetcoll).stats()
    if (statsimport.count != statsmirror.count) {
        print("Counts are not equal")
        printjson(statsimport.count)
        printjson(statsmirror.count)
        quit(2)
    }
}
var md5import=shard.getDB(sourcedb).runCommand({dbHash:1})
var md5mirror=shard.getDB(targetdb).runCommand({dbHash:1})
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
} #runfiletest

runloadingtest
runfiletest

echo "All tests have successfully completed."
if [ ${DO_DROP} -eq 1 ]; then
dropdatabases
fi
echo "Exiting now."
