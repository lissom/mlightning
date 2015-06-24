#The hash test requires --dropDB 1 is required for verificatio to work as hashing is database wide
#The hash test requires that a mongoS which can access the md5s is on localhost:27017 on the machine this script is ran on
#Do NOT use connection strings for database connections, use the form perferred by the shell
#It is recommended to use MONGO1 === MONGO2 (this is necessary for the MD5 verification)
#Note that the MD5 tests assume that the non-sharded test cases take place on the first shard, or the test will have fasle results
OPTIONS=
DRY_RUN=
DO_DROP=1
DO_REMOVE_DUMP=1
if [ ! -z ${DRY_RUN} ] && [ ${DRY_RUN} -eq 1 ]; then
  DO_DROP=0
  DO_REMOVE_DUMP=0
fi
ML_PATH=/home/charlie/git/mlightning/Debug/
DATA_DIR=/home/charlie/serialshort/
DUMP_PATH=/tmp/mlightning_test/
DIRECT_IN=1
DIRECT_OUT=1
DIRECT_FINAL_IN=0
DIRECT_FINAL_OUT=0
SUCCESS='\033[1;34m'
FAILURE='\033[0;31m'
TESTLINE='\033[0;32m'
NOCOLOR='\033[0m'
#
#Start functions
#
FIRST_RUN=1
runtest() {
if [ ${FIRST_RUN} -eq 1 ]; then
FIRST_RUN=0
else
    printf "\n\n"
fi
    printf "${TESTLINE}***${1}\n`date`${NOCOLOR}\n"
    shift
    if [ ! -z ${DRY_RUN} ] && [ ${DRY_RUN} -eq 1 ]; then
      echo "$@"
    else
      "$@"
    fi
    local status=$?
    if [ $status -ne 0 ]; then
        printf "${FAILURE}Error with mlightning testing, manual cleanup for the failed test is required (this is intentional, it is kept for debug purposes)${NOCOLOR}\n" >&2
	exit $status
    fi
}

dropdatabases() {
if [ "${MONGO1}" = "${MONGO2}" ]; then
echo "Dropping databases this test created."
mongo ${MONGO1} --norc << EOF
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

removedumppath() {
if [ ${DO_REMOVE_DUMP} -ne 1 ]; then
  return
fi
rm -rf mkdir ${DUMP_PATH}
if [ $? -eq 0 ]; then
  echo "Dump path removed (it did not exist before this test was run)"
else
  echo "Failure to remove the dump path: ${DUMP_PATH}"
fi
}

runloadingtest() {
runtest "Importing data" ${ML_PATH}mlightning ${OPTIONS} --shardKey '{"_id":"hashed"}' --output.uri ${MONGO1} --output.writeConcern 1 --output.direct ${DIRECT_OUT} --output.db import --output.coll original --loadPath ${DATA_DIR} --dropDb 1

runtest "Changing shard key" ${ML_PATH}mlightning ${OPTIONS} --shardKey '{"org":"hashed"}' --output.uri ${MONGO2} --output.writeConcern 1 --output.direct ${DIRECT_OUT} --output.db trans --output.coll trans --input.uri ${MONGO1} --input.db import --input.coll original --input.direct ${DIRECT_IN} --dropDb 1

#Direct isn't used here so routing is verified too
runtest "Reverting back to original shard key" ${ML_PATH}mlightning ${OPTIONS} --shardKey '{"_id":"hashed"}' --output.uri ${MONGO1} --output.writeConcern 1 --output.direct ${DIRECT_FINAL_OUT} --output.db mirror --output.coll mirror --input.uri ${MONGO2} --input.db trans --input.coll trans --input.direct ${DIRECT_FINAL_IN} --dropDb 1

#TODO: pump this into /tmp and use sed to set the variables
if [ "${MONGO1}" = "${MONGO2}" ]; then
runtest "Verifing restarding (The verify is only valid if all operations have taken place on the 127.0.0.1:27017 or the first shard of the cluster)" mongo ${MONOG1} --norc << EOF
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
    //Check to see if the count is increasing over 1 second
    do {
        var oldcount=statsmirror.count;
        sleep(1)
        statsmirror=shard.getDB(targetdb).getCollection(targetcoll).stats()
	print(".")
    } while(oldcount != statsmirror.count)
    print(targetdb + "." + targetcoll + " stablized it's own count over a interval 1s, if this test still fails manual verification is suggested")
    if (statsimport.count != statsmirror.count) {
        print("Counts are not equal")
	print(shardHost)
        print(sourcedb + "." + sourcecoll + ":" statsimport.count)
        print(targetdb + "." + targetcoll + ":" statsmirror.count)
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
fi
} #runloadingtest

runfiletest() {
runtest "Dumping the database" ${ML_PATH}mlightning ${OPTIONS} --input.uri ${MONGO1} --input.direct ${DIRECT_IN} --input.db mirror --input.coll mirror --outputType mltn --workPath ${DUMP_PATH}
runtest "Restoring the database" ${ML_PATH}mlightning ${OPTIONS} --shardKey '{"_id":"hashed"}' --output.uri ${MONGO1} --output.direct ${DIRECT_OUT} --output.db mltnimport --output.coll mirror --inputType mltn --loadPath ${DUMP_PATH}
removedumppath
if [ "${MONGO1}" = "${MONGO2}" ]; then
runtest "Verifying dump and restore (The verify is only valid if all operations have taken place on the 127.0.0.1:27017 or the first shard of the cluster)" mongo ${MONOG1} --norc << EOF
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
//quit(0) prevents this from being saved in the history
quit(0)
EOF
fi
} #runfiletest

shardedtests()
{
printf "\nStarting Sharded Tests\n"
SHARDED=1
MONGO1=127.0.0.1:27017
MONGO2=${MONGO1}
runloadingtest
runfiletest
if [ ${DO_DROP} -eq 1 ]; then
dropdatabases
fi
}

replicatests() {
printf "\nStarting Replica Tests\n"
SHARDED=0
#Assuming we started a sharded cluster to begin with, removing mongoS
pkill mongos
MONGO1=127.0.0.1:27018
MONGO2=$MONGO1
runloadingtest
runfiletest
if [ ${DO_DROP} -eq 1 ]; then
dropdatabases
fi
}

#
#Start run
#
shardedtests
replicatests

printf "\nThere are no tests for unsharded collections in a sharded cluster\n"

#if [ -z ${DRY_RUN} ] || [ ${DRY_RUN} -eq 0 ]; then
printf "${SUCCESS}`date`\n***\n***\n***    All tests have successfully completed!\n***\n***${NOCOLOR}\n"
#fi
