#The hash test requires --dropDB 1 is required for verificatio to work as hashing is database wide
#The hash test requires that a mongoS which can access the md5s is on localhost:27017 on the machine this script is ran on
ML_PATH=/home/charlie/git/mlightning/Debug/
MONGO1=mongodb://127.0.0.1:27017
MONGO2=mongodb://127.0.0.1:27017
DATA_DIR=/home/charlie/serialshort/
DIRECT=1
echo ***Importing Data
#${ML_PATH}mlightning --shardKey '{"_id":"hashed"}' --output.uri ${MONGO1} --output.direct 1 --output.db import --output.coll original --loadPath ${DATA_DIR} --dropDb 1
echo ***
echo ***
echo ***Changing shard key
#${ML_PATH}mlightning --shardKey '{"org":"hashed"}' --output.uri ${MONGO2} --output.direct 1 --output.db trans --output.coll trans --input.uri ${MONGO1} --input.db import --input.coll original --dropDb 1
echo ***
echo ***
echo ***Reverting back to original shard key
#Direct isn't used here so routing is verified too
#${ML_PATH}mlightning --shardKey '{"_id":"hashed"}' --output.uri ${MONGO1} --output.direct 0 --output.db mirror --output.coll mirror --input.uri ${MONGO2} --input.db trans --input.coll trans --dropDb 1
echo ***
echo ***
echo ***Verifing
mongo --nodb --norc <<EOF
var mongos=new Mongo()
var shardHost=mongos.getDB("config").getCollection("shards").findOne().host.toString()
print(shardHost)
var shard=new Mongo(shardHost)
//Ensure that there are documents on the shard being tested
var statsimport=shard.getDB("import").getCollection("original").stats()
var statsmirror=shard.getDB("mirror").getCollection("mirror").stats()
if(statsimport.count < 1) {
    print("No documents to test on: " + shard);
    quit(2);
}
if (statsimport.count != statsmirror.count) {
    print("Counts are not equal")
    quit(2)
}
var md5import=shard.getDB("import").runCommand({dbHash:1})
var md5mirror=shard.getDB("mirror").runCommand({dbHash:1})
printjson(md5import)
printjson(md5mirror)
if (md5import.md5 != md5mirror.md5) {
    print("MD5 check failed");
    quit(1)
}
print("SUCCESS")
EOF

