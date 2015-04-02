### Usage example
As a useage example, this is the test case to verify that mLightning is working properly.
Please note this assumes that there is a dataset with at least two fields: "_id" and "new_key".

### Caveat emptor
The target database is dropped in these examples, tread carefully.
Databases used:
import
trans
mirror
The balancer is turned off by mLightning by default.  I strongly suggest keeping this behavior as not only does this free up maximum IO, initial splits can have issues if the balancer is on.
Input is always direct from mongoD if using MongoDB as a source.
Output can be either direct or use mongoS.  --output.direct 1 to use direct to shards.  It's much faster.

#### Write concerns
Write concern format:
--output.writeConcern <number>
If <number> = -1 then w:majority is used.
By default mlightning uses w:0.  If data cannot be lost then I would strongly suggest using w:2 (or whatever the majority is in the cluster being used).
Note that with w:0 mlighting will probably return before all data is "visible" in the database.

#### Import the test data in the import.original namespace.
Lets assume there are a bunch of json files to import in /data
mlightning --shardKey '{"_id":"hashed"}' --output.uri mongodb://127.0.0.1:27017 --output.direct 1 --output.db import --output.coll original --loadPath /data --dropDb 1
If using BSON add the option: --inputType bson

#### Reshard the collection with a new key field, called "new_key"
mlightning --shardKey '{"new_key":"hashed"}' --output.uri mongodb://127.0.0.1:27017 --output.direct 1 --output.db trans --output.coll trans --input.uri mongodb://127.0.0.1:27017 --input.db import --input.coll original --dropDb 1

#### Reshard to revert back to the orginal key
mlightning --shardKey '{"_id":"hashed"}' --output.uri mongodb://127.0.0.1:27017 --output.direct 0 --output.db mirror --output.coll mirror --input.uri mongodb://127.0.0.1:27017 --input.db trans --input.coll trans --dropDb 1

#### Verify collections are the same
Please note that this assumes the same chunks are on each shard.  If this is note the case then this test for sameness will not work.  This is usually the case, but sometimes initial chunk distribution can have issues.
The counts on all collection should be the same (when issued from a mongoS).
Hashing is datbase wide, so dropDb must have been used for this to work.
On a mongoD (this will not work on mongoS) with identical chunk ranges (this should happen by default) run these commands:
use import
db.runCommand({dbHash:1})
use mirror
db.runCommand({dbHash:1})
The value of the "md5" field from each commmand should be indentical.
