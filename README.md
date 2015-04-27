# mLightning

mLightning
----------

mlightning is a high speed router for getting data into, out of and moved around in [MongoDB](http://www.mongodb.org).  To bypass mongoS either a hashed shard key or manually calculated presplits are required (calculating presplits will be added to mLightning in the future).  mongoS can be used if there aren't presplits available, however, be aware that this will generally be orders of magnitude slower with decent hardware.  Using a hash can be faster than calculating presplits in many cases with decent hardware, and so can be the better choice when using ephemeral data sets (for instance calculating end of day aggregate performance from several sources that are coalesced into one dataset with MongoDB).

Written in C++, it requires the following to compile:
* C++11 support (gcc 4.8.2)
* mongo-cxx-driver using the legacy branch, the flag -std=c++11 is required.  Known to compile against release legacy-v1.0.0.
* boost - program options, system, regex, thread, chrono and filesystem.  Boost 1.55 in known to work, earlier versions may work, but this hasn't been tested.
* scons (and therefore python)
* tcmalloc (optional)

See [INSTALL.md](./INSTALL.md) for installation instructions.

See [EXAMPLES.md](./EXAMPLES.md) for usage example.

Disclaimer
----------

This software is not supported by [MongoDB, Inc.](http://www.mongodb.com) under any of their commercial support subscriptions or otherwise. Any usage of mLightning is at your own risk. 
Bug reports, feature requests and questions can be posted in the [Issues](https://github.com/lissom/mlightning/issues?state=open) section here on github. 

