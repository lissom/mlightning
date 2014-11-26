# mLightning

mLightning
----------

mlightning is a tool designed to quick load data into [MongoDB](http://www.mongodb.org) sharded clusters.

Written in C++, it requires the following to compile:
* C++11 support
* mongo-cxx-driver using the legacy branch, -std=c++11 is required.
* boost - program options, system, regex, thread, chrono and filesystem.  Boost 1.55+ in known to work, earlier versions may
* scons (and therefore python)
* tcmalloc (optional)

This program has been verified to compile on Ubuntu 14.04 with gcc 4.8.2 (standard on ubuntu)

#### gcc on Ubuntu 14.04
```sudo apt-get install gcc```

#### tcmalloc on Ubuntu 14.04
```sudo apt-get install libtcmalloc-minimal4
cd /usr/lib
sudo ln -s libtcmalloc.so.4 libtcmalloc.so```

#### Scons on Ubuntu 14.04
```sudo apt-get install scons duplicity```

#### Boost on Ubuntu 14.04
```sudo apt-get install libboost-all-dev```

#### MongoDB C++ Driver
To compile and install the mongo C++ driver (assuming installing into /usr/local is desired): <br>
```git clone https://github.com/mongodb/mongo-cxx-driver.git
cd mongo-cxx-driver
sudo scons -j16 --opt=on --c++11 --prefix=/usr/local install```

#### mLightning
To compile mlightning (with tcmalloc): <br>
```scons --allocator=tcmalloc```

#### Help
```./mlightning -h```


Disclaimer
----------

This software is not supported by [MongoDB, Inc.](http://www.mongodb.com) under any of their commercial support subscriptions or otherwise. Any usage of mtools is at your own risk. 
Bug reports, feature requests and questions can be posted in the [Issues](https://github.com/rueckstiess/mtools/issues?state=open) section here on github. 

