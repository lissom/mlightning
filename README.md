# mlightning

mLightning
----------

mlightning is a tool designed to quick load data into sharded clusters.

Written in C++, it requires the following to compile:
* C++11 support
* mongo-cxx-driver using the legacy branch, -std=c++11 is required.
* boost - program options, system, regex, thread, chrono and filesystem.  Boost 1.55+ in known to work, earlier versions may
* scons (and therefore python)
* tcmalloc (optional)

This program has been verified to compile on gcc 4.8.2 & Ubuntu 14.04.

tcmalloc on Ubuntu 14.04:
```sudo apt-get install libtcmalloc-minimal4
cd /usr/lib
ln -s libtcmalloc.so.4 libtcmalloc.so```

scons on Ubuntu 14.04:
```sudo apt-get install scons duplicity````

boost on Ubuntu 14.04:
```sudo apt-get install libboost-all-dev```

To compile and install the mongo C++ driver (assuming installing into /usr/local is desired):
```
git clone https://github.com/mongodb/mongo-cxx-driver.git
cd mongo-cxx-driver
sudo scons -j16 --opt=on --c++11 --prefix=/usr/local install
```

To compile mlightning (with tcmalloc):
```scons --allocator=tcmalloc```
