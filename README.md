# mlightning

mLightning
----------

mlightning is a tool designed to quick load data into sharded clusters.

Written in C++, it requires the follow to compile:
* C++11 support, mongo-cxx-driver using the legacy branch, -std=c++11 is required.
* boost - program options, system, regex, thread, chrono and filesystem.  Boost 1.55+ in known to work, earlier versions may
* tcmalloc (optional)

This program has been verified to compile on gcc 4.8.2 & Ubuntu 14.04.

To install the mongo C++ driver (assuming you want to install into /usr/local):
```git clone https://github.com/mongodb/mongo-cxx-driver.git
cd mongo-cxx-driver
sudo scons -j16 --opt=on --c++11 --prefix=/usr/local install```

tcmalloc on Ubuntu 14.04:
```sudo apt-get libtcmalloc-minimal4
cd /usr/lib
ln -s libtcmalloc.so.4 libtcmalloc.so```

To install mlightning:
```scons```
or
```scons --allocator=tcmalloc```
