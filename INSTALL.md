This program has been verified to compile on Ubuntu 14.04 with gcc 4.8.2 (default on ubuntu).



#### gcc (g++) on Ubuntu 14.04
	sudo apt-get install g++
	
#### snappy on Ubuntu 14.04	
	sudo apt-get install libsnappy-dev

#### tcmalloc on Ubuntu 14.04
	sudo apt-get install libtcmalloc-minimal4
	cd /usr/lib
	sudo ln -s libtcmalloc.so.4 libtcmalloc.so
    (Alternatively you can install the full gperftools suite which includes tcmalloc)

#### Scons on Ubuntu 14.04
	sudo apt-get install scons

#### Boost on Ubuntu 14.04
	sudo apt-get install libboost-all-dev

#### MongoDB C++ Driver
To compile and install the mongo C++ driver (assuming installing into /usr/local is desired):

	git clone https://github.com/mongodb/mongo-cxx-driver.git
	cd mongo-cxx-driver
	git checkout legacy
	sudo scons -j16 --opt=on --c++11 --prefix=/usr/local install

#### mLightning
	scons
	
To compile mlightning with tcmalloc:

	scons --allocator=tcmalloc

#### Help
	mlightning -h
