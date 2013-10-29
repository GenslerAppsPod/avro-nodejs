#Avro-Nodejs

A wrapper for the c++ implemenation of Avro.

#The Why

  Javascript is a async callback world so the avro library should play in that world. I've also seen far too many partially completed avro javascript projects on github. 

#API

###Avro sync

  avro.encodeDatum(schema, value);

  avro.decodeDatum(schema, Buffer);

#####Example
	var addon = require('../build/Release/avro');
    var avro = new addon.Avro();
    var bytes = avro.encodeDatum('"double"', 12345.89);
    var result = avro.decodeDatum('"double"', new Buffer(bytes));
	avro.close();

The result will contain the value that we encoded.

###Avro async

  avro.queueSchema(schema, onSuccess, onError);

  avro.push(Buffer);

  Currently there is only async support for decoding avro. 
  The implementation is designed for the ability to queue multiple avro schemas to be
  decoded with queueSchema(). The push() function pushes bytes to the stream being parsed in a none blocking manor.
  On the completion the call backs are called. 

#####Example
	var addon = require('../build/Release/avro');
    var avro = new addon.Avro();
    avro.queueSchema('{"type": "map", "values": "bytes"',
      function(datum){
        console.log("datum: ", datum);
      },
      function(error){
        console.log(error);
      });

    avro.push(message.binarydata);//message as defined by some stream or websocket. 
	avro.close();

###Closing

The currently implementation is defined to keep getting input from queueSchema and push. So to close out the Avro object call.
		
		avro.close();


# Testing

To run the tests you'll need to install jasmine-node.

	npm install jasmine-node -g

Once that is done just run the specified test. 

	jasmine-node --verbose --matchall tests/AvroSync.js

There seems to be a bug in jasmine node and the only way I can get it to detect my test files is by running
the --matchall option. Possibly upgrading the version of jasmine-node may fix this. TODO

#Build and run

Install Node.js.  There are various methods to install Node.js that vary from system to system.
Information on installing using package managers can be found at:
<https://github.com/joyent/node/wiki/Installing-Node.js-via-package-manager>
Other installation options can be found at: <http://nodejs.org/download/>

Install and build the Avro C++ project found here:
<https://github.com/apache/avro>

The Avro C++ library is dependent on one or more Boost libraries, so you may need to install Boost
before compiling Avro.
On Ubuntu, installing the package "libboost-all-dev" is known to work, though it may be possible to
install with a smaller subset of the Boost libraries.

Specific build instructions for the C++ Avro library may be found in the "PATH_TO_AVRO_CHECKOUT/lang/c++/README" file.
However, it seems that currently the documentation there is a little off.
The following has been known to work:

Execute the following from within the "PATH_TO_AVRO_CHECKOUT/lang/c++" folder:

    ./build.sh test

If all of the required dependencies are installed, that should configure and build the Node C++
libs and run the included tests.  This should be all that is needed for this step, and you should
now have a shiny new shared lib in the "PATH_TO_AVRO_CHECKOUT/lang/c++/build" folder (and a symlink named "libavrocpp.so"
pointing to it).

If desired, you can easily create a Debian package (or RPM if its your preferred package manager)
using checkinstall by executing the following:

    sudo checkinstall \
                --install=no \
                --pkgname="avro-cpp" \
                --pkgrelease="_avro version here_" \
                --maintainer="_maintainer email here_" \
                --addso=yes

Then simply use dpkg to install the generated .deb package.

    dpkg -i avro-cpp_VERSION_ARCH.deb

Next, it may be necessary to update the paths in the "binding.gyp" file.  Simply edit the file and
replace paths in the following lines with appropriate paths:

    'include_dirs': ['/usr/local/include'],

    'ldflags': ['-Lavrocpp/lib'],
    'libraries': ['avrocpp/lib/libavrocpp.so']

For example, if you built the Avro C++ libraries and installed under the prefix "/usr/local",
you would update the lines to the following:

    'include_dirs': ['/usr/local/include'],

    'ldflags': ['-L/usr/local/lib'],
    'libraries': ['/usr/local/lib/libavrocpp.so']

If you don't already have them installed, there are several npm packages that are required to
build, install and use the library.

The library its self does not require websockets but it is recommended for the async use case using
a websocket implementation (https://github.com/Worlize/WebSocket-Node),
which can be installed via npm by running the following from the root folder of your avro-nodejs
folder:

    npm install websocket

To build, you need the node package node-gyp:

    sudo npm install -g node-gyp

After you have all of the prerequisite packages installed, you can build avro-nodejs.

    node-gyp configure build

Tests under the "tests" subfolder can then be run:

    cd tests
    node AvroSync.js


Copyright (c) 2013 Gensler <ben@metzger.cc>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.




