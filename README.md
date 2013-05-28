#Avro-Nodejs

A wrapper for the c++ implemenation of Avro.

#Why

  Javascript is a async callback world so the avro library should play in that world. 

#API

###Avro sync functionality

  avro.encodeDatum(schema, value);

  avro.decodeDatum(schema, Buffer);

#####Example

    var avro = new addon.Avro();
    var bytes = avro.encodeDatum('"float"', 12345.89);
    avro.decodeDatum('"float"', new Buffer(bytes));

###Avro async functionality

  avro.queueSchema(schema, onSuccess, onError);

  avro.push(Buffer);

  Currently there is only async support for decoding avro. 
  The implementation is designed for the ability to queue multiple avro schemas to be
  decoded with queueSchema(). The push() function pushes bytes to the stream being parsed in a none blocking manor.
  On the completion the call backs are called. 

#####Example

    var avro = new addon.Avro();
    avro.queueSchema('{"type": "map", "values": "bytes"',
      function(datum){
        console.log("datum: ", datum);
      },
      function(error){
        console.log(error);
      });

    avro.push(message.binarydata);//message as defined by some stream or websocket. 

#Build and run

    node-gyp configure build

    node server.js





Copyright (c) 2013 Benjamin Metzger <ben@metzger.cc>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.




