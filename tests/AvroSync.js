var fs = require('fs');
var addon = require('../build/Release/avro');

var Buffer = require('buffer').Buffer;

var avro = new addon.Avro();

avro.onerror = function(error){
  console.log(error);
};

var buildingSchema = '{\
    "name": "Organization",\
    "type": "record",\
    "fields": [\
      { "name": "id", "type": \
        {\
          "name": "GUID",\
          "type": "record",\
          "fields": [\
            { "name": "bytes", "type": "bytes"}\
          ]\
        }\
      },\
      { "name": "name", "type": "string" },\
      { "name": "organizationType", "type": "string" }\
    ]\
  }';

var fixedExample = '{\
  "name": "com.gensler.scalavro.protocol.HandshakeRequest",\
  "type": "record",\
  "fields": [{\
    "name": "clientHash",\
    "type": {\
      "name": "MD5",\
      "type": "fixed",\
      "size": 16,\
      "namespace": "com.gensler.scalavro.protocol",\
      "aliases": []\
    }\
  }, {\
    "name": "clientProtocol",\
    "type": ["null", "string"]\
  }, {\
    "name": "serverHash",\
    "type": "com.gensler.scalavro.protocol.MD5"\
  }, {\
    "name": "meta",\
    "type": ["null", {\
      "type": "map",\
      "values": "bytes"\
    }]\
  }]\
}';


var complexSchema = '{\
  "name": "GetOrganization",\
  "type": "record",\
  "fields": [\
    { "name": "id", "type": \
      {\
        "name": "common.GUID",\
        "type": "record",\
        "fields": [\
          { "name": "bytes", "type": "bytes"}\
        ]\
      }\
    }\
  ]\
}';

var complexUnion = '[{\
  "name": "A",\
  "type": "record",\
  "fields": [{"name": "x", "type": ["int", "string"]}]\
  }, {\
    "name": "B",\
    "type": "record",\
    "fields": [{"name": "x", "type": "string"}]\
  }]';

var union ='["string", "double"]';

var handshakeResponse = '{\
    "type": "record",\
    "name": "handshakeResponse",\
    "namespace": "com.gensler.scalavro.protocol",\
    "fields": [\
      {"name": "match",\
       "type": {"type": "enum", "name": "HandshakeMatch",\
                "symbols": ["BOTH", "CLIENT", "NONE"]}},\
      {"name": "serverProtocol",\
       "type": ["null", "string"]},\
      {"name": "serverHash",\
       "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},\
      {"name": "meta",\
       "type": ["null", {"type": "map", "values": "bytes"}]}\
    ]\
  }';

var map = '{"type": "map","values": "bytes"}';
// Some of the types supported
// TODO finish off examples.
var fixedResult = avro.decodeDatum(fixedExample, new Buffer(
  avro.encodeDatum(fixedExample, { 
      clientHash:  new Buffer([ 120, 231, 49, 2, 125, 143, 213, 14, 214, 66, 52, 11, 124, 154, 99, 179 ]),
      clientProtocol: { string: "client"},
      serverHash:  new Buffer([ 120, 231, 49, 2, 125, 143, 213, 14, 214, 66, 52, 11, 124, 154, 99, 179 ]),
      meta: null
  })
));

var handshakeResponseResult = avro.decodeDatum(handshakeResponse, new Buffer(
  avro.encodeDatum(handshakeResponse, {
    match: "CLIENT",
    serverProtocol: null,
    serverHash: null,
    meta: null    
  })
));
/*
for(var i = 0;i<100;i++){
  var avroLoop = new addon.Avro();
  var complexUnionResult = avroLoop.decodeDatum(complexUnion, new Buffer(avroLoop.encodeDatum(complexUnion, { "A": {"x": {string: "a String"}}})));
  console.log("complex union result: ",complexUnionResult);
  avroLoop.close();
}
*/

var complexUnionResult = avro.decodeDatum(complexUnion, new Buffer(avro.encodeDatum(complexUnion, { "A": {"x": {string: "a String"}}})));
var unionResult = avro.decodeDatum(union, new Buffer(avro.encodeDatum(union, { string: "we have a string"})));
var mapResult = avro.decodeDatum(map, new Buffer(avro.encodeDatum(map, {sequence: new Buffer(avro.encodeDatum('"long"', 12345))})));
var booleanResult = avro.decodeDatum('"boolean"', new Buffer(avro.encodeDatum('"boolean"', true )));
var stringResult = avro.decodeDatum('"string"', new Buffer(avro.encodeDatum('"string"', "A string to parse" )));
var complexResult = avro.decodeDatum(complexSchema, new Buffer(avro.encodeDatum(complexSchema, { id: { bytes: new Buffer([8,-85,-51,18,52]) }})));
var longResult = avro.decodeDatum('"long"', new Buffer(avro.encodeDatum('"long"', 12345)));

console.log("handshake result: ", handshakeResponseResult);
console.log("complex union result: ",complexUnionResult);
console.log("fixed result: ", fixedResult);
console.log("union result: ", unionResult);
console.log("boolean result: ", booleanResult);
console.log("map result: ", mapResult);
console.log("long result: ", longResult);
console.log("string result: ", stringResult);
console.log("complex result: ", complexResult);

//Since avro starts another thread in the background for reading data to stop node 
// we need to send a kill to the process.
avro.close();
console.log("end of file");