var fs = require('fs');
var addon = require('../build/Release/avro');

var Buffer = require('buffer').Buffer;

var avro = new addon.Avro();

avro.onerror = function(error){
  console.log(error);
};

var buildingSchema = '{\
    "name": "com.gensler.models.organizations.Organization",\
    "type": "record",\
    "fields": [\
      { "name": "id", "type": \
        {\
          "name": "com.gensler.models.common.GUID",\
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

var complexSchema = '{\
  "name": "com.gensler.organizations.GetOrganization",\
  "type": "record",\
  "fields": [\
    { "name": "id", "type": \
      {\
        "name": "com.gensler.models.common.GUID",\
        "type": "record",\
        "fields": [\
          { "name": "bytes", "type": "bytes"}\
        ]\
      }\
    }\
  ]\
}';


var map = '{"type": "map","values": "bytes"}';
// Some of the types supported
// TODO finish off examples.
var mapResult = avro.decodeDatum(map, new Buffer(avro.encodeDatum(map, {sequence: new Buffer(avro.encodeDatum('"long"', 12345))})));
var booleanResult = avro.decodeDatum('"boolean"', new Buffer(avro.encodeDatum('"boolean"', true )));
var stringResult = avro.decodeDatum('"string"', new Buffer(avro.encodeDatum('"string"', "A string to parse" )));
var complexResult = avro.decodeDatum(complexSchema, new Buffer(avro.encodeDatum(complexSchema, { id: { bytes: new Buffer([8,-85,-51,18,52]) }})));
var longResult = avro.decodeDatum('"long"', new Buffer(avro.encodeDatum('"long"', 12345)));

console.log("boolean result: ", booleanResult);
console.log("map result: ", mapResult);
console.log("long result: ", longResult);
console.log("string result: ", stringResult);
console.log("complex result: ", complexResult);

//Since avro starts another thread in the background for reading data to stop node 
// we need to send a kill to the process.
process.kill();