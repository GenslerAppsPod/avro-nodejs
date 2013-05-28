var fs = require('fs');
var addon = require('./build/Release/avro');
var WebSocketClient = require('websocket').client;

var client = new WebSocketClient();

var Buffer = require('buffer').Buffer;

var avro = new addon.Avro();

avro.onschema = function(schema){
  console.log("schema 1");
};

avro.onerror = function(error){
  console.log(error);
};

avro.ondatum = function(datum){
  console.log("onDatum",datum);
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
var bytes = avro.encodeDatum('"float"', 12345.39);
console.log(avro.decodeDatum('"float"', new Buffer(bytes)));
//avro.encodeDatum(map, {sequence: new Buffer(bytes)});
/*
var mapResult = avro.decodeDatum(map, avro.encodeDatum(map, {sequence: avro.encodeDatum('"long"', 12345)}));
var booleanResult = avro.decodeDatum('"boolean"', avro.encodeDatum('"boolean"', true ));
var stringResult = avro.decodeDatum('"string"', avro.encodeDatum('"string"', "A string to parse" ));
var complexResult = avro.decodeDatum(complexSchema, avro.encodeDatum(complexSchema, { id: { bytes: [8,-85,-51,18,52] }}));
var longResult = avro.decodeDatum('"long"', avro.encodeDatum('"long"', 12345));

console.log("boolean result: ", booleanResult);
console.log("map result: ", mapResult);
console.log("long result: ", longResult);
console.log("string result: ", stringResult, " byte length: ",avro.encodeDatum('"string"', "A string to parse" ).length);
console.log("complex result: ", complexResult, " byte length: ",avro.encodeDatum(complexSchema, { id: { bytes: [8,-85,-51,18,52] }}).length);
*/
/*
client.on('connectFailed', function(error) {
    console.log('Connect Error: ' + error.toString());
});

client.on('connect', function(connection) {
    avro.queueSchema('{"type": "map", "values": "bytes"}',
    function(datum){
      var sequenceNum = avro.decodeDatum('"long"', datum.sequence);
      console.log(datum, sequenceNum);
      avro.queueSchema('"boolean"', function(errorFlag){
        if(!errorFlag){
          avro.queueSchema(buildingSchema);
        }else{
          console.log(errorFlag);
        }
      },
      function(error){

      });
    },
    function(errer){
      console.log(error);
    });
  connection.on('message', function(message) {
    if (message.type === 'utf8') {
      console.log('Received Message: ' + message.utf8Data);
      connection.sendUTF(message.utf8Data);
    }
    else if (message.type === 'binary') {
      console.log("message");
      avro.push(message.binaryData);
    }
  });
  connection.sendBytes(new Buffer(avro.encodeDatum(map, {sequence: avro.encodeDatum('"long"', 12345)})));
  connection.sendBytes(new Buffer(avro.encodeDatum('"string"', "com.gensler.organizations.GetOrganization")));
  connection.sendBytes(new Buffer(avro.encodeDatum(complexSchema, { id: { bytes: [8,-85,-51,18,52] }})));
});


client.connect('ws://node1.genslerwi.com:9000/api/gis-data-api/0.1.0');

//complex schema 
*/