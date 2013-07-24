var fs = require('fs');
var addon = require('./build/Release/avro');
var WebSocketClient = require('websocket').client;

var client = new WebSocketClient();

var Buffer = require('buffer').Buffer;

var avro = new addon.Avro();

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

client.on('connectFailed', function(error) {
    console.log('Connect Error: ' + error.toString());
});

client.on('connect', function(connection) {
    avro.queueSchema('{"type": "map", "values": "bytes"}',
    function(datum){
      var sequenceNum = avro.decodeDatum('"long"', new Buffer(datum.sequence));
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
  connection.sendBytes(new Buffer(avro.encodeDatum(map, {sequence: new Buffer(avro.encodeDatum('"long"', 12345))})));
  connection.sendBytes(new Buffer(avro.encodeDatum('"string"', "com.gensler.organizations.GetOrganization")));
  connection.sendBytes(new Buffer(avro.encodeDatum(complexSchema, { id: { bytes: new Buffer([8,-85,-51,18,52]) }})));
});


client.connect('ws://node1.genslerwi.com:9000/api/gis-data-api/0.1.0');