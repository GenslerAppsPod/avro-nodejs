var fs = require('fs');
var addon = require('./build/Release/avro');

var Buffer = require('buffer').Buffer;

var WebSocketClient = require('websocket').client;

var client = new WebSocketClient();

var avro = new addon.Avro();

avro.onschema = function(schema){
  console.log("schema 1");
};

avro.onerror = function(error){
  console.log("error");
};

avro.ondatum = function(datum){
  console.log("onDatum",datum);
  if(typeof datum.sequence !== "undefined"){
    var buf = new Buffer(datum.sequence);
    var sequenceNum = avro.parseDatum('\"long\"', buf);
    console.log("our sequence", sequenceNum);
  }
};

/*
var jsonSchema = '{ "type": "record", "name": "cpx", "fields" : [{"name": "re", "type": ["double", "null"]},{"name": "im", "type" : "double", "default" : "2.0"},{"name": "name", "type": "string", "default" : "hello"},{"name": "array", "type": [{ "type": "array", "items": "string" }, "null"], "default": "null"},{"name": "map", "type": [{ "type": "map", "values": "int"}]}]}';

avro1.queueSchema(jsonSchema);

fs.open("data.bin", 'r', function(status, fd) {
  fs.fstat(fd,function(err, stats){
    var i=0;
    var s=stats.size;
    var buffer = new Buffer(100);

    console.log('.'+"test.bin"+' '+s);
    buf(fs,fd,0,s,buffer);

    //console.log(addon.decode().length);
  });
});


var buf=function(fs,fd,i,s,buffer){
  if(i+buffer.length<s){
    fs.read(fd,buffer,0,buffer.length,i,function(e,l,b){
      avro1.push(b.slice(0,l));

      i=i+buffer.length;
      setTimeout(function(){
        buf(fs,fd,i,s,buffer);}, 1);
    });
  }else{
    fs.read(fd,buffer,0,buffer.length,i,function(e,l,b){
      avro1.push(b.slice(0,l));
      fs.close(fd);
    });
  }
};
*/

client.on('connectFailed', function(error) {
    console.log('Connect Error: ' + error.toString());
});

client.on('connect', function(connection) {
  console.log('WebSocket client connected');
  avro.queueSchema('{"type": "map", "values": "bytes"}',
    function(datum){
      var sequenceNum = avro.parseDatum('"long"', new Buffer(datum.sequence));
      console.log(datum, sequenceNum);
      avro.queueSchema('"boolean"', function(errorFlag){
        if(!errorFlag){
          var buildingSchema = "\
                              {\
                                \"name\": \"com.gensler.models.organizations.Organization\",\
                                \"type\": \"record\",\
                                \"fields\": [\
                                  { \"name\": \"id\", \"type\": \
                                    {\
                                      \"name\": \"com.gensler.models.common.GUID\",\
                                      \"type\": \"record\",\
                                      \"fields\": [\
                                        { \"name\": \"bytes\", \"type\": \"bytes\"}\
                                      ]\
                                    }\
                                  },\
                                  { \"name\": \"name\", \"type\": \"string\" },\
                                  { \"name\": \"organizationType\", \"type\": \"string\" }\
                                ]\
                              }";

          avro.queueSchema(buildingSchema,
          function(datum){
            console.log(datum);
          },
          function(error){

          });
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
  connection.on('error', function(error) {
    console.log("Connection Error: " + error.toString());
  });
  connection.on('close', function() {
    console.log('echo-protocol Connection Closed');
  });
  connection.on('message', function(message) {
    if (message.type === 'utf8') {
      console.log('Received Message: ' + message.utf8Data);
      connection.sendUTF(message.utf8Data);
    }
    else if (message.type === 'binary') {
      console.log('Received Binary Message of ' + message.binaryData.length + ' bytes');
      console.log('Received Binary Message of ' + message.binaryData + ' ');
      avro.push(message.binaryData);
      //connection.sendBytes(message.binaryData);
    }
  });

  function sendRequest() {
    console.log("sendRequest");
    // Map[String, Seq[Byte]]("sequence" -> serializedBytesOf(12345L)) ==
    //    2, 16, 115, 101, 113, 117, 101, 110, 99, 101, 6, -14, -64, 1, 0
    var metadata = new Buffer(15);
    metadata[0] = 2;
    metadata[1] = 16;
    metadata[2] = 115;
    metadata[3] = 101;
    metadata[4] = 113;
    metadata[5] = 117;
    metadata[6] = 101;
    metadata[7] = 110;
    metadata[8] = 99;
    metadata[9] = 101;
    metadata[10] = 6;
    metadata[11] = -14;
    metadata[12] = -64;
    metadata[13] = 1;
    metadata[14] = 0;

    // "com.gensler.organizations.GetOrganization" ==
    //   82, 99, 111, 109, 46, 103, 101, 110, 115, 108, 101, 114, 46,
    //   111, 114, 103, 97, 110, 105, 122, 97, 116, 105, 111, 110, 115,
    //   46, 71, 101, 116, 79, 114, 103, 97, 110, 105, 122, 97, 116, 105,
    //   111, 110
    var messageName = new Buffer(42);
    messageName[0] = 82;
    messageName[1] = 99;
    messageName[2] = 111;
    messageName[3] = 109;
    messageName[4] = 46;
    messageName[5] = 103;
    messageName[6] = 101;
    messageName[7] = 110;
    messageName[8] = 115;
    messageName[9] = 108;
    messageName[10] = 101;
    messageName[11] = 114;
    messageName[12] = 46;
    messageName[13] = 111;
    messageName[14] = 114;
    messageName[15] = 103;
    messageName[16] = 97;
    messageName[17] = 110;
    messageName[18] = 105;
    messageName[19] = 122;
    messageName[20] = 97;
    messageName[21] = 116;
    messageName[22] = 105;
    messageName[23] = 111;
    messageName[24] = 110;
    messageName[25] = 115;
    messageName[26] = 46;
    messageName[27] = 71;
    messageName[28] = 101;
    messageName[29] = 116;
    messageName[30] = 79;
    messageName[31] = 114;
    messageName[32] = 103;
    messageName[33] = 97;
    messageName[34] = 110;
    messageName[35] = 105;
    messageName[36] = 122;
    messageName[37] = 97;
    messageName[38] = 116;
    messageName[39] = 105;
    messageName[40] = 111;
    messageName[41] = 110;

    // GetOrganization(GUID("ab-cd-12-34")) ==
    //   8, -85, -51, 18, 52
    var record = new Buffer(5);
    record[0] = 8;
    record[1] = -85;
    record[2] = -51;
    record[3] = 18;
    record[4] = 52;


    connection.sendBytes(metadata);
    connection.sendBytes(messageName);
    connection.sendBytes(record);
  }
  sendRequest();
});



client.connect('ws://node1.genslerwi.com:9000/api/gis-data-api/0.1.0');
