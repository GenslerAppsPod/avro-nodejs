var fs = require('fs');
var addon = require('../build/Release/avro');

var Buffer = require('buffer').Buffer;

var avro = new addon.Avro();

avro.decodeFile("test.bin",
function(datum){
  console.log(datum);
},
function(error){
  console.log(error);
});
avro.close();

