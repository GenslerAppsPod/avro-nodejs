var fs = require('fs');
var addon = require('../build/Release/avro');

var Buffer = require('buffer').Buffer;

var avro = new addon.Avro();

avro.onerror = function(error){
  console.log(error);
};

avro.ondatum = function(datum){
  console.log("onDatum",datum);
  avro.close();
};

var jsonSchema = '{\
  "type": "record",\
  "name": "cpx",\
  "fields" : [{\
    "name": "re",\
    "type": ["double", "null"]\
    },\
    {\
      "name": "im",\
      "type" : "double",\
      "default" : "2.0"\
    },\
    {\
      "name": "name",\
      "type": "string",\
      "default" : "hello"\
    },\
    {\
      "name": "array",\
      "type": [{\
        "type": "array",\
        "items": "string"\
      }, "null"],\
      "default": "null"\
    },\
    {\
      "name": "map",\
      "type": [{\
        "type": "map",\
        "values": "int"\
      }]\
    }\
  ]}';

avro.queueSchema(jsonSchema);


fs.open("data.bin", 'r', function(status, fd) {
  fs.fstat(fd,function(err, stats){
    var i=0;
    var s=stats.size;
    var buffer = new Buffer(100);

    console.log('.'+"data.bin"+' '+s);
    buf(fs,fd,0,s,buffer);
  });
});


var buf=function(fs,fd,i,s,buffer){
  if(i+buffer.length<s){
    fs.read(fd,buffer,0,buffer.length,i,function(e,l,b){
      avro.push(b.slice(0,l));
      i=i+buffer.length;
      setTimeout(function(){
        buf(fs,fd,i,s,buffer);
      }, 1);
    });
  }else{
    fs.read(fd,buffer,0,buffer.length,i,function(e,l,b){
      var section = b.slice(0,l);
      avro.push(section);
      fs.close(fd);
    });
  }
};
