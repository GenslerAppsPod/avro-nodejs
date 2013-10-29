var fs = require('fs');
var addon = require('../build/Release/avro');

var avro = new addon.Avro();

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



describe("An Async test for Avro", function(){
  it("should parse the datum from a byte stream", function(done){

    avro.queueSchema(jsonSchema, 
      function(datum){
        expect({ re: 0, im: 105, name: '', array: [], map: [] }).toEqual(datum);
        done();
        avro.close();
      },
      function(error){
        expect(false).toEqual(false);
        done();
        avro.close();
      }
    );

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
  });
});
