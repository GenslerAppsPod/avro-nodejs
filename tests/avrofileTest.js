var avro = require('../build/Release/avro');

describe("The File oriented actions with avro", function(){

  it("should be able to read in schema from file", function(){
    avro.onschema = function(schema){
      expect(schema).toBeTruthy();
    }
    avro.setSchema("../cpx.json"); 

  });

});


