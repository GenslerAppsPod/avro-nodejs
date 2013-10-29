var fs = require('fs');
var addon = require('../build/Release/avro');

describe("Avro File decoding", function() {
  it("should return the datum contained in the file", function(done){
    var avro = new addon.Avro();
    var datumCount = 0;
    avro.decodeFile("test.bin",
      function(datum){
		  expect(datum.im).toEqual(datumCount++);
		  if(datumCount >= 99){
			  avro.close();
			  done();
		  }
		},
		function(error){
		  console.log(error);
	    }
	  );
	});
});
