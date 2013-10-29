var fs = require('fs');
var addon = require('../build/Release/avro');

var avro = new addon.Avro();


describe("An Async test for Avro", function(){
	it("should emit an event when the avro object has been closed", function(done){
		avro.onclose = function(){
			expect(true).toEqual(true);
			done();
		}
		avro.close();
	});
});