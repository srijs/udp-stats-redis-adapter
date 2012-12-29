var dgram = require('dgram');

var c = dgram.createSocket('udp4');


function test() {
  var obj = {
  	bucket: 'test',
  	value: Math.floor(Math.random() * 101),
  	kind: 'typie'
  };
  m = new Buffer(JSON.stringify(obj));
  c.send(m, 0, m.length, 6667, 'localhost', function (err, bytes) {
    console.log(err, bytes);
  });
}

setInterval(test, 1);