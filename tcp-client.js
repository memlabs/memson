/* Or use this example tcp client written in node.js.  (Originated with 
example code from 
http://www.hacksparrow.com/tcp-socket-programming-in-node-js.html.) */

var net = require('net');
const BSON = require('bson');
const Long = BSON.Long;

// Serialize a document


var client = new net.Socket();
client.connect(17653, '127.0.0.1', function() {
	console.log('Connected');
	const doc = { long: Long.fromNumber(100), james:'perry' };
    const data = BSON.serialize(doc);
    client.write(data);
});

client.on('data', function(data) {
	console.log('Received: ' + data);
	client.destroy(); // kill client after server's response
});

client.on('close', function() {
	console.log('Connection closed');
});