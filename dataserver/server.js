var express = require('express');

var app = express();

app.set('port', (process.env.PORT || 5000));

//http://expressjs.com/en/guide/database-integration.html#cassandra
var cassandra = require('cassandra-driver');
var KEYSPACE = "tickerkeyspace";
var client = new cassandra.Client({ contactPoints: ['localhost'], keyspace: KEYSPACE});


//To serve static files such as images, CSS files, and JavaScript files, use the express.static built-in middleware function in Express.
//http://expressjs.com/en/starter/static-files.html

app.use(express.static(__dirname + '/views'));


app.get("/quotes/:symbol/:state", function(req, res) {
	var symbol = req.params['symbol'];
	var state = req.params['state'];
	console.log("Requesting Stock data: symbol = " + symbol + ", state = " + state);
	//res.send(req.params)
	if (state == "current") {
		client.execute('select * from ' + symbol, function (err, result) {
  			if (err) console.log(err);
  			else res.json(result.rows[0]);
		});
	}
	else if (state == "historical") {
		client.execute('select * from ' + symbol, function (err, result) {
  			if (err) console.log(err);
  			else res.json(result.rows);
		});
	}

});

var server = app.listen(app.get('port'), function() {
	var host = server.address().address;
	var port = server.address().port;

	console.log("Example app listening at http://%s:%s", host, port);
	//console.log('Node app is running on port', app.get('port'));

})

