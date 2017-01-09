var dir = "/Users/zhjin/Desktop/projects/DistributedML/"
var express = require('express');
var app = express();

//http://expressjs.com/en/guide/database-integration.html#cassandra
var cassandra = require('cassandra-driver')
var KEYSPACE = "tickerkeyspace"
var client = new cassandra.Client({ contactPoints: ['localhost'], keyspace: KEYSPACE})


//To serve static files such as images, CSS files, and JavaScript files, use the express.static built-in middleware function in Express.
//http://expressjs.com/en/starter/static-files.html
app.use(express.static(dir))


app.get("/quotes/:stock/:state", function(req, res) {
	var stock = req.params['stock']
	var state = req.params['state']
	console.log("Requesting Stock data: name = " + stock + ", state = " + state)
	//res.send(req.params)
	if (state == "current") {
		client.execute('select * from ticker_final', function (err, result) {
  			if (err) throw err
  			res.json(result.rows[0])
		})
	}
	else if (state == "historical") {
		client.execute('select * from ticker_final', function (err, result) {
  			if (err) throw err
  			res.json(result.rows)
		})
	}

})

var server = app.listen(8081, function() {
	var host = server.address().address
	var port = server.address().port

	console.log("Example app listening at http://%s:%s", host, port)

})

