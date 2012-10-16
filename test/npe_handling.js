var assert = require("assert");

var orient = require("../lib/orientdb"),
    GraphDb = orient.GraphDb,
    Server = orient.Server;

var serverConfig = require("../config/test/serverConfig");
var dbConfig = require("../config/test/dbConfig");

var server = new Server(serverConfig);
var graphdb = new GraphDb("temp", server, dbConfig);

function createVertexes(graphdb, callback) {
    graphdb.createVertex({ id:0 }, function (err, rootNode) {
        assert(!err, err);

        graphdb.createVertex({ name:"first node" }, function (err, firstNode) {
            assert(!err, err);

            graphdb.createVertex({ name:"second node" }, function (err, secondNode) {
                assert(!err, err);

                graphdb.createEdge(rootNode, firstNode, function (err, edge) {
                    assert(!err, err);

                    assert.equal(rootNode["out"][0], edge["@rid"]);
                    assert.equal(firstNode["in"][0], edge["@rid"]);

                    assert.equal(rootNode["@rid"], edge["out"]);
                    assert.equal(firstNode["@rid"], edge["in"]);

                    graphdb.createEdge(rootNode, secondNode, function (err) {
                        assert(!err, err);

                        var query = "delete from E where out = " + rootNode['@rid'] + " and in = " + secondNode['@rid'];
                        graphdb.command(query, function (err) {
                            assert(!err, err);

                            callback(rootNode, firstNode, secondNode);
                        });
                    });
                });
            });
        });
    });
}

graphdb.open(function (err) {

    assert(!err, "Error while opening the database: " + err);

    assert.equal("OGraphVertex", graphdb.getClassByName("OGraphVertex").name);
    assert.equal("OGraphVertex", graphdb.getClassByName("V").name);
    assert.equal("OGraphEdge", graphdb.getClassByName("OGraphEdge").name);
    assert.equal("OGraphEdge", graphdb.getClassByName("E").name);

    createVertexes(graphdb, function (rootNode, firstNode, secondNode) {

        var query = "Completely broken sql";
        graphdb.command(query, function (err) {
            assert(err, "no error received using wrong sql.");

            query = "DELETE EDGE FROM " + rootNode['@rid'] + " TO " + secondNode['@rid'];
            graphdb.command(query, function (err) {
                assert(err, "orientDB should send a NPE. if it's not, it's been fixed.");

                graphdb.close();
            });
        });
    });
});