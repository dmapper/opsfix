var _ = require('lodash');
var mongo = require('mongodb');
var async = require('async');

module.exports = function(url, excludes, callback) {
  var resultes = {};

  mongo.MongoClient.connect(url, function (err, db) {
    if (err) throw err;

    db.collections(function (err, collections) {

      var cols = [];
      collections.forEach(function (col) {
        var name = name = col.collectionName;
        var isOps = name.indexOf('_ops') !== -1 || name.indexOf('ops_') !== -1;
        if (!isOps && excludes.indexOf(name) === -1) {
          cols.push(name);
        }
      });

      if (!callback) console.log('Collections: ', cols.join(', '));
      async.eachSeries(cols, fixCollection.bind(null, db), function () {
        db.close();
        if (callback) {
          callback(null, resultes);
        } else {
          process.exit();
        }
      });
    });
  });

  function fixCollection(db, snapshotsCollectionName, done){
    var oplogsCollectionName = snapshotsCollectionName + '_ops';

    var snapshotsCollection = db.collection(snapshotsCollectionName);
    var oplogsCollection = db.collection(oplogsCollectionName);


    snapshotsCollection.find({"_v" : {$exists: true}, _m: {$exists: true}}).toArray(function (err, snapshots) {
      if (err) throw err;
      snapshots = snapshots || [];

      var opsIds = snapshots.map(function(snapshot){
        var version = snapshot._v - 1;
        return snapshot._id + ' v' + version;
      });

      oplogsCollection.find({_id: {$in: opsIds}}).toArray(function (err, ops) {
        var existsOpsIds = ops.map(function(op){
          return op.name;
        });


        var docs = prepareDocArray(snapshots, existsOpsIds);
        console.log('collection', snapshotsCollectionName, snapshots.length, ops.length);
        //return done()

        if (docs.length > 0) {
          oplogsCollection.insert(docs, function(err, r){
            if (err){
              console.log('Err while insert docs', err, docs, oplogsCollectionName)
            }
            console.log('Results', r.insertedCount, r.insertedIds);
            done();
          });
        } else {
          done()
        }
      });
    });
  }

};

function prepareDocArray(snapshots, existsOpsIds){
  var timestamp = Date.now();

  var docs = [];

  snapshots.forEach(function(snapshot){

    var version = snapshot._v - 1;
    var id = snapshot._id;

    if (existsOpsIds.indexOf(id) === -1) {

      var doc = {
        "_id": id + " v" + version,
        "m": {
          "ts": timestamp
        },
        "v": version,
        "name": id
      };

      if (snapshot._data === null) {
        // DELETE
        doc.del = true
      } else {
        // CREATE
        var snap = castToSnapshot(snapshot);
        doc.create = snap.data
      }

      docs.push(doc);
    }

  });

  return docs;
}

function castToSnapshot(doc) {
  if (!doc) return;
  var type = doc._type;
  var v = doc._v;
  var docName = doc._id;
  var data = doc._data;
  var meta = doc._m;
  if (data === void 0) {
    doc = shallowClone(doc);
    delete doc._type;
    delete doc._v;
    delete doc._id;
    delete doc._m;
    return {
      data: doc
      , type: type
      , v: v
      , docName: docName
      , m: meta
    };
  }
  return {
    data: data
    , type: type
    , v: v
    , docName: docName
    , m: meta
  };
}

function shallowClone(object) {
  var out = {};
  for (var key in object) {
    out[key] = object[key];
  }
  return out;
}
