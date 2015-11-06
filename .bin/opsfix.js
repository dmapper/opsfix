#!/usr/bin/env node

var fixer = require('../index');
var argv = require('yargs')
    .usage('Usage: $0 -u [url] -e [collections list]')
    .alias('u', 'url')
    .alias('e', 'excludes')
    .describe('u', 'mongodb url')
    .describe('e', 'collections to exclude')
    .default('e', 'sessions,objects')
    .example('$0 -u mongodb://localhost:27017/idg', 'create initial ops docs for all snapshots')
    .argv;

var url = argv.u;
var excludes = argv.e.split(',');

fixer(url, excludes);

