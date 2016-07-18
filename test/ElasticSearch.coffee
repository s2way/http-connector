ElasticSearch = require './../src/ElasticSearch'
expect = require 'expect.js'

describe 'The ElasticSearch connector', ->

    it 'should throw an exception if the data source is invalid', ->
        expect ->
            new ElasticSearch
        .to.throwError (e) ->
            expect(e.name).to.be 'Error'
            expect(e.message).to.be 'Invalid ES data source'

