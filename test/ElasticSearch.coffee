ElasticSearch = require './../src/ElasticSearch'
expect = require 'expect.js'

describe 'The ElasticSearch connector', ->

    it 'should throw an exception if the data source is invalid', ->
        expect ->
            new ElasticSearch
        .to.throwError (e) ->
            expect(e.name).to.be 'Error'
            expect(e.message).to.be 'Invalid ES data source'

    it 'should validating host without port if the datasource port not passed', ->

        expectedHost = 'www.elasticsearch.com'

        deps =
            elasticsearch:
                Client: (params) ->
                    expect(params.host).to.eql expectedHost

        new ElasticSearch host: expectedHost, deps