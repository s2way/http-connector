ElasticSearch = require './../src/ElasticSearch'
expect = require 'expect.js'

describe 'The ElasticSearch connector', ->

    describe 'when call constructor method', ->

        it 'should throw an exception if the data source is invalid', ->
            expect ->
                new ElasticSearch
            .to.throwError (e) ->
                expect(e.name).to.be 'Error'
                expect(e.message).to.be 'Invalid ES data source'

        it 'should call elasticsearch lib without auth params', (done) ->

            dataSource =
                host: 'localhost'
                port: '8888'
                protocol: 'https'
                log: 'test_log'
                keepAlive: true
                timeout: 5000

            expectedParams =
                host:
                    host: dataSource.host
                    port: dataSource.port
                    protocol: dataSource.protocol
                log: dataSource.log 
                keepAlive: dataSource.keepAlive
                requestTimeout: dataSource.timeout

            deps =
                elasticsearch:
                    Client: (params) ->
                        expect(params).to.eql expectedParams
                        done()

            new ElasticSearch dataSource, deps

        it 'should call elasticsearch lib with auth params', (done) ->

            dataSource =
                host: 'localhost'
                port: '8888'
                protocol: 'https'
                log: 'test_log'
                keepAlive: true
                timeout: 5000
                user: 'default_es_user'
                password: 'default_es_password'

            expectedParams =
                host:
                    host: dataSource.host
                    port: dataSource.port
                    protocol: dataSource.protocol
                    auth: "#{dataSource.user}:#{dataSource.password}"
                log: dataSource.log 
                keepAlive: dataSource.keepAlive
                requestTimeout: dataSource.timeout

            deps =
                elasticsearch:
                    Client: (params) ->
                        expect(params).to.eql expectedParams
                        done()

            new ElasticSearch dataSource, deps

    describe 'when call query method', ->

        it 'validate the default params passed', (done) ->

            expectedOptions =
                index: null
                type: null
                body: null

            esClient = new ElasticSearch {}
            esClient.client =
                search: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.query()

        it 'validate the correctly params passed', (done) ->

            params =
                index: 'any_index'
                type: 'any_type'
                query: 'any_query'
                scroll: 'any_scroll'
                size: 'any_size'
                source: 'any_source'

            expectedOptions =
                index: params.index
                type: params.type
                body: params.query
                scroll: params.scroll
                size: params.size
                _source: params.source

            esClient = new ElasticSearch {}
            esClient.client =
                search: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()
            esClient.query params, ->

    describe 'when call scroll method', ->

        it 'validate the default params passed', (done) ->

            expectedOptions =
                scrollId: null
                scroll: null

            esClient = new ElasticSearch {}
            esClient.client =
                scroll: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.scroll()

        it 'validate the correctly params passed', (done) ->

            params =
                scrollId: 'any_scrollId'
                scroll: 'any_scroll'

            expectedOptions =
                scrollId: params.scrollId
                scroll: params.scroll

            esClient = new ElasticSearch {}
            esClient.client =
                scroll: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()
            esClient.scroll params, ->

    describe 'when call clearScroll method', ->

        it 'validate the correctly params passed', (done) ->

            expectedParams =
                scrollId: 'any_scrollId'

            esClient = new ElasticSearch {}
            esClient.client =
                clearScroll: (scrollId, callback) ->
                    expect(scrollId).to.eql expectedParams
                    done()
            esClient.clearScroll expectedParams.scrollId, ->

    describe 'when call get method', ->

        it 'validate the default params passed', (done) ->

            expectedOptions =
                index: null
                type: null
                id: 0

            esClient = new ElasticSearch {}
            esClient.client =
                get: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.get()

        it 'validate the correctly params passed', (done) ->

            params =
                index: 'any_index'
                type: 'any_type'
                id: 'any_id'

            expectedOptions =
                index: params.index
                type: params.type
                id: params.id

            esClient = new ElasticSearch {}
            esClient.client =
                get: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.get params, ->

    describe 'when call save method', ->

        it 'validate the default params passed', (done) ->

            expectedOptions =
                index: null
                type: null
                body: null

            esClient = new ElasticSearch {}
            esClient.client =
                index: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.save()

        it 'validate the correctly params passed', (done) ->

            params =
                index: 'any_index'
                type: 'any_type'
                data: 'any_data'
                id: 'any_id'

            expectedOptions =
                index: params.index
                type: params.type
                body: params.data
                id: params.id

            esClient = new ElasticSearch {}
            esClient.client =
                index: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.save params, ->

    describe 'when call create method', ->

        it 'validate the default params passed', (done) ->

            expectedOptions =
                index: null
                type: null
                id: null
                body: null

            esClient = new ElasticSearch {}
            esClient.client =
                create: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.create()

        it 'validate the correctly params passed', (done) ->

            params =
                index: 'any_index'
                type: 'any_type'
                id: 'any_id'
                data: 'any_data'

            expectedOptions =
                index: params.index
                type: params.type
                id: params.id
                body: params.data

            esClient = new ElasticSearch {}
            esClient.client =
                create: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.create params, ->

    describe 'when call update method', ->

        it 'validate the default params passed', (done) ->

            expectedOptions =
                index: null
                type: null
                id: 0
                body:
                    doc: null

            esClient = new ElasticSearch {}
            esClient.client =
                update: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.update()

        it 'validate the correctly params passed', (done) ->

            params =
                index: 'any_index'
                type: 'any_type'
                id: 'any_id'
                data: 'any_data'
                version: 'any_version'
                ttl: 'any_ttl'

            expectedOptions =
                index: params.index
                type: params.type
                id: params.id
                body:
                    doc: params.data
                version: params.version
                ttl: params.ttl

            esClient = new ElasticSearch {}
            esClient.client =
                update: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.update params, ->

    describe 'when call deleteByQuery method', ->

        it 'validate the correctly params passed', (done) ->

            params =
                index: 'any_index'
                query: 'any_query'

            expectedOptions =
                index: params.index
                body:
                    query: params.query

            esClient = new ElasticSearch {}
            esClient.client =
                deleteByQuery: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.deleteByQuery params, ->

    describe 'when call bulk method', ->

        it 'validate the correctly params with refreshIndex default value ', (done) ->

            params =
                data: 'any_data'

            expectedOptions =
                body:
                    data: params.data
                refresh: false

            esClient = new ElasticSearch {}
            esClient.client =
                bulk: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.bulk params, ->

        it 'validate the correctly params with refreshIndex param is true ', (done) ->

            params =
                data: 'any_data'

            expectedOptions =
                body:
                    data: params.data
                refresh: false

            esClient = new ElasticSearch {}
            esClient.client =
                bulk: (options, callback) ->
                    expect(options).to.eql expectedOptions
                    done()

            esClient.bulk params, ->

    describe 'when call indexExists method', ->

        it 'validate the correctly params passed', (done) ->

            expectedIndex =
                index: 'any_index'

            esClient = new ElasticSearch {}
            esClient.client =
                indices:
                    exists: (index, callback) ->
                        expect(index).to.eql expectedIndex
                        done()

            esClient.indexExists expectedIndex.index, ->

    describe 'when call createIndex method', ->

        it 'validate the default params passed', (done) ->

            expectedOptions =
                index: undefined
                body: {}

            esClient = new ElasticSearch {}
            esClient.client =
                indices:
                    create: (options, callback) ->
                        expect(options).to.eql expectedOptions
                        done()

            esClient.createIndex()

        it 'validate the correctly params passed', (done) ->

            params =
                index: 'any_index'
                mappings: 'any_mappings'
                settings: 'any_settings'

            expectedOptions =
                index: params.index
                body:
                    mappings: params.mappings
                    settings: params.settings

            esClient = new ElasticSearch {}
            esClient.client =
                indices:
                    create: (options, callback) ->
                        expect(options).to.eql expectedOptions
                        done()

            esClient.createIndex params, ->

    describe 'when call getMapping method', ->

        it 'validate the correctly params passed', (done) ->

            expectedParams = 'any_params'

            esClient = new ElasticSearch {}
            esClient.client =
                indices:
                    getMapping: (params, callback) ->
                        expect(params).to.eql expectedParams
                        done()

            esClient.getMapping expectedParams, ->

    describe 'when call putMapping method', ->

        it 'validate the default params passed', (done) ->

            expectedOptions =
                index: null
                type: null
                ignoreConflicts: false
                body:
                    undefined:
                        properties: undefined

            esClient = new ElasticSearch {}
            esClient.client =
                indices:
                    putMapping: (options, callback) ->
                        expect(options).to.eql expectedOptions
                        done()

            esClient.putMapping()

        it 'validate the correctly params passed', (done) ->

            params =
                index: 'any_index'
                type: 'any_type'
                ignoreConflicts: true
                mapping: 'any_mapping'

            expectedOptions =
                index: params.index
                type: params.type
                ignoreConflicts: true
                body:
                    "#{params.type}":
                        properties: params.mapping

            esClient = new ElasticSearch {}
            esClient.client =
                indices:
                    putMapping: (options, callback) ->
                        expect(options).to.eql expectedOptions
                        done()

            esClient.putMapping params, ->

    describe 'when call ping method', ->

        it 'validate the correctly params passed', (done) ->

            expectedParams = 'any_params'

            esClient = new ElasticSearch {}
            esClient.client =
                ping: (params, callback) ->
                    expect(params).to.eql expectedParams
                    done()

            esClient.ping expectedParams, ->

    describe 'when call close method', ->

        it 'validate if the close method was called', ->

            closeCalled = false

            esClient = new ElasticSearch {}
            esClient.client =
                close: ->
                    closeCalled = true

            esClient.close()
            expect(closeCalled).to.be.ok()