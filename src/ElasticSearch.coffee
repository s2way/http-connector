elasticsearch = require 'elasticsearch'

class ElasticSearch

    constructor: (dataSource, deps) ->
        throw new Error 'Invalid ES data source' unless dataSource?
        @_elasticsearch = deps?.elasticsearch or elasticsearch
        @client = new @_elasticsearch.Client(
            host:
                host: dataSource.host
                auth: "#{dataSource.user}:#{dataSource.password}"
                port: dataSource.port or 9200
                protocol: dataSource.protocol or 'http'
            log: dataSource.log
            keepAlive: dataSource.keepAlive or false
            requestTimeout: dataSource.timeout or 30000
            apiVersion: '5.0'
        )

    errors: elasticsearch.errors

    query: (params, callback) ->
        options =
            index: params?.index or null
            type: params?.type or null
            body: params?.query or null

        options.scroll = params.scroll if params?.scroll?
        options.size = params.size if params?.size?
        options._source = params.source if params?.source?

        @client.search options, callback

    scroll: (params, callback) ->
        options =
            scrollId: params?.scrollId or null
            scroll: params?.scroll or null

        @client.scroll options, callback

    clearScroll: (scrollId, callback) ->
        @client.clearScroll {scrollId}, callback

    # Get a typed JSON from the index based on its id
    get: (params, callback) ->
        options =
            index: params?.index or null
            type: params?.type or null
            id: params?.id or 0

        @client.get options, callback

    # Stores a typed JSON document in an index, making it searchable
    # If no id is passed, ES will assign one
    # This is an upsert-like function, use create() if you want unique document index control
    save: (params, callback) ->
        options =
            index: params?.index or null
            type: params?.type or null
            body: params?.data or null

        options.id = params.id if params?.id?

        @client.index options, callback

    # Adds a typed JSON document in a specific index, making it searchable
    # If a document with the same index, type, and id already exists, an error will occur
    create: (params, callback) ->
        options =
            index: params?.index or null
            type: params?.type or null
            id: params?.id or null
            body : params?.data or null

        @client.create options, callback

    # Update parts of a document
    update: (params, callback) ->

        options =
            index: params?.index or null
            type: params?.type or null
            id: params?.id or 0
            body:
                doc: params?.data or null

        options.version = params.version if params?.version
        options.ttl = params.ttl if params?.ttl

        @client.update options, callback

    deleteByQuery: (params, callback) ->
        { index, query } = params
        @client.deleteByQuery {index, body: query: query}, callback

    bulk: (data, callback, refreshIndex = false) ->
        @client.bulk {body : data, refresh: refreshIndex}, callback

    indexExists: (index, callback) ->
        @client.indices.exists index : index, callback

    createIndex: (params, callback) ->
        options =
            index: params?.index

        options.body = {}
        options.body.mappings = params.mappings if params?.mappings?
        options.body.settings = params.settings if params?.settings?
        @client.indices.create options, callback

    # send: index and type
    getMapping: (params, callback) ->
        @client.indices.getMapping params, callback

    putMapping: (params, callback) ->
        options =
            index: params?.index or null
            type: params?.type or null
            ignoreConflicts: params?.ignoreConflicts or false
            body:
                "#{params?.type}":
                    properties: params?.mapping
        @client.indices.putMapping options, callback

    ping: (params, callback) ->
        @client.ping params, callback

    close: ->
        @client.close()

module.exports = ElasticSearch
