elasticsearch = require 'elasticsearch'

class ElasticSearch

    constructor: (dataSource, deps) ->
        throw new Error 'Invalid ES data source' unless dataSource?
        @_elasticsearch = deps?.elasticsearch or elasticsearch
        @client = new @_elasticsearch.Client(
            host: if dataSource.port? then dataSource.host + ':' + dataSource.port else dataSource.host
            log: dataSource.log
            keepAlive: false
            requestTimeout: dataSource.timeout or 30000
        )

    query: (params, callback) ->
        options =
            index: params?.index or null
            type: params?.type or null
            body: params?.query or null

        options.scroll = params?.scroll if params?.scroll?
        options.size = params?.size if params?.size?

        @client.search options, callback

    scroll: (params, callback) ->
        options =
            scrollId: params?.scrollId or null
            scroll: params?.scroll or null

        @client.scroll options, callback

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

        options.id = params?.id if params?.id?

        @client.index options, callback

    # Adds a typed JSON document in a specific index, making it searchable
    # If a document with the same index, type, and id already exists, an error will occur
    create: (params, callback) ->
        options =
            index: params?.index or null
            type: params?.type or null
            id: params?.id or 0
            body : params?.data or null

        @client.create options, callback

    bulk: (data, callback, refreshIndex = false) ->
        @client.bulk {body : data, refresh: refreshIndex}, callback

    indexExists: (index, callback) ->
        @client.indices.exists index : index, callback

    createIndex: (params, callback) ->
        @client.indices.create params, callback

    putMapping: (params, callback) ->
        @client.indices.putMapping params, callback

module.exports = ElasticSearch