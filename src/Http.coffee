'use strict'

path = require 'path'
uuid = require 'uuid'
qs = require 'querystring'

class HttpConnector

    constructor: (container) ->
        @restify = container?.restify || require 'restify'

    get: (params, callback) ->

        if params?.type is 'json'
            client = @restify.createJsonClient url:params.url
        else
            client = @restify.createStringClient url:params.url

        path = params?.path || ''
        path = "#{path}?#{qs.stringify(params.urlParams)}" if params?.urlParams?

        options =
            path: path

        options.headers = params.headers if params?.headers?

        client.get options, (err, req, res, data) ->
            return callback err if err?
            callback null, data

    post: (params, callback) ->

        options =
            url: params.url

        options.headers = params.headers if params?.headers?

        if params?.type is 'json'
            client = @restify.createJsonClient options
        else
            client = @restify.createStringClient options

        path = params?.path || ''

        client.post path, params?.data, (err, req, res, data) ->
            return callback err if err?
            callback null, data

    put: (params, callback) ->

        options =
            url: params.url

        options.headers = params.headers if params?.headers?

        if params?.type is 'json'
            client = @restify.createJsonClient options
        else
            client = @restify.createStringClient options

        path = params?.path || ''

        client.put path, params?.data, (err, req, res, data) ->
            return callback err if err?
            callback null, data

module.exports = HttpConnector
