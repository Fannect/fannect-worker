require "mocha"
should = require "should"
request = require "request"
mongoose = require "mongoose"
async = require "async"

process.env.REDIS_URL = null # "redis://redistogo:f74caf74a1f7df625aa879bf817be6d1@perch.redistogo.com:9203"
process.env.MONGO_URL = "mongodb://admin:testing@linus.mongohq.com:10064/fannect"
process.env.NODE_ENV = "production"
process.env.NODE_TESTING = true

require "./lib/worker"