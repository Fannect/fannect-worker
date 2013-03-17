require("coffee-script");

redis = (require("../common/utils/redis"))(process.env.REDIS_URL)
mongoose = require "mongoose"
mongooseTypes = require "mongoose-types"

mongoose.connect(process.env.MONGO_URL || "mongodb://admin:testing@linus.mongohq.com:10064/fannect")
// mongoose.connect(process.env.MONGO_URL || "mongodb://halloffamer:krzj2blW7674QGk3R1ll967LO41FG1gL2Kil@fannect-production.member0.mongolayer.com:27017/fannect-production")
mongooseTypes.loadTypes mongoose

// db = mongoose.connection
// db.on("error", console.error.bind(console, "mongo connection error:"))
// db.on("open", console.log.bind(console, "mongo connection opened"))
// db.on("close", console.log.bind(console, "mongo connection closed"))

Worker = require("./lib/worker");

worker = new Worker();
worker.start();