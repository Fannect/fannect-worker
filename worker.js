require("coffee-script");

redis = (require("../common/utils/redis"))(process.env.REDIS_QUEUE_URL, "queue");
mongoose = require("mongoose");
mongooseTypes = require("mongoose-types");

mongoose.connect(process.env.MONGO_URL || "mongodb://admin:testing@linus.mongohq.com:10064/fannect");
// mongoose.connect(process.env.MONGO_URL || "mongodb://halloffamer:krzj2blW7674QGk3R1ll967LO41FG1gL2Kil@fannect-production.member0.mongolayer.com:27017/fannect-production")
mongooseTypes.loadTypes(mongoose);

sendgrid = new (require("sendgrid-web"))({ 
   user: process.env.SENDGRID_USER or "fannect", 
   key: process.env.SENDGRID_PASSWORD or "1Billion!" 
});

// Colors
red = "\u001b[31m";
green = "\u001b[32m";
white = "\u001b[37m";
reset = "\u001b[0m";

Worker = require("./lib/worker");

worker = new Worker();
worker.start();

worker.on("error", function (err, job) {
   console.log(red + "ERROR: " + JSON.stringify(err)); 
   console.log(red + "\tJOB: " + JSON.stringify(job));
   console.log("");

   error = err ? err.stack || err : "(none)"

   sendgrid.send({
      to: process.env.EMAIL_TO or "blake@fannect.me"
      from: "logger@fannect.me"
      subject: "Worker Error"
      html: "Error: " + error + "<br>Job: " + JSON.stringify(job)
   });
});

process.on("SIGTERM", function () {
   worker.stop();
});