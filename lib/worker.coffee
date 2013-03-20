EventEmitter = require("events").EventEmitter
redis = (require "../common/utils/redis").queue
Job = require "../common/jobs/Job"

class Worker extends EventEmitter
   constructor: (redis_client) ->
      super
      @state = "stopped"
      @redis = redis_client or redis
      @redis.on "message", (channel, message) =>
         if channel == "new_job" and message? and @state == "waiting"
            @state = "reactivating"
            @redis.unsubscribe "new_job", (err) =>
               return console.error "Failed to reactivate: ", err if err
               @state = "active"
               @emit("active")
               @process(message)

   start: () =>
      @state = "active"
      @emit("start")
      @nextJob()

   stop: (cb) =>
      @redis.unsubscribe "new_job", (err) =>
         if err
            cb(err) if err and cb
            return console.error "Failed to stop: ", err
         @state = "stopped"
         @emit("stop")
         cb() if cb

   wait: (cb) =>
      # Subscribe so it activates when a job is added
      @redis.subscribe "new_job", (err) =>
         if err
            cb(err) if err and cb
            return console.error "Failed to start waiting: ", err
         @state = "waiting"
         @emit("waiting")
         cb() if cb

   nextJob: (skip = 0) =>
      @redis.lindex "job_queue", -1 - skip, (err, jobDef) =>
         throw err if err

         # if no jobs in queue, move to waiting 
         return @wait() unless jobDef

         @process(jobDef, skip)

   process: (jobDef, index = 0) =>
      # create job from job definition JSON
      job = Job.create(jobDef)

      # process job and return
      run = () =>
         @redis.lrem "job_queue", -1, jobDef, (err, result) =>
            return @nextJob(index + 1) if result == 0
            @emit("process", job)
            job.run (err) =>
               if err then @emit("error", err, job)
               else @emit("complete", job)

               if job.is_locking
                  @redis.del "lock:#{job.locking_id}", (err, result) =>
                     @nextJob(0)
               else @nextJob(0)

      # check if job is locked
      if job.is_locking 
         rand = Math.round(Math.random() * new Date())
         temp_key = "temp:#{job.locking_id}-#{rand}"

         @redis.multi()
            .setnx(temp_key, true)
            .expire(temp_key, 1200)
            .renamenx(temp_key, "lock:#{job.locking_id}")
            .exec (err, replies) =>
               throw err if err
               
               # move to next job if failed to acquire lock
               if replies[replies.length - 1] == 0
                  @redis.del(temp_key)
                  return @nextJob(index + 1) 
            
               # lock acquired, now process
               run()                  

      else run()

module.exports = Worker