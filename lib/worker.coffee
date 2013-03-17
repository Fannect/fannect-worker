EventEmitter = require("events").EventEmitter
redis = (require "../common/utils/redis").queue
Job = require "../common/jobs/Job"

class Worker extends EventEmitter
   constructor: (redis_client) ->
      super
      @state = "stopped"
      @redis = redis_client or redis
      @redis.on "message", (channel, message) =>
         if channel == "new_job"
            @redis.unsubscribe("new_job")
            @start()

   start: () =>
      @state = "started"
      @emit("start")
      @nextJob 0, (err) =>
         throw err if err
         @start() if @state != "stopped"

   stop: () =>
      @redis.unsubscribe("new_job")
      @state = "stopped"
      @emit("stop")

   wait: () =>
      # Subscribe so it activates when a job is added
      @redis.subscribe("new_job")
      @emit("waiting")

   nextJob: (skip = 0, cb) =>
      @redis.lindex "job_queue", -1 - skip, (err, jobDef) =>
         throw err if err

         # if no jobs in queue, move to waiting 
         return @wait() unless jobDef

         # create job from job definition JSON
         job = Job.create(jobDef)

         # process job and return
         process = () =>
            @redis.lrem "job_queue", -1, jobDef, (err, result) =>
               return @nextJob(skip + 1, cb) if result == 0
               @emit("process", job)
               job.run (err) =>
                  if err then @emit("error", err, job)
                  else @emit("complete", job)

                  if job.is_locking
                     @redis.del "lock:#{job.locking_id}", (err, result) ->
                        cb(err)
                  else cb(err)

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
                     return @nextJob(skip + 1, cb) 
               
                  # lock acquired, now process
                  process()                  

         else process()

module.exports = Worker