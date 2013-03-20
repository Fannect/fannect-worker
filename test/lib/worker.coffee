require "mocha"
should = require "should"
Job = require "../../common/jobs/Job"
redis = require "../../common/utils/redis"
queue = redis(null, "queue")
queue2 = redis(null, "queue2")
queue3 = redis(null, "queue3")

Worker = require "../../lib/worker"

class TestJob extends Job
   constructor: (data = {}) ->
      data.is_locking = false
      data.type = "test"
      super data
   run: (cb) => setTimeout cb, 50

class TestLockJob extends Job
   constructor: (data = {}) ->
      data.is_locking = true
      data.locking_id = "test_lock_job_id"
      data.type = "test_lock"
      super data
   run: (cb) =>
      setTimeout cb, 100

Job.types.test = TestJob
Job.types.test_lock = TestLockJob

describe "Worker", () ->

   describe "basic processing", () ->

      beforeEach (done) ->
         queue.multi()
         .ltrim("job_queue", -1, 0)
         .lpush("job_queue", '{"type":"test","meta":{"job_num":1}}')
         .lpush("job_queue", '{"type":"test","meta":{"job_num":2}}')
         .lpush("job_queue", '{"type":"test","meta":{"job_num":3}}')
         .exec done

      it "should process all queued jobs", (done) ->
         processCount = 0
         completeCount = 0

         worker = new Worker()
         worker.start()
         worker.on("process", -> processCount++)
         worker.on("complete", -> completeCount++)
         worker.on "waiting", () -> 
            processCount.should.equal(3)
            completeCount.should.equal(processCount)
            worker.stop()
            done()

      it "should process queued jobs without lock in order", (done) ->
         processCount = 0

         worker = new Worker()
         worker.start()
         
         worker.on "process", (job) ->
            job.meta.job_num.should.equal(++processCount)
         
         worker.on "waiting", () -> 
            processCount.should.equal(3)
            worker.stop()
            done()

      it "should process queued jobs in order when parallelized", (done) ->    
         processCount = 0

         worker1 = new Worker()
         worker2 = new Worker(queue2)
         
         worker1.start()
         setTimeout (-> worker2.start()), 5
         
         worker1.on "process", (job) -> 
            job.meta.job_num.should.equal(++processCount)
         
         worker2.on "process", (job) -> 
            job.meta.job_num.should.equal(++processCount)
         
         worker2Done = false

         worker1.on "waiting", () -> 
            processCount.should.equal(3)
            worker2Done.should.be.true
            worker1.stop()
            done() # call done here because it should be the last one
            
         worker2.on "waiting", () -> 
            worker2.stop()
            worker2Done = true

   describe "waiting behavior", () ->

      it "should change from waiting state when a job is queued", (done) ->
         worker = new Worker()

         worker.start()

         completed = 0
         startedWaiting = false
         worker.on "complete", () -> completed++

         jobDef = '{"type":"test","meta":{"job_num":1}}'
         
         worker.on "waiting", () ->
            if completed == 0
               startedWaiting = true
               setTimeout () ->
                  queue2.multi()
                  .ltrim("job_queue", -1, 0)
                  .lpush("job_queue", jobDef)
                  .publish("new_job", jobDef)
                  .exec()
               , 10
            else
               completed.should.equal(1)
               startedWaiting.should.be.true
               worker.stop()
               done()

      it "should only process a job once when multiple workers waiting", (done) ->
         worker1 = new Worker()
         worker2 = new Worker(queue2)

         worker1.start()
         worker2.start()

         jobDef = '{"type":"test","meta":{"job_num":1}}'

         completed = 0
         process = 0

         completedCallback = () ->
            completed++
            setTimeout () ->
               process.should.equal(1)
               completed.should.equal(1)
               worker1.stop()
               worker2.stop()
               done()
            , 110

         worker1.on "complete", completedCallback
         worker2.on "complete", completedCallback
         worker1.on "process", () -> process++
         worker2.on "process", () -> process++

         worker1.on "waiting", () ->
            return unless completed == 0
            # This is required in case worker2 gets job instead of worker1
            setTimeout () ->
               return unless completed == 0
               queue3.multi()
               .ltrim("job_queue", -1, 0)
               .lpush("job_queue", jobDef)
               .publish("new_job", jobDef)
               .exec()
            , 60

   describe "processing with locking", () ->

      beforeEach (done) ->
         queue.multi()
         .del("lock:test_lock_job_id")
         .ltrim("job_queue", -1, 0)
         .lpush("job_queue", '{"type":"test_lock","meta":{"job_num":1}}')
         .lpush("job_queue", '{"type":"test","meta":{"job_num":2}}')
         .lpush("job_queue", '{"type":"test_lock","meta":{"job_num":3}}')
         .exec done

      it "should lock jobs correctly", (done) ->    
         processCount = 0

         worker1 = new Worker()
         worker2 = new Worker(queue2)
         
         worker1.start()
         setTimeout (-> worker2.start()), 5
         
         completed = 0

         worker1.on "complete", (job) -> 
            [1,3].should.include(job.meta.job_num)
            completed++
            
         worker2.on "complete", (job) -> 
            job.meta.job_num.should.equal(2)
            completed++

         worker1.on "waiting", () -> 
            completed.should.equal(3)
            worker1.stop()
            done()