require "mocha"
should = require "should"
Job = require "../../common/jobs/Job"
redis = require "../../common/utils/redis"
queue = redis(null, "queue")
queue2 = redis(null, "queue2")

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

   describe "processing with locking", () ->

      beforeEach (done) ->
         queue.multi()
         .del("lock:test_lock_job_id")
         .ltrim("job_queue", -1, 0)
         .lpush("job_queue", '{"type":"test_lock","meta":{"job_num":1}}')
         .lpush("job_queue", '{"type":"test","meta":{"job_num":2}}')
         .lpush("job_queue", '{"type":"test_lock","meta":{"job_num":3}}')
         .exec done

      it "should process queued jobs in order when parallelized", (done) ->    
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