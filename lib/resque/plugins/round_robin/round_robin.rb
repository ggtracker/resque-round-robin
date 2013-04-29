
module Resque::Plugins
  module RoundRobin
    def filter_busy_queues qs
      busy_queues = Resque::Worker.working.map { |worker| worker.job["queue"] }.compact
      Array(qs.dup).compact - busy_queues
    end

    def rotated_queues
      @n ||= 0
      @n += 1
      rot_queues = queues # since we rely on the resque-dynamic-queues plugin, this is all the queues, expanded out
      if rot_queues.size > 0
        @n = @n % rot_queues.size
        rot_queues.rotate(@n)
      else
        rot_queues
      end
    end

    def queue_depth queuename
      busy_queues = Resque::Worker.working.map { |worker| worker.job["queue"] }.compact
      # find the queuename, count it.
      busy_queues.select {|q| q == queuename }.size
    end

    DEFAULT_QUEUE_DEPTH = 0
    def should_work_on_queue? queuename

      # megahack :(
      #
      # tried to use an env variable for a less hacky hack, but i dont
      # know how to get env variables into the EY resque daemon, and
      # Ive been fighting to get this to round-robining system to work
      # for five hours now.  If you hate this, you can fix it.
      #
      if Resque.size("python") > 2
#        $stderr.puts "not working on queue, python queue too big. (I have #{queues.size} queues)"
        return false
      end

#      $stderr.puts "working on queue, python queue is small. (I have #{queues.size} queues)"

      return true if not ['replays-low, replays-high'].include?(queuename)
    end

    # if any of our queues are wildcarded, then we want to round robin among them
    def should_round_robin?
#      $stderr.puts "srr, @srr = #{@srr} queues=#{queues} @queues=#{@queues}"
      return @srr unless @srr.nil?
      @srr = @queues[0].include? '*'
    end

    def reserve_with_round_robin

      if not should_round_robin?
        return reserve_without_round_robin
      end

      qs = rotated_queues
      qs.each do |queue|
        log! "Checking #{queue}"
        if should_work_on_queue?(queue) && job = Resque::Job.reserve(queue)
          log! "Found job on #{queue}"
          return job
        end
        # Start the next search at the queue after the one from which we pick a job.
        @n += 1
      end

      nil
    rescue Exception => e
      log "Error reserving job: #{e.inspect}"
      log e.backtrace.join("\n")
      raise e
    end

    def self.included(receiver)
      receiver.class_eval do
        alias reserve_without_round_robin reserve
        alias reserve reserve_with_round_robin
      end
    end

  end # RoundRobin
end # Resque::Plugins

