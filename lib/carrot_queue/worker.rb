require "multi_json"

module CarrotQueue
  class Worker

    class << self
      def shutdown?
        @shutdown == true
      end

      def shutdown!
        @shutdown = true
      end
    end

    attr_accessor :jobs_per_fork

    def initialize(queue_name, perform_proc)
      @queue_name     = queue_name.to_s
      @perform_proc   = perform_proc
      @jobs_per_fork  = 50
    end

    def queue
      @queue ||= { jobs: CarrotQueue.queue(@queue_name),
                   errors: CarrotQueue.queue([@queue_name, "errors"].join(".")),
                   retryable: CarrotQueue.queue([@queue_name, "retryable"].join(".")) }
    end

    def process_jobs
      while !CarrotQueue::Worker.shutdown?
        begin
          if msg = queue[:jobs].pop(:ack => true)
            fork do
              @jobs_per_fork.times do |i|
                break unless msg

                begin
                  @perform_proc.call(msg)
                rescue Exception => e
                  queue[:retryable].publish(msg)
                  queue[:errors].publish(MultiJson.encode({
                    msg: msg,
                    error: { type: e.class.to_s, msg: e.message,
                             backtrace: e.backtrace.join("\n") } }))
                ensure
                  queue[:jobs].ack
                end

                if i < (@jobs_per_fork - 1)
                  msg = queue[:jobs].pop(:ack => true)
                end
              end
            end # fork
            Process.waitall
          else
            sleep 0.10
          end
        rescue Carrot::AMQP::Server::ServerDown => e
          puts "[Error] #{e.class.to_s}: #{e.message}"
          Carrot.reset; @queue = nil
          sleep(1); retry
        end
      end # while
    end # def process_jobs

  end
end

%w(USR1 TERM INT).each do |sig|
  Signal.trap(sig) do
    CarrotQueue::Worker.shutdown!
  end
end
