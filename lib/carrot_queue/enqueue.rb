module CarrotQueue
  module Enqueue
    module ClassMethods
      def enqueue(queue, msg)
        tries = 0
        begin
          queue(queue).publish(msg, :persistent => true)
        rescue Carrot::AMQP::Server::ServerDown => e
          tries += 1; Carrot.reset
          tries == 1 ? retry : raise(e)
        end
      end
    end

    module InstanceMethods
    end

    def self.included(base)
      base.send :include, InstanceMethods
      base.send :extend, ClassMethods
    end
  end
end