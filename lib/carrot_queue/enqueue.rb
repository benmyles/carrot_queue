module CarrotQueue
  module Enqueue
    module ClassMethods
      def enqueue(queue, msg)
        queue(queue).publish(msg, :persistent => true)
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