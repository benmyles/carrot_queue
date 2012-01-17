require "carrot"

require "carrot_queue/enqueue"
require "carrot_queue/worker"

module CarrotQueue
  def self.queue(q)
    Carrot.queue(q, {durable: true})
  end

  include CarrotQueue::Enqueue
end

CQ = CarrotQueue