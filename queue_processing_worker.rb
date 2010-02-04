class QueueProcessingWorker < BackgrounDRb::MetaWorker
  set_worker_name :queue_processing_worker
  set_no_auto_load true #insure QueueProcessingWorker is not loaded on bdrb start up

  # worker initialization method
  def create(args = nil)
    logger.info "QueueProcessingWorker startup: #{Time.now}"
  end

  def start_running_jobs
    NewRelic::Agent.manual_start
    while true
      begin
        QueueEntry.run_next_job
        sleep 10 # sleep this much before polling for a new job
      rescue # workers should never die but only log errors and try again
        logger.info "QueueProcessingWorker Error: #{$!}"
        logger.info "QueueProcessingWorker: #{Time.now}\n" + $!.backtrace.join("\n")
      end
    end
  end
end
