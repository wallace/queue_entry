class QueueProcessingWorker < BackgrounDRb::MetaWorker
  set_worker_name :queue_processing_worker
  def create(args = nil)
    # this method is called, when worker is loaded for the first time
     
    #TODO -- check that queue contains recurring tasks such as auto-enrollment
    #processing, letter generation and curriculum status check for all accounts
    #(meta task that calls methods that generate the tasks per account)
     
    logger.info "startup"
    TzTime.zone = TimeZone.new(0)
    QueueEntry.find_and_restart_already_started_jobs_for_current_server
  end

  def run_next_job
    logger.info "run jext job"
    QueueEntry.run_next_job
  end

end

