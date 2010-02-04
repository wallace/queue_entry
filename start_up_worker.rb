class StartUpWorker < BackgrounDRb::MetaWorker
  set_worker_name :start_up_worker
  QUEUE_WORKER_LIMIT = 2

  # this method is called on backgroundrb startup by default.
  # it releases any jobs run by this server that didn't complete back to the
  # job pool so any server may run them and then starts up the right amount of
  # queue_processing_worker's
  #
  #TODO -- check that queue contains recurring tasks such as auto-enrollment
  #processing, letter generation and curriculum status check for all accounts
  #(meta task that calls methods that generate the tasks per account)
  def create(args = nil)
    logger.info "StartUpWorker startup: #{Time.now}"

    QueueEntry.reset_started_jobs_for_current_server
    StartUpWorker.start_correct_number_of_workers

    #kill myself to conserve memory
    exit
  end

  def StartUpWorker.queue_worker_count
    MiddleMan.all_worker_info.values.flatten.select { |w| :queue_processing_worker == w[:worker] }.size
  end

  def StartUpWorker.start_correct_number_of_workers
    1.upto(QUEUE_WORKER_LIMIT) do |worker_key|
      MiddleMan.new_worker(:worker => :queue_processing_worker, :worker_key => worker_key)
      MiddleMan.worker(:queue_processing_worker, worker_key).async_start_running_jobs
    end
  end
end
