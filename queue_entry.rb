class QueueEntry < ActiveRecord::Base
  serialize :action_args
  validates_presence_of :scheduled_for, :category
  before_create :set_queued_on_time
  belongs_to :user
  belongs_to :account
  belongs_to :resource, :class_name => "ResourceDocument", :foreign_key => "resource_id"
  
  tz_time_attributes :queued_on, :started_on, :completed_on, :scheduled_for
  
  MAX_NUMBER_OF_JOBS_PER_SERVER = 4

  def QueueEntry.report_long_running_jobs_older_than(num)
    old_jobs = QueueEntry.find_started_jobs_older_than(num)
    old_jobs.collect!(&:to_yaml)

    Notifier.deliver_long_running_job("Long Running Job older than #{num / 60} minutes", old_jobs.join("\n ------------ \n")) unless old_jobs.blank?
    return { :detail_message => nil, :time_complete => TzTime.now, :resource_id => nil }
  end
  def QueueEntry.find_started_jobs_older_than(num)
    QueueEntry.find(:all,
                    :conditions => ["started_on < ?", TzTime.now - num.to_i],
                    :order => "scheduled_for")
  end
  def QueueEntry.find_already_started_jobs_for_server(server_id)
    QueueEntry.find(:all, 
                    :conditions => ["queue_job_server_id = ? and started_on is not null", server_id.to_s], 
                    :order => "scheduled_for")
  end
  #Returns the next job on the queue that is unassigned (not started and not queued).
  def QueueEntry.find_next_job_for_action_worker
    job = QueueEntry.find(:first, 
                          :conditions => [ "started_on is null and scheduled_for <= ?", TzTime.now ],
                          :order => "scheduled_for")

    #insure that there is a strict job completion time order per account
    #ex: job 1 takes 5 minutes, job 2 takes 30 seconds both for account 3. we
    #must insure that job 1 is complete before job 2 can begin
    (job.nil? || QueueEntry.exists?(["account_id = ? and started_on is not null", job.account_id])) ? nil : job
  end
  #server_id is probably the IP addy of the queue server that makes this call
  def QueueEntry.get_next_job_from_queue(server_id, job_limit = true)
    queue_entry = nil

    QueueEntry.transaction do
      if (!job_limit || QueueEntry.find_all_by_queue_job_server_id(server_id).length < MAX_NUMBER_OF_JOBS_PER_SERVER)
        queue_entry = QueueEntry.find_next_job_for_action_worker 
        unless queue_entry.nil?
          queue_entry.update_attributes!({:started_on => TzTime.now, 
                                          :queue_job_server_id => server_id }) 
        end
      end
    end 

    queue_entry
  end
  def execute_action
    begin 
      klass = self.action_klass.constantize

      if self.action_id.nil? #action_id == nil indicates class action_method
        obj = klass
      else
        obj = klass.find(self.action_id.to_i)
      end

      result_hash = nil
      if self.action_args.blank?
        logger.info("     QueueEntry: #{self.id} - #{self.action_klass}.find(#{self.action_id}).send(#{self.action_method})")
        result_hash = obj.send(self.action_method)
      else
        logger.info("     QueueEntry: #{self.id} - #{self.action_klass}.find(#{self.action_id}).send(#{self.action_method.to_sym}, '#{self.action_args}')")
        new_args = case self.action_args
                   when Fixnum 
                     self.action_args
                   else
                     self.action_args.dup
                   end
        result_hash = obj.send(self.action_method, new_args)
      end
      logger.info("     QueueEntry: #{self.id} completed - result hash: '#{result_hash.inspect}'")

      #enforce that action_method conforms to execute_action specification
      if result_hash.class != Hash || !result_hash.has_key?(:detail_message) || !result_hash.has_key?(:time_complete) || !result_hash.has_key?(:resource_id)
        logger.error("     QueueEntry: #{self.id} - #{obj.inspect}\n action_method:'#{self.action_method}'\n action_args:'#{self.action_args}'\n result_hash:'#{result_hash.inspect}}'")
        raise "Invalid return type for method implementing execute_action interface.  Must return type Hash with the following keys - :detail_message, :time_complete, :resource_id"
      end

      self.detail_message = result_hash[:detail_message]
      self.completed_on = result_hash[:time_complete]
      self.resource_id = result_hash[:resource_id]
    rescue
      logger.fatal("     QueueEntry: #{self.id} - Exception: '#{$!}'")
      self.completed_on = TzTime.now
      self.detail_message.add_context("QueueEntry: #{self.id}", $!)
      self.detail_message.set_context #necessary because detail_message isn't saved through integration association unless attribute has changed and we cache context information in instance var to save on writes
      self.detail_message.failed
      Notifier.deliver_job_exception_notification(self.detail_message, $!, self)
    end
    self.destroy

    log_action
    generate_recurring_entry if self.recurring?
    notify_action_complete

    result_hash
  end
  #create new queue entry
  def generate_recurring_entry
    QueueEntry.create!(self.attributes.merge({ :started_on => nil, 
                                               :queue_job_server_id => nil, 
                                               :scheduled_for => scheduled_for ? scheduled_for + recurring_interval.to_i : TzTime.now + recurring_interval.to_i }))
  end
  #Returns true if this queue entry is a recurring entry (it has a
  #recurring_interval value that is greater than 0)
  def recurring?
    0 < self.recurring_interval.to_i
  end

  def QueueEntry.find_and_restart_already_started_jobs_for_current_server
    QueueEntry.find_already_started_jobs_for_server(QueueEntry.server_id).each do |queue_entry|
      queue_entry.execute_action
    end
  end

  def QueueEntry.server_id
    #TODO -- server_id needs to be a uniquely id across web-app servers that run these rake tasks
    "1"
  end

  def QueueEntry.run_next_job
    queue_entry = QueueEntry.get_next_job_from_queue(QueueEntry.server_id)
    unless queue_entry.nil?
      queue_entry.execute_action
    else
      logger.info "No job available for worker."
    end
  end

  def set_queued_on_time
    self.queued_on = TzTime.now
  end
  def detail_message
    @detail_message ||= DetailMessage.new
  end
  def detail_message=(dm)
    @detail_message = dm
  end
  def log_action
    #modify the attr_hash for LogEntry, all other attrs are the same
    attr_hash = self.attributes(:except => [:lock_version, :scheduled_for, :recurring_interval, :id ])
    attr_hash.merge!({"success_level" => detail_message.success_level})

    detail_message.log_entry = LogEntry.create!(attr_hash)
    detail_message.save!
  end
  #TODO - stubbed out for now; case statement on category for different letters on job completion
  def notify_action_complete
    email_info = self.category || "#{self.action_id}:#{self.action_method} action complete but no category provided"
    email_info_body = email_info
    email_info_body += "\n  Link to generated resource: http://#{self.account.nil? ? 'www' : self.account.subdomain}.rollbook.com#{self.resource.relative_url}" unless self.resource_id.nil?
    email_info_body += "\n\n #{self.detail_message.context}" unless self.detail_message.blank?
    Notifier.deliver_job_completion(email_info, email_info, User.find(self.user_id)) unless 0 == self.user_id.to_i
  end

#Used for testing purposes
  def QueueEntry.queue(account_id, user_id, scheduled_for = TzTime.now, recurring_interval = nil)
    QueueEntry.create!({:action_klass => "QueueEntry",
                       #:action_id => self.id.to_s,
                       :account_id => account_id,
                       :action_method => "sample_exception", 
                       :action_args => { :test_string => 'abc', :test_array => [1,2,3]}, 
                       :category => "sample",
                       :scheduled_for => scheduled_for,
                       :recurring_interval => recurring_interval,
                       :user_id => user_id})
  end
  def QueueEntry.sample_exception(args)
    puts "This method generates an exception"
    puts "to test how the execute_action method"
    puts "handles exceptions."
    puts "#{args}"
    raise "Oh no!"
  end
end
