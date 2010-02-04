class QueueEntry < ActiveRecord::Base
  serialize :action_args
  validates_presence_of :scheduled_for, :category
  before_create :set_queued_on_time
  belongs_to :user
  belongs_to :account
  belongs_to :resource, :class_name => "ResourceDocument", :foreign_key => "resource_id"

  attr_accessor :success_level, :mute_email

  QUEUEABLE_METHODS = {
    :Account => %[bulk_course_package_update check_all_auto_enrollments_for_new_enrollments check_all_enrollments_for_curriculum_updates process_all_plan_renewals process_all_account_triggers process_auto_enrollments process_curriculum_updates process_triggers],
    :Course => %[enroll_users destroy_enrollments update_enrollments],
    :CourseSession => %[enroll_users update_enrollments destroy_enrollments],
    :Integration => %[import clean_up_unfinished_integration_creations_older_than],
    :Letter => %[generate_communication],
    :LogEntry => %[clean_up_log_entries_older_than],
    :Report => %[generate_report]
  }
  
  MAX_NUMBER_OF_JOBS_PER_SERVER = 4

  def QueueEntry.find_started_jobs_older_than(num)
    QueueEntry.find(:all,
                    :conditions => ["started_on < ?", Time.now - num.to_i],
                    :order => "scheduled_for")
  end
  def QueueEntry.find_started_jobs_for_server(server_id)
    QueueEntry.find(:all, 
                    :conditions => ["queue_job_server_id = ? and started_on is not null", server_id.to_s], 
                    :order => "scheduled_for")
  end
  # Returns the next job on the queue that is unassigned (not started and not queued) and doesn't have a job from the same account already running.
  # insure that there is a strict job completion time order per account
  # ex: job 1 takes 5 minutes, job 2 takes 30 seconds both for account 3. we
  # must insure that job 1 is complete before job 2 can begin
  def QueueEntry.find_next_job_for_action_worker
    conds_str = ["started_on is null", "scheduled_for <= ?"]
    conds_arr = [Time.now]

    account_ids_with_running_jobs = QueueEntry.all(:conditions => ["started_on is not null"]).map! {|q| q.account_id }.compact
    unless account_ids_with_running_jobs.empty?
      conds_str << "account_id NOT IN (#{account_ids_with_running_jobs.join(',')})"
    end

    QueueEntry.find(:first, 
                    :conditions => [conds_str.join(" AND ")] + conds_arr,
                    :order => "scheduled_for")
  end
  #server_id is probably the IP addy of the queue server that makes this call
  def QueueEntry.get_next_job_from_queue(server_id, job_limit = true)
    queue_entry = nil

    QueueEntry.transaction do
      if (!job_limit || QueueEntry.find_all_by_queue_job_server_id(server_id).length < MAX_NUMBER_OF_JOBS_PER_SERVER)
        queue_entry = QueueEntry.find_next_job_for_action_worker 
        unless queue_entry.nil?
          queue_entry.update_attributes!({:started_on => Time.now, 
                                          :queue_job_server_id => server_id }) 
        end
      end
    end 

    queue_entry
  end
  def valid_action?
    valid_methods = QUEUEABLE_METHODS[klass_name_for_notifier.to_sym]
    valid_methods.include?(self.action_method)
  end
  def execute_action
    tz = Time.zone
    Time.zone = self.user.tzid unless self.user.blank?
    begin 
      # raise if action_klass, action_method are NOT present in QUEUEABLE_METHODS constant
      raise ArgumentError.new("Invalid action_klass/action_method combination") unless self.valid_action?

      klass = self.action_klass.constantize

      if self.action_id.nil? #action_id == nil indicates class action_method
        obj = klass
      else
        obj = klass.find(self.action_id.to_i)
      end

      result_hash = nil
      if self.action_args.blank?
        result_hash = obj.send(self.action_method)
      else
        new_args = case self.action_args
                   when Fixnum 
                     self.action_args
                   else
                     self.action_args.dup
                   end
        result_hash = obj.send(self.action_method, new_args)
      end

      #enforce that action_method conforms to execute_action specification
      if result_hash.class != Hash || !result_hash.has_key?(:success_level) || !result_hash.has_key?(:detail_message) || !result_hash.has_key?(:time_complete) || !result_hash.has_key?(:resource_id)
        raise "Invalid return type for method implementing execute_action interface.  Must return type Hash with the following keys - :success_level, :detail_message, :time_complete, :resource_id"
      end

      self.success_level = result_hash[:success_level]
      self.detail_message = result_hash[:detail_message]
      self.completed_on = result_hash[:time_complete]
      self.resource_id = result_hash[:resource_id]
    rescue
      self.completed_on = Time.now
      self.detail_message.add_context("QueueEntry: #{self.id}", $!)
      self.success_level = 'failure'

      self.internal_error_notification($!, "ActiveRecord::QueueEntry: ")
    end
    Time.zone = tz

    # insure that we're notified if there's an error when running system tasks
    begin
      self.destroy

      log_action
      generate_recurring_entry if self.recurring?
      notify_action_complete
    rescue
      self.internal_error_notification($!, "ActiveRecord::QueueEntry:SystemTasks #{$!}")
      raise # reraise the current exception, just in case
    end

    result_hash
  end
  #create new queue entry
  def generate_recurring_entry
    QueueEntry.create!(self.attributes.merge({ :started_on => nil, 
                                               :queue_job_server_id => nil, 
                                               :scheduled_for => scheduled_for ? scheduled_for + recurring_interval.to_i : Time.now + recurring_interval.to_i }))
  end
  #Returns true if this queue entry is a recurring entry (it has a
  #recurring_interval value that is greater than 0)
  def recurring?
    0 < self.recurring_interval.to_i
  end

  # Release all jobs for this server to be started by any server.  Should be
  # called only after reboot of this bdrb server.
  def QueueEntry.reset_started_jobs_for_current_server
    QueueEntry.transaction do
      QueueEntry.find_started_jobs_for_server(QueueEntry.server_id).each do |queue_entry|
        queue_entry.update_attributes(:started_on => nil, :queue_job_server_id => nil) 
      end
    end
  end

  def QueueEntry.server_id
    #TODO -- server_id needs to be a uniquely id across web-app servers that run these rake tasks
    "1"
  end

  def QueueEntry.run_job(id)
    queue_entry = nil

    QueueEntry.transaction do
      begin
        queue_entry = QueueEntry.find(id)
        queue_entry.update_attributes!({:started_on => Time.now, 
                                        :queue_job_server_id => server_id }) 
      rescue
        logger.info "#{Time.now} No such job."
      end
    end 

    queue_entry.execute_action if queue_entry
  end

  def QueueEntry.run_next_job
    queue_entry = QueueEntry.get_next_job_from_queue(QueueEntry.server_id)
    unless queue_entry.nil?
      if @child = fork
        Process.wait
        # establish a new db connection for the parent process for all tables so
        # that db connections for parent isn't lost when child process dies
        @@connection_handler = ActiveRecord::ConnectionAdapters::ConnectionHandler.new
        ActiveRecord::Base.establish_connection
      else
        begin 
          queue_entry.update_attribute(:worker_pid, Process.pid)
          queue_entry.execute_action
        rescue
          self.internal_error_notification($!, "ActiveRecord::QueueEntry: ")
        ensure 
          exit!
        end
      end
    else
      logger.info "#{Time.now} No job available for worker."
    end
  end

  def set_queued_on_time
    self.queued_on = Time.now
  end
  def detail_message
    @detail_message ||= DetailMessage.new
  end
  def detail_message=(dm)
    @detail_message = dm
  end
  def log_action
    #modify the attr_hash for LogEntry, all other attrs are the same
    attr_hash = self.attributes.reject {|k,v| ['lock_version', 'scheduled_for', 'recurring_interval', 'id'].include?(k)}

    attr_hash[:success_level] = self.success_level
    detail_message.log_entry = LogEntry.create!(attr_hash)
    detail_message.save!
  end

  # action_klass must be a descendant of ActiveRecord::Base
  # returns a string that represents the klass; use base class name that
  # inherits from AR::Base
  def klass_name_for_notifier
    klass = self.action_klass.constantize
    raise ArgumentError.new("Invalid action_klass '#{action_klass}' for the QueueEntry class.") unless klass.ancestors.include? ActiveRecord::Base

    while klass.superclass != ActiveRecord::Base
      klass = klass.superclass 
    end
    klass.to_s
  end

  # dynamically calls the appropriate Notifier method based on the queue_entry action_klass and action_method
  def notify_action_complete
    return if self.user.blank? or self.mute_email

    notifier_method_name = "queue_completion_#{self.klass_name_for_notifier}_#{self.action_method}".underscore
    if Notifier.instance_methods.include?(notifier_method_name)
      Notifier.send("deliver_#{notifier_method_name}", self)
    else
      Notifier.deliver_queue_completion_default(self)
    end
  end
  
  #Used for testing purposes
  def to_s
    out = []
    out << "id: #{self.id}"
    out << "user: #{self.user.full_name unless self.user.blank?} (#{self.user_id})"
    out << "account: #{self.account.name unless self.account.blank?} (#{self.account_id})"
    out << "action_id: #{self.account_id}"
    out << "action_klass: #{self.action_klass}"
    out << "action_method: #{self.action_method}"
    out << "action_args: #{self.action_args}"
    out << "category: #{self.category}"
    out << "description: #{self.description}"
    out << "queued_on: #{self.queued_on}"
    out << "started_on: #{self.started_on}"
    out << "completed_on: #{self.completed_on}"
    out << "scheduled_for: #{self.scheduled_for}"
    out << "recurring_interval: #{self.recurring_interval}"
    out << "queue_job_server_id: #{self.queue_job_server_id}"
    out.join("\n")
  end
  def QueueEntry.queue(account_id, user_id, scheduled_for = Time.now, recurring_interval = nil)
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
    raise "Sample test exception - Oh no!"
  end
end
