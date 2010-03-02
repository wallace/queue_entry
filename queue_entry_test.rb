require File.dirname(__FILE__) + '/../test_helper'

class QueueEntryTest < ActiveRecord::TestCase
  def setup
    QueueEntry.class_eval do
      QueueEntry::QUEUEABLE_METHODS[:Integration] << %[fake_instance_method]
      QueueEntry::QUEUEABLE_METHODS[:QueueEntry] = %[test_class_method]
    end
    IntegrationUser.class_eval do
      def fake_instance_method
        { :success_level => 'success', :detail_message => DetailMessage.create!(:account_id => 1, :log_entry_id => 1), :time_complete => nil, :resource_id => nil }
      end
    end

    @user_int = IntegrationUser.new(:account_id => 1)
    @user_int.save(false)
  end

  #no worker ids or backgroundrb ids are defined
  def test_find_next_job_for_action_worker_on_restart
    q = Factory.create(:queue_entry)
    assert_equal q, QueueEntry.find_next_job_for_action_worker
  end

  def test_find_next_job_for_action_worker_on_restart_for_server_id_of_two
    t = Time.now
    q  = Factory.create(:queue_entry, :scheduled_for => t,         :started_on => t, :queue_job_server_id => 2)
    q1 = Factory.create(:queue_entry, :scheduled_for => t - 1.day, :started_on => t, :queue_job_server_id => 2)
    q2 = Factory.create(:queue_entry, :scheduled_for => t - 1.day, :started_on => t, :queue_job_server_id => 1)
    assert_equal [q, q1].sort { |x,y| x.id <=> y.id }, QueueEntry.find_started_jobs_for_server(2).sort { |x,y| x.id <=> y.id }
  end

  def test_should_find_next_job_for_action_worker_with_assigned_and_unassigned_jobs_in_queue
    Factory.create(:queue_entry, :scheduled_for => Time.now, :queue_job_server_id => 1)
    q = Factory.create(:queue_entry, :scheduled_for => Time.now - 2.days)

    assert_equal q, QueueEntry.find_next_job_for_action_worker
  end

  def test_find_next_job_for_action_worker_with_only_assigned_jobs_in_queue
    #jobs are available if started_on is not null
    q = Factory.create(:queue_entry, :started_on => Time.now, :queue_job_server_id => 2)
    assert_nil QueueEntry.find_next_job_for_action_worker
  end

  def test_find_next_job_for_action_worker_with_no_jobs_in_queue
    QueueEntry.delete_all
    assert_nil QueueEntry.find_next_job_for_action_worker
  end

  def test_find_next_job_for_action_worker_with_only_jobs_in_the_future_in_the_queue
    q = Factory.create(:queue_entry, :scheduled_for => Time.now + 1.day)
    assert_nil QueueEntry.find_next_job_for_action_worker
  end

  def test_should_find_next_unassigned_job_for_first_account_with_no_other_running_jobs_with_multiple_started_jobs_in_the_queue
    a1 = Factory.create(:account, :subdomain => 'bar')
    a2 = Factory.create(:account, :subdomain => 'foobar')
    a3 = Factory.create(:account, :subdomain => 'foo')
         Factory.create(:queue_entry, :scheduled_for => Time.now - 10.minutes, :started_on => Time.now, :account => a1)
         Factory.create(:queue_entry, :scheduled_for => Time.now -  5.minutes,                          :account => a1)
         Factory.create(:queue_entry, :scheduled_for => Time.now -  4.minutes, :started_on => Time.now, :account => a2)
         Factory.create(:queue_entry, :scheduled_for => Time.now -  3.minutes,                          :account => a2)
    @q = Factory.create(:queue_entry, :scheduled_for => Time.now -  1.minutes,                          :account => a3)
    assert_equal @q, QueueEntry.find_next_job_for_action_worker
  end

  def test_recurring_method
    assert_equal true,  QueueEntry.new({:recurring_interval => 1}).recurring?
    assert_equal false, QueueEntry.new({:recurring_interval => 0}).recurring?
    assert_equal false, QueueEntry.new.recurring?
  end

  def test_execute_action
    q = Factory.create(:queue_entry, :action_klass => "IntegrationUser", 
                                   :action_id => @user_int.id, 
                                   :action_method => "fake_instance_method", 
                                   :scheduled_for => Time.now - 3.days)

    q1 = QueueEntry.find_next_job_for_action_worker
    sleep(1)
    result_hash = q1.execute_action

    assert_nil(result_hash[:time_complete])
    assert_equal(DetailMessage, result_hash[:detail_message].class)
    assert_nil(result_hash[:resource_id])
  end

  def test_log_action
    orig_q = Factory.create(:queue_entry, :action_klass => "Test klass", :scheduled_for => Time.now - 3.days)
    q = QueueEntry.find_next_job_for_action_worker
    sleep(1)

    q.send(:log_action)
    assert_equal 1, DetailMessage.find(:all).length, "No detail message created after log action"
  end

  def test_generate_recurring_entry_with_queue_entry_recurring
    q1 = Factory.create(:queue_entry, :action_klass => "Test klass",
                                    :queue_job_server_id => "1", 
                                    :scheduled_for => Time.now - 3.days, 
                                    :recurring_interval => 23)

    amt = QueueEntry.find(:all).length

    q = QueueEntry.find_next_job_for_action_worker
    sleep(1)

    q.generate_recurring_entry

    assert_equal amt + 1, QueueEntry.find(:all).length, "Queue Entry should not be deleted and another recurring one should be there as well"
    assert_equal q1.scheduled_for + q1.recurring_interval, QueueEntry.find(:all).last.scheduled_for
  end

  def test_like_im_a_action_worker
    q1 = Factory.create(:queue_entry, :action_klass => "IntegrationUser",
                                    :action_id => @user_int.id, 
                                    :action_method => "fake_instance_method", 
                                    :scheduled_for => Time.now - 3.days)

    queue_entry = QueueEntry.get_next_job_from_queue("1")
    amt = QueueEntry.find(:all).length
    queue_entry.execute_action

    assert_equal 1, LogEntry.find(:all).length, "No log entry created after log action"
    assert_equal 1, DetailMessage.find(:all).length, "No detail message created after log action"
    assert_equal amt - 1, QueueEntry.find(:all).length, "Queue Entry record not deleted"
  end

  def test_like_im_a_action_worker_with_a_recurring_queued_job
    q1 = Factory.create(:queue_entry, :action_klass => "IntegrationUser", 
                                    :action_id => @user_int.id, 
                                    :action_method => "fake_instance_method", 
                                    :scheduled_for => Time.now - 3.days, 
                                    :recurring_interval => 1)

    queue_entry = QueueEntry.get_next_job_from_queue("1")
    amt = QueueEntry.find(:all).length
    queue_entry.execute_action

    assert_equal 1, LogEntry.find(:all).length, "No log entry created after log action"
    assert_equal 1, DetailMessage.find(:all).length, "No detail message created after log action"
    assert_equal amt, QueueEntry.find(:all).length, "Queue Entry record not deleted"

    sleep(1)

    queue_entry = QueueEntry.get_next_job_from_queue("1")
    queue_entry.execute_action

    assert_equal 2, LogEntry.find(:all).length, "No log entry created after log action"
    assert_equal 2, DetailMessage.find(:all).length, "No detail message created after log action"
    assert_equal amt, QueueEntry.find(:all).length, "Queue Entry record not deleted"
  end

  def test_action_worker_with_long_running_previous_dependent_task
    #simulate a long running task that is not complete yet
    q = Factory.create(:queue_entry, :action_klass        => "IntegrationUser",
                                   :action_id           => @user_int.id, 
                                   :account_id          => 1, 
                                   :queue_job_server_id => 1, 
                                   :action_method       => "import",
                                   :scheduled_for       => Time.now - 3.days,
                                   :started_on          => Time.now - 1.day)

    #create a second task for the same account that is fired up by cron job before first task completes
    q1 = Factory.create(:queue_entry, :action_klass        => "IntegrationUser",
                                    :action_id           => @user_int.id, 
                                    :account_id          => 1, 
                                    :queue_job_server_id => 1, 
                                    :action_method       => "import",
                                    :scheduled_for       => Time.now - 3.days)

    #insure that second job does not show up as available
    assert_nil (QueueEntry.get_next_job_from_queue("1"))
  end 

  def test_execute_action_with_class_method_and_action_id_nil
    q = Factory.create(:queue_entry, :action_id => nil, 
                                   :action_klass => "QueueEntry", 
                                   :action_method => "test_class_method", 
                                   :account_id => 1)

    class << QueueEntry
      def test_class_method
        { :success_level => 'success', :detail_message => DetailMessage.create!(:account_id => 1, :log_entry_id => 1), :time_complete => nil, :resource_id => nil }
      end
    end

    result_hash = q.execute_action

    assert_nil(result_hash[:time_complete])
    assert_equal(DetailMessage, result_hash[:detail_message].class)
    assert_nil(result_hash[:resource_id])
  end

  def test_that_resource_id_is_in_log_entry_after_successful_execute_action_run_that_generates_resource_id
    q1 = Factory.create(:queue_entry, :action_id => nil, 
                                    :action_klass => "QueueEntry", 
                                    :action_method => "test_class_method", 
                                    :account_id => 1)

    @@rd = ResourceDocument.create!(:account_id => 1) #create a resource of id of one so that notify_action_complete can access the generated_url attribute of a real existing record
    class << QueueEntry
      def test_class_method
        { :success_level => 'success', :detail_message => DetailMessage.create!(:account_id => 1, :log_entry_id => 1), :time_complete => nil, :resource_id => @@rd.id }
      end
    end

    assert_equal([], LogEntry.find(:all))
    result_hash = q1.execute_action

    assert_nil(result_hash[:time_complete])
    assert_equal(DetailMessage, result_hash[:detail_message].class)
    assert_equal(@@rd.id, result_hash[:resource_id])
    assert_equal(@@rd.id, LogEntry.find(:first).resource_id)
  end

  def test_should_find_no_jobs_older_than_1_year
    assert_equal([], QueueEntry.find_started_jobs_older_than(1.year.seconds))
  end

  def test_should_find_one_job_older_than_30_minutes
    q = Factory.create(:queue_entry, :action_id => nil, 
                                   :action_klass => "QueueEntry", 
                                   :action_method => "test_class_method", 
                                   :account_id => 1, 
                                   :started_on => Time.now - 30.minutes)

    assert_equal([q], QueueEntry.find_started_jobs_older_than(30.minutes - 1))
  end

  #see ticket #148 in track and revision 1399
  def test_should_not_generate_an_error
    User.delete_all
    Course.delete_all
    Enrollment.delete_all
    QueueEntry.delete_all
    c = CourseSelfStudy.create!(:account_id => 1, :name => 'test')
    limit = 1
    1.upto(limit) do |i|
      u = User.create!(:account_id => 1, :first_name => i.to_s, :last_name => i.to_s, :login => i.to_s, :password => i.to_s)
      Enrollment.create!(:account_id => 1, :course => c, :user => u)
    end
    e_ids = Enrollment.find(:all).collect { |e| e.id }
    t = Time.now

    q = Factory.create(:queue_entry, :action_klass => c.class.to_s,
                   :action_id => c.id.to_s, 
                   :action_method => "update_enrollments", 
                   :action_args => { :enrollment_ids => e_ids, :enrollment_params => { :due_on => t }},
                   :category => "bulk enrollment update",
                   :scheduled_for => t)

    assert_nothing_raised { q.execute_action }
  end
end
