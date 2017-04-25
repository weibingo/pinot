/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.minion;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotHelixTaskResourceManager</code> manages all the task resources in Pinot cluster.
 */
public class PinotHelixTaskResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixTaskResourceManager.class);

  public static final String JOB_QUEUE_PREFIX = "JobQueue_";
  public static final String TASK_PREFIX = "Task_";

  private final TaskDriver _taskDriver;

  public PinotHelixTaskResourceManager(@Nonnull TaskDriver taskDriver) {
    _taskDriver = taskDriver;
  }

  public void createJobQueue(String taskType) {
    String jobQueueName = getJobQueueName(taskType);
    LOGGER.info("Creating JobQueue: {}", jobQueueName);

    // Set full parallelism
    JobQueue jobQueue = new JobQueue.Builder(jobQueueName).setWorkflowConfig(
        new WorkflowConfig.Builder().setParallelJobs(Integer.MAX_VALUE).build()).build();
    _taskDriver.createQueue(jobQueue);
  }

  public void stopJobQueue(String taskType) {
    String jobQueueName = getJobQueueName(taskType);
    LOGGER.info("Stopping JobQueue: {}", jobQueueName);
    _taskDriver.stop(jobQueueName);
  }

  public void resumeJobQueue(String taskType) {
    String jobQueueName = getJobQueueName(taskType);
    LOGGER.info("Resuming JobQueue: {}", jobQueueName);
    _taskDriver.resume(jobQueueName);
  }

  public void deleteJobQueue(String taskType) {
    String jobQueueName = getJobQueueName(taskType);
    LOGGER.info("Deleting JobQueue: {}", jobQueueName);
    _taskDriver.delete(jobQueueName);
  }

  public void submitTask(String taskType, PinotTaskConfig pinotTaskConfig) {
    String taskName = TASK_PREFIX + taskType + '_' + System.nanoTime();
    LOGGER.info("Submitting task: {} of type: {} with config: {}", taskName, taskType, pinotTaskConfig);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().addTaskConfigs(Collections.singletonList(pinotTaskConfig.toHelixTaskConfig())).setIgnoreDependentJobFailure(true);
    _taskDriver.enqueueJob(getJobQueueName(taskType), taskName, jobBuilder);
  }

  public boolean deleteTask(String taskType, String taskName) {
    String jobQueueName = getJobQueueName(taskType);
    LOGGER.info("Deleting task: {} of type: {}", taskName, taskType);
    try {
//      _taskDriver.stop(jobQueueName);
//      _taskDriver.pollForJobState(jobQueueName, taskName, TaskState.NOT_STARTED, TaskState.STOPPED, TaskState.FAILED,
//          TaskState.COMPLETED, TaskState.ABORTED);

      // To delete job, need to remove job type prefix
      _taskDriver.deleteJob(jobQueueName, taskName.substring(jobQueueName.length() + 1));
//      Thread.sleep(1_000L);
//
//      _taskDriver.resume(jobQueueName);
      return true;
    } catch (Exception e) {
      LOGGER.error("Caught exception while deleting task: {} of type: {}", taskName, taskType, e);
      return false;
    }
  }

  public Map<String, TaskState> getTaskStates(String taskType) {
    return _taskDriver.getWorkflowContext(getJobQueueName(taskType)).getJobStates();
  }

  public static String getJobQueueName(String taskType) {
    return JOB_QUEUE_PREFIX + taskType;
  }
}
