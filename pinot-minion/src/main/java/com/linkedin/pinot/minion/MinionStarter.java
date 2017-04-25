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
package com.linkedin.pinot.minion;

import com.linkedin.pinot.minion.taskfactory.TaskFactoryRegistry;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.task.TaskStateModelFactory;


/**
 * The class <code>MinionStarter</code> provides methods to start and stop the Pinot Minion.
 * <p>Pinot Minion will automatically join the given Helix cluster as a participant.
 */
public class MinionStarter {
  public static final String MINION_PREFIX = "Minion_";

  private final String _zkAddress;
  private final String _helixClusterName;
  private final String _minionId;

  private HelixManager _helixManager;

  public MinionStarter(String zkAddress, String helixClusterName, String minionName) {
    _zkAddress = zkAddress;
    _helixClusterName = helixClusterName;
    _minionId = MINION_PREFIX + minionName;
  }

  /**
   * Start the Pinot Minion.
   */
  public void start()
      throws Exception {
    _helixManager = new ZKHelixManager(_helixClusterName, _minionId, InstanceType.PARTICIPANT, _zkAddress);
    _helixManager.getStateMachineEngine()
        .registerStateModelFactory("Task",
            new TaskStateModelFactory(_helixManager, TaskFactoryRegistry.getTaskFactoryRegistry()));
    _helixManager.connect();
  }

  /**
   * Stop the Pinot Minion.
   */
  public void stop() {
    _helixManager.disconnect();
  }
}
