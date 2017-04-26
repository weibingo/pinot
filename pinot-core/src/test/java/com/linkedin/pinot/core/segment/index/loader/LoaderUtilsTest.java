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
package com.linkedin.pinot.core.segment.index.loader;

import com.linkedin.pinot.common.utils.CommonConstants;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;


public class LoaderUtilsTest {

  @Test
  public void testReloadFailureRecovery()
      throws IOException {
    File tempDir = Files.createTempDirectory(LoaderUtilsTest.class.getName()).toFile();
    FileUtils.deleteDirectory(tempDir);
    Assert.assertTrue(tempDir.mkdirs());

    String segmentName = "dummySegment";
    String indexFileName = "dummyIndex";
    File indexDir = new File(tempDir, segmentName);
    File segmentBackupDir = new File(tempDir, segmentName + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    File segmentTempDir = new File(tempDir, segmentName + CommonConstants.Segment.SEGMENT_TEMP_DIR_SUFFIX);

    // Only index directory exists (normal case)
    Assert.assertTrue(indexDir.mkdir());
    FileUtils.touch(new File(indexDir, indexFileName));
    LoaderUtils.reloadFailureRecovery(indexDir);
    Assert.assertTrue(indexDir.exists());
    Assert.assertTrue(new File(indexDir, indexFileName).exists());
    Assert.assertFalse(segmentBackupDir.exists());
    Assert.assertFalse(segmentTempDir.exists());
    FileUtils.deleteDirectory(indexDir);

    // Only segment backup directory exists (failed after the first renaming)
    Assert.assertTrue(segmentBackupDir.mkdir());
    FileUtils.touch(new File(segmentBackupDir, indexFileName));
    LoaderUtils.reloadFailureRecovery(indexDir);
    Assert.assertTrue(indexDir.exists());
    Assert.assertTrue(new File(indexDir, indexFileName).exists());
    Assert.assertFalse(segmentBackupDir.exists());
    Assert.assertFalse(segmentTempDir.exists());
    FileUtils.deleteDirectory(indexDir);

    // Index directory and segment backup directory exist (failed before second renaming)
    Assert.assertTrue(indexDir.mkdir());
    Assert.assertTrue(segmentBackupDir.mkdir());
    FileUtils.touch(new File(segmentBackupDir, indexFileName));
    LoaderUtils.reloadFailureRecovery(indexDir);
    Assert.assertTrue(indexDir.exists());
    Assert.assertTrue(new File(indexDir, indexFileName).exists());
    Assert.assertFalse(segmentBackupDir.exists());
    Assert.assertFalse(segmentTempDir.exists());
    FileUtils.deleteDirectory(indexDir);

    // Index directory and segment temporary directory exist (failed after second renaming)
    Assert.assertTrue(indexDir.mkdir());
    FileUtils.touch(new File(indexDir, indexFileName));
    Assert.assertTrue(segmentTempDir.mkdir());
    LoaderUtils.reloadFailureRecovery(indexDir);
    Assert.assertTrue(indexDir.exists());
    Assert.assertTrue(new File(indexDir, indexFileName).exists());
    Assert.assertFalse(segmentBackupDir.exists());
    Assert.assertFalse(segmentTempDir.exists());
    FileUtils.deleteDirectory(indexDir);

    FileUtils.deleteDirectory(tempDir);
  }
}
