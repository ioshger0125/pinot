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
package com.linkedin.pinot.minion.executor;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.minion.RawIndexConverter;
import com.linkedin.pinot.minion.exception.TaskCancelledException;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConvertToRawIndexTaskExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConvertToRawIndexTaskExecutor.class);

  @Override
  public void executeTask(@Nonnull PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String downloadURL = configs.get(MinionConstants.DOWNLOAD_URL_KEY);
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    LOGGER.info("Executing ConvertToRawIndexTask on table: {}, segment: {} with downloadURL: {}, uploadURL: {}",
        tableName, segmentName, downloadURL, uploadURL);

    File tempDataDir = new File(new File(_minionContext.getDataDir(), MinionConstants.ConvertToRawIndexTask.TASK_TYPE),
        "tmp-" + System.nanoTime());
    Preconditions.checkState(tempDataDir.mkdirs());
    try {
      // Download the tarred segment file
      File tempTarredSegmentFile = new File(tempDataDir, "tarredSegmentFile");
      SegmentFetcherFactory.getSegmentFetcherBasedOnURI(downloadURL)
          .fetchSegmentToLocal(downloadURL, tempTarredSegmentFile);

      // Un-tar the segment file
      File tempSegmentDir = new File(tempDataDir, "segmentDir");
      TarGzCompressionUtils.unTar(tempTarredSegmentFile, tempSegmentDir);
      File[] files = tempSegmentDir.listFiles();
      Preconditions.checkState(files != null && files.length == 1);
      File indexDir = files[0];

      // Convert the segment
      File convertedIndexDir = new File(tempDataDir, "convertedIndexDir");
      new RawIndexConverter(indexDir, convertedIndexDir).convert();

      // Tar the converted segment
      File convertedTarredSegmentFile = new File(
          TarGzCompressionUtils.createTarGzOfDirectory(convertedIndexDir.getPath(),
              new File(tempDataDir, "convertedTarredSegmentFile").getPath()));

      // Check whether the task get cancelled before uploading the segment
      if (_cancelled) {
        throw new TaskCancelledException(
            MinionConstants.ConvertToRawIndexTask.TASK_TYPE + " task on table: " + tableName + ", segment: "
                + segmentName + " has been cancelled");
      }

      // Upload the converted tarred segment file
      FileUploadUtils.sendFile(uploadURL, "convertedTarredSegmentFile", new FileInputStream(convertedTarredSegmentFile),
          convertedTarredSegmentFile.length(), FileUploadUtils.SendFileMethod.POST);
    } catch (TaskCancelledException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      FileUtils.deleteQuietly(tempDataDir);
    }
  }
}
