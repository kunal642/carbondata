/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.api.CarbonOutputCommitter;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

public class MapredCarbonOutputCommitter extends OutputCommitter {

  CarbonOutputCommitter carbonOutputCommitter;

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    Random random = new Random();
    JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
    TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
    TaskAttemptID attemptID = new TaskAttemptID(task, random.nextInt());
    org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl context =
        new TaskAttemptContextImpl(jobContext.getJobConf(), attemptID);
    CarbonLoadModel carbonLoadModel =
        HiveCarbonUtil.getCarbonLoadModel(jobContext.getConfiguration());
    CarbonTableOutputFormat.setLoadModel(jobContext.getConfiguration(), carbonLoadModel);
    carbonOutputCommitter =
        new CarbonOutputCommitter(new Path(jobContext.getJobConf().get("location")), context);
    carbonOutputCommitter.setupJob(jobContext);
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }

  @Override
  public void abortJob(JobContext jobContext, int status) throws IOException {
    if (carbonOutputCommitter != null) {
      carbonOutputCommitter.abortJob(jobContext, JobStatus.State.FAILED);
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    Configuration configuration =
        (Configuration) ThreadLocalSessionInfo.getCarbonSessionInfo().getNonSerializableExtraInfo()
            .get("carbonConf");
    CarbonLoadModel carbonLoadModel = MapredCarbonOutputFormat.getLoadModel(configuration);
    ThreadLocalSessionInfo.unsetAll();
    SegmentFileStore.writeSegmentFile(carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable(),
        carbonLoadModel.getSegmentId(), String.valueOf(carbonLoadModel.getFactTimeStamp()));
    SegmentFileStore.mergeIndexAndWriteSegmentFile(carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable(),
        carbonLoadModel.getSegmentId(), String.valueOf(carbonLoadModel.getFactTimeStamp()));
    carbonOutputCommitter.commitJob(jobContext);
  }
}
