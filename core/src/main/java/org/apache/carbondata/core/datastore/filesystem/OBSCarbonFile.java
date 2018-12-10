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
package org.apache.carbondata.core.datastore.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class OBSCarbonFile extends S3CarbonFile {

  public OBSCarbonFile(String filePath) {
    super(filePath);
  }

  public OBSCarbonFile(String filePath, Configuration hadoopConf) {
    super(filePath, hadoopConf);
  }

  public OBSCarbonFile(Path path, Configuration hadoopConf) {
    super(path, hadoopConf);
  }

  @Override
  protected CarbonFile[] getFiles(FileStatus[] listStatus) {
    if (listStatus == null) {
      return new CarbonFile[0];
    }
    CarbonFile[] files = new CarbonFile[listStatus.length];
    for (int i = 0; i < files.length; i++) {
      files[i] = new OBSCarbonFile(listStatus[i].getPath(), hadoopConf);
    }
    return files;
  }

  @Override
  public CarbonFile getParentFile() {
    Path parent = fileStatus.getPath().getParent();
    return null == parent ? null : new OBSCarbonFile(parent, hadoopConf);
  }
}
