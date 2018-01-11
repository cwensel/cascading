/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.tap.local.hadoop;

import java.beans.ConstructorProperties;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.local.hadoop.LocalHadoopFlowProcess;
import cascading.tap.FileAdaptorTap;
import cascading.tap.Tap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Class LocalHfs adapts the Hadoop {@link cascading.tap.hadoop.Hfs} {@link Tap} for use with the
 * {@link cascading.flow.local.LocalFlowConnector} and {@link cascading.flow.local.planner.LocalPlanner}.
 * <p>
 * That is, "local mode" Cascading application can access data on an Apache Hadoop HDFS cluster if the proper
 * Hadoop properties on are set on the LocalFlowConnector instance.
 */
public class LocalHfs extends FileAdaptorTap<Properties, InputStream, OutputStream, Configuration, RecordReader, OutputCollector>
  {
  @ConstructorProperties("original")
  public LocalHfs( Tap<Configuration, RecordReader, OutputCollector> original )
    {
    super( original, LocalHadoopFlowProcess::new, HadoopUtil::createJobConf );
    }
  }
