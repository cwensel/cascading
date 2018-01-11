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

package cascading.flow.local.hadoop;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tap.local.hadoop.LocalHfs;
import org.apache.hadoop.mapred.JobConf;

/**
 * Class LocalHadoopFlowProcess is for use with the {@link LocalHfs} implementation.
 */
public class LocalHadoopFlowProcess extends FlowProcessWrapper<JobConf>
  {
  FlowProcess<? extends Properties> local;
  JobConf conf;

  public LocalHadoopFlowProcess( FlowProcess<? extends Properties> delegate )
    {
    super( delegate );

    local = delegate;
    }

  @Override
  public JobConf getConfig()
    {
    if( conf == null )
      conf = HadoopUtil.createJobConf( local.getConfig() );

    return conf;
    }

  @Override
  public JobConf getConfigCopy()
    {
    return new JobConf( getConfig() );
    }

  @Override
  public Object getProperty( String key )
    {
    return conf.get( key );
    }

  @Override
  public Collection<String> getPropertyKeys()
    {
    Set<String> keys = new HashSet<String>();

    for( Map.Entry<String, String> entry : conf )
      keys.add( entry.getKey() );

    return Collections.unmodifiableSet( keys );
    }
  }
