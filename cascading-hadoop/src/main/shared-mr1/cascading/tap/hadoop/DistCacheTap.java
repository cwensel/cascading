/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.io.IOException;
import java.net.URI;

import cascading.flow.FlowProcess;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

/**
 * Class DistCacheTap is a Tap decorator for Hfs and can be used to move a file to the
 * {@link org.apache.hadoop.filecache.DistributedCache} on read when accessed cluster side.
 * <p/>
 * This is useful for {@link cascading.pipe.HashJoin}s.
 * <p/>
 * The distributed cache is only used when the Tap is used as a source. If the DistCacheTap is used as a sink,
 * it will delegate to the provided parent instance and not use the DistributedCache.
 */
public class DistCacheTap extends BaseDistCacheTap
  {
  /**
   * Constructs a new DistCacheTap instance with the given Hfs.
   *
   * @param parent an Hfs or GlobHfs instance representing a small file.
   */
  public DistCacheTap( Hfs parent )
    {
    super( parent );
    }

  @Override
  protected Path[] getLocalCacheFiles( FlowProcess<? extends Configuration> flowProcess ) throws IOException
    {
    return DistributedCache.getLocalCacheFiles( flowProcess.getConfigCopy() );
    }

  @Override
  protected void addLocalCacheFiles( Configuration conf, URI uri )
    {
    DistributedCache.addCacheFile( uri, conf );
    }
  }
