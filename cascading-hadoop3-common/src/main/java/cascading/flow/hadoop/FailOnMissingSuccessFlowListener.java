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

package cascading.flow.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowException;
import cascading.flow.FlowListener;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.util.Util;
import org.apache.hadoop.fs.Path;

/**
 * Class FailOnMissingSuccessFlowListener is a {@link FlowListener} that tests that all sources to a {@link Flow}
 * have a {@code _SUCCESS} file before allowing the Flow to execute.
 * <p>
 * If any source Tap is a directory, existence of {@code _SUCCESS} is made, if the source is a file, the existence test
 * is skipped.
 * <p>
 * This listener will unwind any {@link PartitionTap} or {@link MultiSourceTap} instances looking for {@link Hfs} instances
 * to verify. If any Tap is found that is not a PartitionTap, MultiSourceTap, or Hfs type, an error will be thrown.
 */
public class FailOnMissingSuccessFlowListener implements FlowListener
  {
  @Override
  public void onStarting( Flow flow )
    {
    Map<String, Tap> sources = flow.getSources();

    for( Map.Entry<String, Tap> entry : sources.entrySet() )
      {
      String key = entry.getKey();
      Tap value = entry.getValue();
      Set<Hfs> taps = Util.createIdentitySet();

      accumulate( taps, value );

      for( Hfs tap : taps )
        {
        if( !testExists( flow, tap ) )
          throw new FlowException( "cannot start flow: " + flow.getName() + ", _SUCCESS file missing in tap: '" + key + "', at: " + value.getIdentifier() );
        }
      }
    }

  public boolean testExists( Flow flow, Hfs tap )
    {
    try
      {
      // don't test for _SUCCESS if the tap is a file, only if a directory
      if( !tap.isDirectory( flow.getFlowProcess() ) )
        return true;

      return new Hfs( new TextLine(), new Path( tap.getPath(), "_SUCCESS" ).toString() ).resourceExists( flow.getFlowProcess() );
      }
    catch( IOException exception )
      {
      throw new FlowException( exception );
      }
    }

  public void accumulate( Set<Hfs> taps, Tap value )
    {
    if( value == null )
      return;

    if( value instanceof Hfs )
      taps.add( (Hfs) value );
    else if( value instanceof PartitionTap )
      taps.add( (Hfs) ( (PartitionTap) value ).getParent() );
    else if( value instanceof MultiSourceTap )
      iterate( taps, (MultiSourceTap) value );
    else
      throw new IllegalArgumentException( "unsupprted Tap type: " + value.getClass().getName() );
    }

  public void iterate( Set<Hfs> taps, MultiSourceTap value )
    {
    Iterator<Tap> childTaps = value.getChildTaps();

    while( childTaps.hasNext() )
      accumulate( taps, childTaps.next() );
    }

  @Override
  public void onStopping( Flow flow )
    {

    }

  @Override
  public void onCompleted( Flow flow )
    {

    }

  @Override
  public boolean onThrowable( Flow flow, Throwable throwable )
    {
    return false;
    }
  }
