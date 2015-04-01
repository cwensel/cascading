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

package cascading.tap.hadoop.io;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.tap.TapException;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.tuple.util.TupleViews;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class CombineInputPartitionTupleEntryIterator implements Iterator<Tuple>
  {
  private final TupleEntrySchemeIterator childIterator;
  private final FlowProcess<JobConf> flowProcess;
  private final TupleEntry partitionEntry;
  private final Fields sourceFields;
  private final Partition partition;
  private final String parentIdentifier;

  private Tuple base;
  private Tuple view;
  private String currentFile;

  public CombineInputPartitionTupleEntryIterator( FlowProcess<JobConf> flowProcess, Fields sourceFields,
                                                  Partition partition, String parentIdentifier, TupleEntrySchemeIterator childIterator )
    {
    this.flowProcess = flowProcess;
    this.partition = partition;
    this.parentIdentifier = parentIdentifier;
    this.childIterator = childIterator;
    this.sourceFields = sourceFields;
    this.partitionEntry = new TupleEntry( partition.getPartitionFields(), Tuple.size( partition.getPartitionFields().size() ) );
    }

  @Override
  public boolean hasNext()
    {
    return childIterator.hasNext();
    }

  @Override
  public Tuple next()
    {
    String currentFile = getCurrentFile();
    if( this.currentFile == null || !this.currentFile.equals( currentFile ) )
      {
      this.currentFile = currentFile;

      try
        {
        String childIdentifier = new Path( currentFile ).getParent().toString(); // drop part-xxxx
        partition.toTuple( childIdentifier.substring( parentIdentifier.length() + 1 ), partitionEntry );
        }
      catch( Exception exception )
        {
        throw new TapException( "unable to parse partition given parent: " + parentIdentifier + " and child: " + currentFile, exception );
        }

      base = TupleViews.createOverride( sourceFields, partitionEntry.getFields() );

      TupleViews.reset( base, Tuple.size( sourceFields.size() ), partitionEntry.getTuple() );

      view = TupleViews.createOverride( sourceFields, childIterator.getFields() );
      }

    Tuple tuple = childIterator.next().getTuple();
    TupleViews.reset( view, base, tuple );

    return view;
    }

  private String getCurrentFile()
    {
    String result = flowProcess.getStringProperty( "mapreduce.map.input.file" );

    if( result == null )
      result = flowProcess.getStringProperty( "map.input.file" );

    return result;
    }

  @Override
  public void remove()
    {
    childIterator.remove();
    }
  }
