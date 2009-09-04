/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.tap.hadoop;

import java.io.IOException;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Hfs;
import cascading.tap.TapException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class HadoopOutputCollector extends HadoopEntryCollector
  {
  private Hfs sink;
  private TupleEntry outputEntry;

  public HadoopOutputCollector( Hfs sink, HadoopFlowProcess flowContext )
    {
    super( flowContext );
    this.sink = sink;
    this.outputEntry = new TupleEntry( sink.getSinkFields() );
    }

  @Override
  protected void collect( Tuple tuple )
    {
    try
      {
      outputEntry.setTuple( tuple );

      sink.sink( outputEntry, this );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to write to: " + sink.getPath(), exception );
      }
    }
  }
