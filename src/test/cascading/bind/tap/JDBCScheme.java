/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.bind.tap;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** A mock Scheme for testing purposes. */
public class JDBCScheme extends Scheme
  {

  public JDBCScheme( Fields fields )
    {
    super( fields, fields );
    }

  @Override
  public void sourceConfInit( FlowProcess flowProcess, Tap tap, Object conf )
    {
    }

  @Override
  public void sinkConfInit( FlowProcess flowProcess, Tap tap, Object conf )
    {
    }

  @Override
  public boolean source( FlowProcess flowProcess, SourceCall sourceCall ) throws IOException
    {
    return false;
    }

  @Override
  public void sink( FlowProcess flowProcess, SinkCall sinkCall ) throws IOException
    {
    }
  }
