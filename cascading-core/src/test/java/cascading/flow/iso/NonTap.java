/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.iso;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.NullScheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class NonTap extends Tap
  {
  String identifier = "non-tap-" + System.identityHashCode( this );

  public NonTap( String identifier )
    {
    this();
    this.identifier = identifier;
    }

  public NonTap()
    {
    super( new NullScheme( Fields.UNKNOWN, Fields.ALL ) );
    }

  public NonTap( String identifier, Fields fields )
    {
    this( fields );
    this.identifier = identifier;
    }

  public NonTap( Fields fields )
    {
    super( new NullScheme( fields, fields ) );
    }

  @Override
  public String getIdentifier()
    {
    return identifier;
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess flowProcess, Object object ) throws IOException
    {
    return null;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess flowProcess, Object object ) throws IOException
    {
    return null;
    }

  @Override
  public boolean createResource( Object conf ) throws IOException
    {
    return false;
    }

  @Override
  public boolean deleteResource( Object conf ) throws IOException
    {
    return false;
    }

  @Override
  public boolean resourceExists( Object conf ) throws IOException
    {
    return false;
    }

  @Override
  public long getModifiedTime( Object conf ) throws IOException
    {
    return 0;
    }
  }
