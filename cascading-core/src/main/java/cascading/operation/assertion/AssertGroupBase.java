/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.assertion;

import java.beans.ConstructorProperties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cascading.flow.FlowProcess;
import cascading.operation.GroupAssertion;
import cascading.operation.GroupAssertionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class AssertGroupBase extends BaseAssertion<AssertGroupBase.Context> implements GroupAssertion<AssertGroupBase.Context>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( AssertGroupBase.class );

  /** Field patternString */
  protected String patternString;

  /** Field size */
  protected final long size;

  public static class Context
    {
    Pattern pattern;
    Long count;
    String fields;
    String group;

    public Context set( long count, String fields, String group )
      {
      this.count = count;
      this.fields = fields;
      this.group = group;

      return this;
      }

    public Context reset()
      {
      count = null;
      fields = null;
      group = null;

      return this;
      }
    }

  @ConstructorProperties({"message", "size"})
  public AssertGroupBase( String message, long size )
    {
    super( message );
    this.size = size;
    }

  @ConstructorProperties({"message", "patternString", "size"})
  protected AssertGroupBase( String message, String patternString, long size )
    {
    super( message );
    this.patternString = patternString;
    this.size = size;
    }

  public String getPatternString()
    {
    return patternString;
    }

  public long getSize()
    {
    return size;
    }

  private Pattern getPattern()
    {
    Pattern pattern;

    if( patternString == null )
      pattern = Pattern.compile( ".*" );
    else
      pattern = Pattern.compile( patternString );

    return pattern;
    }

  private boolean matchWholeTuple( Tuple input, Pattern pattern )
    {
    if( patternString == null )
      return true;

    Matcher matcher = pattern.matcher( input.toString( "\t", false ) );

    LOG.debug( "pattern: {}, matches: {}", pattern, matcher.matches() );

    return matcher.matches();
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    operationCall.setContext( new Context() );
    operationCall.getContext().pattern = getPattern();
    }

  @Override
  public void start( FlowProcess flowProcess, GroupAssertionCall<Context> assertionCall )
    {
    TupleEntry groupEntry = assertionCall.getGroup();
    Context context = assertionCall.getContext();

    // didn't match, so skip
    if( !matchWholeTuple( groupEntry.getTuple(), context.pattern ) )
      context.reset();
    else
      context.set( 0L, groupEntry.getFields().print(), groupEntry.getTuple().print() );
    }

  @Override
  public void aggregate( FlowProcess flowProcess, GroupAssertionCall<Context> assertionCall )
    {
    Long groupSize = assertionCall.getContext().count;

    // didn't match, so skip
    if( groupSize != null )
      assertionCall.getContext().count += 1L;
    }

  @Override
  public void doAssert( FlowProcess flowProcess, GroupAssertionCall<Context> assertionCall )
    {
    Context context = assertionCall.getContext();
    Long groupSize = context.count;

    if( groupSize == null ) // didn't match, so skip
      return;

    if( assertFails( groupSize ) )
      {
      if( patternString == null )
        fail( groupSize, size, context.fields, context.group );
      else
        fail( patternString, groupSize, size, context.fields, context.group );
      }
    }

  protected abstract boolean assertFails( Long groupSize );

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof AssertGroupBase ) )
      return false;
    if( !super.equals( object ) )
      return false;

    AssertGroupBase that = (AssertGroupBase) object;

    if( size != that.size )
      return false;
    if( patternString != null ? !patternString.equals( that.patternString ) : that.patternString != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( patternString != null ? patternString.hashCode() : 0 );
    result = 31 * result + (int) ( size ^ size >>> 32 );
    return result;
    }
  }
