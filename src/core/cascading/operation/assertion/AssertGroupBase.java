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

package cascading.operation.assertion;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cascading.flow.FlowProcess;
import cascading.operation.GroupAssertion;
import cascading.operation.GroupAssertionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.log4j.Logger;

/**
 *
 */
public abstract class AssertGroupBase extends BaseAssertion<AssertGroupBase.Context> implements GroupAssertion<AssertGroupBase.Context>
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( AssertGroupBase.class );

  /** Field patternString */
  protected String patternString;

  /** Field pattern */
  private transient Pattern pattern;

  /** Field size */
  protected long size;

  public static class Context
    {
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

  public AssertGroupBase( String message, long size )
    {
    super( message );
    this.size = size;
    }

  protected AssertGroupBase( String message, String patternString, long size )
    {
    super( message );
    this.patternString = patternString;
    this.size = size;
    }

  private Pattern getPattern()
    {
    if( pattern != null )
      return pattern;

    if( patternString == null )
      pattern = Pattern.compile( ".*" );
    else
      pattern = Pattern.compile( patternString );

    return pattern;
    }

  private boolean matchWholeTuple( Tuple input )
    {
    if( patternString == null )
      return true;

    Matcher matcher = getPattern().matcher( input.toString( "\t" ) );

    if( LOG.isDebugEnabled() )
      LOG.debug( "pattern: " + getPattern() + ", matches: " + matcher.matches() );

    return matcher.matches();
    }

  public void start( FlowProcess flowProcess, GroupAssertionCall<Context> assertionCall )
    {
    if( assertionCall.getContext() == null )
      assertionCall.setContext( new Context() );

    TupleEntry groupEntry = assertionCall.getGroup();
    Context context = assertionCall.getContext();

    // didn't match, so skip
    if( !matchWholeTuple( groupEntry.getTuple() ) )
      context.reset();
    else
      context.set( 0L, groupEntry.getFields().print(), groupEntry.getTuple().print() );
    }

  public void aggregate( FlowProcess flowProcess, GroupAssertionCall<Context> assertionCall )
    {
    Long groupSize = (Long) assertionCall.getContext().count;

    // didn't match, so skip
    if( groupSize != null )
      assertionCall.getContext().count += 1L;
    }

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
  }
