/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cascading.operation.GroupAssertion;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.log4j.Logger;

/**
 *
 */
public abstract class AssertGroupBase extends BaseAssertion implements GroupAssertion
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( AssertGroupBase.class );

  /** Field COUNT */
  private static final String COUNT = "count";
  /** Field FIELDS */
  private static final String FIELDS = "fields";
  /** Field GROUP */
  private static final String GROUP = "group";

  /** Field patternString */
  protected String patternString;

  /** Field pattern */
  private transient Pattern pattern;

  /** Field size */
  protected long size;

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

  /** @see cascading.operation.Aggregator#start(java.util.Map, cascading.tuple.TupleEntry) */
  @SuppressWarnings("unchecked")
  public void start( Map context, TupleEntry groupEntry )
    {
    // didn't match, so skip
    if( !matchWholeTuple( groupEntry.getTuple() ) )
      return;

    context.put( COUNT, 0L );
    context.put( FIELDS, groupEntry.getFields().print() );
    context.put( GROUP, groupEntry.getTuple().print() );
    }

  /** @see cascading.operation.Aggregator#aggregate(java.util.Map, cascading.tuple.TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    Long groupSize = (Long) context.get( COUNT );

    // didn't match, so skip
    if( groupSize != null )
      context.put( COUNT, groupSize + 1L );
    }

  public void doAssert( Map context )
    {
    Long groupSize = (Long) context.get( COUNT );

    if( groupSize == null ) // didn't match, so skip
      return;

    if( assertFails( groupSize ) )
      {
      if( patternString == null )
        fail( groupSize, size, context.get( FIELDS ), context.get( GROUP ) );
      else
        fail( patternString, groupSize, size, context.get( FIELDS ), context.get( GROUP ) );
      }
    }

  protected abstract boolean assertFails( Long groupSize );
  }
