/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.operation.regex;

import java.beans.ConstructorProperties;
import java.util.regex.Matcher;

import cascading.flow.FlowProcess;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class RegexMatcher is the base class for common regular expression operations.
 * <p>
 * This operation uses {@link java.util.regex.Matcher} internally, specifically the method {@link java.util.regex.Matcher#find()}.
 *
 * @see java.util.regex.Matcher
 * @see java.util.regex.Pattern
 */
public class RegexMatcher extends RegexOperation<Matcher>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( RegexMatcher.class );

  public static final String DEFAULT_DELIM = "\t";

  protected final boolean negateMatch;
  protected final String delimiter;

  @ConstructorProperties({"patternString"})
  protected RegexMatcher( String patternString )
    {
    super( patternString );
    this.negateMatch = false;
    this.delimiter = DEFAULT_DELIM;
    }

  @ConstructorProperties({"patternString", "delimiter"})
  protected RegexMatcher( String patternString, String delimiter )
    {
    super( patternString );
    this.negateMatch = false;
    this.delimiter = delimiter;

    if( this.delimiter == null )
      throw new IllegalArgumentException( "delimiter may not be null" );
    }

  @ConstructorProperties({"patternString", "negateMatch"})
  protected RegexMatcher( String patternString, boolean negateMatch )
    {
    super( patternString );
    this.negateMatch = negateMatch;
    this.delimiter = DEFAULT_DELIM;
    }

  @ConstructorProperties({"patternString", "negateMatch", "delimiter"})
  protected RegexMatcher( String patternString, boolean negateMatch, String delimiter )
    {
    super( patternString );
    this.negateMatch = negateMatch;
    this.delimiter = delimiter;

    if( this.delimiter == null )
      throw new IllegalArgumentException( "delimiter may not be null" );
    }

  @ConstructorProperties({"fieldDeclaration", "patternString"})
  protected RegexMatcher( Fields fieldDeclaration, String patternString )
    {
    super( ANY, fieldDeclaration, patternString );
    this.negateMatch = false;
    this.delimiter = DEFAULT_DELIM;

    verify();
    }

  @ConstructorProperties({"fieldDeclaration", "patternString", "delimiter"})
  protected RegexMatcher( Fields fieldDeclaration, String patternString, String delimiter )
    {
    super( ANY, fieldDeclaration, patternString );
    this.negateMatch = false;
    this.delimiter = delimiter;

    if( this.delimiter == null )
      throw new IllegalArgumentException( "delimiter may not be null" );

    verify();
    }

  @ConstructorProperties({"fieldDeclaration", "patternString", "negateMatch"})
  protected RegexMatcher( Fields fieldDeclaration, String patternString, boolean negateMatch )
    {
    super( ANY, fieldDeclaration, patternString );
    this.negateMatch = negateMatch;
    this.delimiter = DEFAULT_DELIM;

    verify();
    }

  @ConstructorProperties({"fieldDeclaration", "patternString", "negateMatch", "delimiter"})
  protected RegexMatcher( Fields fieldDeclaration, String patternString, boolean negateMatch, String delimiter )
    {
    super( ANY, fieldDeclaration, patternString );
    this.negateMatch = negateMatch;
    this.delimiter = delimiter;

    if( this.delimiter == null )
      throw new IllegalArgumentException( "delimiter may not be null" );

    verify();
    }

  public final boolean isNegateMatch()
    {
    return negateMatch;
    }

  public final String getDelimiter()
    {
    return delimiter;
    }

  private void verify()
    {
    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "num fields in fieldDeclaration must be one, found: " + fieldDeclaration.printVerbose() );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Matcher> operationCall )
    {
    operationCall.setContext( getPattern().matcher( "" ) );
    }

  protected boolean matchWholeTuple( Matcher matcher, TupleEntry input )
    {
    Iterable<String> iterable = input.asIterableOf( String.class );
    String join = Util.join( iterable, delimiter, false );

    matcher.reset( join );

    boolean matchFound = matcher.find();

    if( LOG.isDebugEnabled() )
      LOG.debug( "pattern: {}, matches: {}", getPatternString(), matchFound );

    return matchFound == negateMatch;
    }

  protected boolean matchEachElement( Matcher matcher, TupleEntry input )
    {
    return matchEachElementPos( matcher, input ) != -1;
    }

  protected int matchEachElementPos( Matcher matcher, TupleEntry input )
    {
    int pos = 0;

    for( int i = 0; i < input.size(); i++ )
      {
      String value = input.getString( i );

      if( value == null )
        value = "";

      matcher.reset( value );

      boolean matchFound = matcher.find();

      if( LOG.isDebugEnabled() )
        LOG.debug( "pattern: " + getPatternString() + ", matches: " + matchFound + ", element: '" + value + "'" );

      if( matchFound == negateMatch )
        return pos;

      pos++;
      }

    return -1;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof RegexMatcher ) )
      return false;
    if( !super.equals( object ) )
      return false;

    RegexMatcher that = (RegexMatcher) object;

    if( negateMatch != that.negateMatch )
      return false;

    return !( delimiter != null ? !delimiter.equals( that.delimiter ) : that.delimiter != null );
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( negateMatch ? 1 : 0 );
    result = 31 * result + ( delimiter != null ? delimiter.hashCode() : 0 );
    return result;
    }
  }
