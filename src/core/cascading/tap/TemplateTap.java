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

package cascading.tap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.tap.hadoop.TapCollector;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

/**
 * Class TemplateTap can be used to write tuple streams out to subdirectories based on the values in the {@link Tuple}
 * instance.
 * <p/>
 * The constructor takes a {@link Hfs} {@link Tap} and a {@link java.util.Formatter} format syntax String. This allows
 * Tuple values at given positions to be used as directory names. Note that Hadoop can only sink to directories, and
 * all files in those directories are "part-xxxxx" files.
 */
public class TemplateTap extends SinkTap
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( TemplateTap.class );

  /** Field parent */
  private Tap parent;
  /** Field pathTemplate */
  private String pathTemplate;
  /** Field collectors */
  private Map<String, OutputCollector> collectors = new HashMap<String, OutputCollector>();

  private class TemplateCollector extends TupleEntryCollector implements OutputCollector
    {
    JobConf conf;

    public TemplateCollector( JobConf conf )
      {
      this.conf = conf;
      }

    protected void collect( Tuple tuple )
      {
      throw new UnsupportedOperationException( "collect should never be called on TemplateCollector" );
      }

    private OutputCollector getCollector( String path )
      {
      OutputCollector collector = collectors.get( path );

      if( collector != null )
        return collector;

      try
        {
        Path fullPath = new Path( parent.getQualifiedPath( conf ), path );
        Tap tap = new Hfs( parent.getScheme(), fullPath.toString() );

        if( LOG.isDebugEnabled() )
          LOG.debug( "creating collector for path: " + fullPath );

        collector = (OutputCollector) new TapCollector( tap, conf );
        }
      catch( IOException exception )
        {
        throw new TapException( "unable to open template path: " + path, exception );
        }

      collectors.put( path, collector );

      if( LOG.isInfoEnabled() && collectors.size() % 100 == 0 )
        LOG.info( "caching " + collectors.size() + " open Taps" );

      return collector;
      }

    @Override
    public void close()
      {
      super.close();

      for( OutputCollector collector : collectors.values() )
        ( (TupleEntryCollector) collector ).close();

      collectors.clear();
      }

    public void collect( Object key, Object value ) throws IOException
      {
      String path = ( (Tuple) value ).format( pathTemplate );

      getCollector( path ).collect( key, value );
      }
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   *
   * @param parent       of type Tap
   * @param pathTemplate of type String
   */
  public TemplateTap( Hfs parent, String pathTemplate )
    {
    super( parent.getScheme() );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   *
   * @param parent       of type Tap
   * @param pathTemplate of type String
   * @param sinkMode     of type SinkMode
   */
  public TemplateTap( Hfs parent, String pathTemplate, SinkMode sinkMode )
    {
    super( parent.getScheme(), sinkMode );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    }

  /**
   * Method getParent returns the parent Tap of this TemplateTap object.
   *
   * @return the parent (type Tap) of this TemplateTap object.
   */
  public Tap getParent()
    {
    return parent;
    }

  /**
   * Method getPathTemplate returns the pathTemplate {@link java.util.Formatter} format String of this TemplateTap object.
   *
   * @return the pathTemplate (type String) of this TemplateTap object.
   */
  public String getPathTemplate()
    {
    return pathTemplate;
    }

  @Override
  public boolean isWriteDirect()
    {
    return true;
    }

  /** @see Tap#getPath() */
  public Path getPath()
    {
    return parent.getPath();
    }

  @Override
  public TupleEntryCollector openForWrite( JobConf conf ) throws IOException
    {
    return new TemplateCollector( conf );
    }

  /** @see Tap#makeDirs(JobConf) */
  public boolean makeDirs( JobConf conf ) throws IOException
    {
    return parent.makeDirs( conf );
    }

  /** @see Tap#deletePath(JobConf) */
  public boolean deletePath( JobConf conf ) throws IOException
    {
    return parent.deletePath( conf );
    }

  /** @see Tap#pathExists(JobConf) */
  public boolean pathExists( JobConf conf ) throws IOException
    {
    return parent.pathExists( conf );
    }

  /** @see Tap#getPathModified(JobConf) */
  public long getPathModified( JobConf conf ) throws IOException
    {
    return parent.getPathModified( conf );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;
    if( !super.equals( object ) )
      return false;

    TemplateTap that = (TemplateTap) object;

    if( parent != null ? !parent.equals( that.parent ) : that.parent != null )
      return false;
    if( pathTemplate != null ? !pathTemplate.equals( that.pathTemplate ) : that.pathTemplate != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( parent != null ? parent.hashCode() : 0 );
    result = 31 * result + ( pathTemplate != null ? pathTemplate.hashCode() : 0 );
    return result;
    }
  }
