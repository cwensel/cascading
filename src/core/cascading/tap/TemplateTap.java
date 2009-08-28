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

package cascading.tap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowContext;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.hadoop.HfsCollector;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
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
  /** Field keepParentOnDelete */
  private boolean keepParentOnDelete = false;
  /** Field collectors */
  private Map<String, TupleEntryCollector> collectors = new HashMap<String, TupleEntryCollector>();

  private class TemplateCollector extends TupleEntryCollector
    {
    Configuration conf;

    public TemplateCollector( Configuration conf )
      {
      this.conf = conf;
      }

    private TupleEntryCollector getCollector( String path )
      {
      TupleEntryCollector collector = collectors.get( path );

      if( collector != null )
        return collector;

      try
        {
        Tap tap = new Hfs( parent.getScheme(), parent.getQualifiedPath( new Job( conf ) ).toString() );

        if( LOG.isDebugEnabled() )
          LOG.debug( "creating collector for path: " + new Path( parent.getQualifiedPath( new Job( conf ) ), path ) );

        collector = new HfsCollector( tap, path, conf );
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

      try
        {
        for( TupleEntryCollector collector : collectors.values() )
          {
          try
            {
            collector.close();
            }
          catch( Exception exception )
            {
            // do nothing
            }
          }
        }
      finally
        {
        collectors.clear();
        }
      }

    protected void collect( Tuple tuple )
      {
      String path = ( (Tuple) tuple ).format( pathTemplate );

      getCollector( path ).add( tuple );
      }

    }

  public static class TemplateScheme extends Scheme
    {
    private final Scheme scheme;
    private final Fields pathFields;
    private final String pathTemplate;

    public TemplateScheme( Scheme scheme )
      {
      this.scheme = scheme;
      this.pathFields = null;
      this.pathTemplate = null;
      }

    public TemplateScheme( Scheme scheme, String pathTemplate, Fields pathFields )
      {
      this.scheme = scheme;
      this.pathFields = pathFields;
      this.pathTemplate = pathTemplate;
      }

    public Fields getSinkFields()
      {
      return scheme.getSinkFields();
      }

    public void setSinkFields( Fields sinkFields )
      {
      scheme.setSinkFields( sinkFields );
      }

    public Fields getSourceFields()
      {
      return scheme.getSourceFields();
      }

    public void setSourceFields( Fields sourceFields )
      {
      scheme.setSourceFields( sourceFields );
      }

    public int getNumSinkParts()
      {
      return scheme.getNumSinkParts();
      }

    public void setNumSinkParts( int numSinkParts )
      {
      scheme.setNumSinkParts( numSinkParts );
      }

    public boolean isWriteDirect()
      {
      return scheme.isWriteDirect();
      }

    public void sourceInit( Tap tap, Job job ) throws IOException
      {
      scheme.sourceInit( tap, job );
      }

    public void sinkInit( Tap tap, Job job ) throws IOException
      {
      scheme.sinkInit( tap, job );
      }

    public void source( Tuple tuple, TupleEntryCollector tupleEntryCollector )
      {
      scheme.source( tuple, tupleEntryCollector );
      }

    public void sink( TupleEntry tupleEntry, TupleEntryCollector tupleEntryCollector ) throws IOException
      {
      if( pathFields != null )
        {
        Tuple values = tupleEntry.selectTuple( pathFields );
        tupleEntryCollector = ( (TemplateCollector) tupleEntryCollector ).getCollector( values.format( pathTemplate ) );
        }

      scheme.sink( tupleEntry, tupleEntryCollector );
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
    super( new TemplateScheme( parent.getScheme() ) );
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
    super( new TemplateScheme( parent.getScheme() ), sinkMode );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * <p/>
   * keepParentOnDelete, when set to true, prevents the parent Tap from being deleted when {@link #deletePath(Job)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   *
   * @param parent             of type Tap
   * @param pathTemplate       of type String
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   */
  public TemplateTap( Hfs parent, String pathTemplate, SinkMode sinkMode, boolean keepParentOnDelete )
    {
    super( new TemplateScheme( parent.getScheme() ), sinkMode );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    this.keepParentOnDelete = keepParentOnDelete;
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * The pathFields is a selector that selects and orders the fields to be used in the given pathTemplate.
   * <p/>
   * This constructor also allows the sinkFields of the parent Tap to be independent of the pathFields. Thus allowing
   * data not in the result file to be used in the template path name.
   *
   * @param parent       of type Tap
   * @param pathTemplate of type String
   * @param pathFields   of type Fields
   */
  public TemplateTap( Hfs parent, String pathTemplate, Fields pathFields )
    {
    super( new TemplateScheme( parent.getScheme(), pathTemplate, pathFields ) );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * The pathFields is a selector that selects and orders the fields to be used in the given pathTemplate.
   * <p/>
   * This constructor also allows the sinkFields of the parent Tap to be independent of the pathFields. Thus allowing
   * data not in the result file to be used in the template path name.
   *
   * @param parent       of type Tap
   * @param pathTemplate of type String
   * @param pathFields   of type Fields
   * @param sinkMode     of type SinkMode
   */
  public TemplateTap( Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode )
    {
    super( new TemplateScheme( parent.getScheme(), pathTemplate, pathFields ), sinkMode );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * The pathFields is a selector that selects and orders the fields to be used in the given pathTemplate.
   * <p/>
   * This constructor also allows the sinkFields of the parent Tap to be independent of the pathFields. Thus allowing
   * data not in the result file to be used in the template path name.
   * <p/>
   * keepParentOnDelete, when set to true, prevents the parent Tap from being deleted when {@link #deletePath(Job)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   *
   * @param parent             of type Tap
   * @param pathTemplate       of type String
   * @param pathFields         of type Fields
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   */
  public TemplateTap( Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode, boolean keepParentOnDelete )
    {
    super( new TemplateScheme( parent.getScheme(), pathTemplate, pathFields ), sinkMode );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    this.keepParentOnDelete = keepParentOnDelete;
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
  public TupleEntryCollector openForWrite( HadoopFlowContext flowContext ) throws IOException
    {
    Configuration conf = null;

    if( flowContext instanceof Flow )
      conf = ( (Flow) flowContext ).getConfiguration();
    else
      conf = ( (HadoopFlowProcess) flowContext ).getConfiguration();

    return new TemplateCollector( conf );
    }

  /** @see Tap#makeDirs(org.apache.hadoop.mapreduce.Job) */
  public boolean makeDirs( Job conf ) throws IOException
    {
    return parent.makeDirs( conf );
    }

  /** @see Tap#deletePath(org.apache.hadoop.mapreduce.Job) */
  public boolean deletePath( Job conf ) throws IOException
    {
    return keepParentOnDelete || parent.deletePath( conf );
    }

  /** @see Tap#pathExists(org.apache.hadoop.mapreduce.Job) */
  public boolean pathExists( Job conf ) throws IOException
    {
    return parent.pathExists( conf );
    }

  /** @see Tap#getPathModified(org.apache.hadoop.mapreduce.Job) */
  public long getPathModified( Job conf ) throws IOException
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

  @Override
  public String toString()
    {
    return getClass().getSimpleName() + "[\"" + parent + "\"]" + "[\"" + pathTemplate + "\"]";
    }
  }
