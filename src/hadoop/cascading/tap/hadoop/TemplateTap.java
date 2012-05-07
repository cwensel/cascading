/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SinkMode;
import cascading.tap.SinkTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class TemplateTap can be used to write tuple streams out to sub-directories based on the values in the {@link Tuple}
 * instance.
 * <p/>
 * The constructor takes a {@link Hfs} {@link cascading.tap.Tap} and a {@link java.util.Formatter} format syntax String. This allows
 * Tuple values at given positions to be used as directory names. Note that Hadoop can only sink to directories, and
 * all files in those directories are "part-xxxxx" files.
 * <p/>
 * {@code openTapsThreshold} limits the number of open files to be output to. This value defaults to 300 files.
 * Each time the threshold is exceeded, 10% of the least recently used open files will be closed.
 */
public class TemplateTap extends SinkTap<JobConf, OutputCollector>
  {
  public enum Counters
    {
      Paths_Opened, Paths_Closed, Path_Purges
    }

  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( TemplateTap.class );

  /** Field OPEN_FILES_THRESHOLD_DEFAULT */
  private static final int OPEN_TAPS_THRESHOLD_DEFAULT = 300;

  /** Field parent */
  private final Hfs parent;
  /** Field pathTemplate */
  private final String pathTemplate;
  /** Field keepParentOnDelete */
  private boolean keepParentOnDelete = false;
  /** Field openTapsThreshold */
  private int openTapsThreshold = OPEN_TAPS_THRESHOLD_DEFAULT;
  /** Field collectors */
  private final Map<String, TupleEntryCollector> collectors = new LinkedHashMap<String, TupleEntryCollector>( 1000, .75f, true );

  private class TemplateCollector extends TupleEntryCollector implements OutputCollector
    {
    private final FlowProcess<JobConf> flowProcess;
    private final JobConf conf;
    private final Fields parentFields;
    private final Fields pathFields;

    public TemplateCollector( FlowProcess<JobConf> flowProcess )
      {
      super( Fields.asDeclaration( getSinkFields() ) );
      this.flowProcess = flowProcess;
      this.conf = flowProcess.getConfigCopy();
      this.parentFields = parent.getSinkFields();
      this.pathFields = ( (TemplateScheme) getScheme() ).pathFields;
      }

    private TupleEntryCollector getCollector( String path )
      {
      TupleEntryCollector collector = collectors.get( path );

      if( collector != null )
        return collector;

      try
        {
        Path fullPath = new Path( parent.getFullIdentifier( conf ), path );

        LOG.debug( "creating collector for path: {}", fullPath );

        collector = new HadoopTupleEntrySchemeCollector( flowProcess, parent, path );

        flowProcess.increment( Counters.Paths_Opened, 1 );
        }
      catch( IOException exception )
        {
        throw new TapException( "unable to open template path: " + path, exception );
        }

      if( collectors.size() > openTapsThreshold )
        purgeCollectors();

      collectors.put( path, collector );

      if( LOG.isInfoEnabled() && collectors.size() % 100 == 0 )
        LOG.info( "caching {} open Taps", collectors.size() );

      return collector;
      }

    private void purgeCollectors()
      {
      int numToClose = Math.max( 1, (int) ( openTapsThreshold * .10 ) );

      if( LOG.isInfoEnabled() )
        LOG.info( "removing {} open Taps from cache of size {}", numToClose, collectors.size() );

      Set<String> removeKeys = new HashSet<String>();
      Set<String> keys = collectors.keySet();

      for( String key : keys )
        {
        if( numToClose-- == 0 )
          break;

        removeKeys.add( key );
        }

      for( String removeKey : removeKeys )
        closeCollector( collectors.remove( removeKey ) );

      flowProcess.increment( Counters.Path_Purges, 1 );
      }

    @Override
    public void close()
      {
      super.close();

      try
        {
        for( TupleEntryCollector collector : collectors.values() )
          closeCollector( collector );
        }
      finally
        {
        collectors.clear();
        }
      }

    private void closeCollector( TupleEntryCollector collector )
      {
      if( collector == null )
        return;

      try
        {
        ( (TupleEntryCollector) collector ).close();

        flowProcess.increment( Counters.Paths_Closed, 1 );
        }
      catch( Exception exception )
        {
        // do nothing
        }
      }

    protected void collect( TupleEntry tupleEntry ) throws IOException
      {
      if( pathFields != null )
        {
        Tuple pathValues = tupleEntry.selectTuple( pathFields );
        String path = pathValues.format( pathTemplate );

        getCollector( path ).add( tupleEntry.selectTuple( parentFields ) );
        }
      else
        {
        String path = tupleEntry.getTuple().format( pathTemplate );

        getCollector( path ).add( tupleEntry );
        }
      }

    public void collect( Object key, Object value ) throws IOException
      {
      throw new UnsupportedOperationException( "unimplemented" );
      }
    }

  public static class TemplateScheme extends Scheme<JobConf, Void, OutputCollector, Void, Void>
    {
    private final Scheme scheme;
    private final Fields pathFields;

    public TemplateScheme( Scheme scheme )
      {
      this.scheme = scheme;
      this.pathFields = null;
      }

    public TemplateScheme( Scheme scheme, Fields pathFields )
      {
      this.scheme = scheme;

      if( pathFields == null || pathFields.isAll() )
        this.pathFields = null;
      else if( pathFields.isDefined() )
        this.pathFields = pathFields;
      else
        throw new IllegalArgumentException( "pathFields must be defined or the ALL substitution, got: " + pathFields.printVerbose() );
      }

    public Fields getSinkFields()
      {
      if( pathFields == null )
        return scheme.getSinkFields();

      return Fields.merge( scheme.getSinkFields(), pathFields );
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

    @Override
    public void sourceConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, Void, OutputCollector> tap, JobConf conf )
      {
      scheme.sourceConfInit( flowProcess, tap, conf );
      }

    @Override
    public void sourcePrepare( FlowProcess<JobConf> flowProcess, SourceCall<Void, Void> sourceCall ) throws IOException
      {
      scheme.sourcePrepare( flowProcess, sourceCall );
      }

    @Override
    public boolean source( FlowProcess<JobConf> flowProcess, SourceCall<Void, Void> sourceCall ) throws IOException
      {
      throw new UnsupportedOperationException( "not supported" );
      }

    @Override
    public void sourceCleanup( FlowProcess<JobConf> flowProcess, SourceCall<Void, Void> sourceCall ) throws IOException
      {
      scheme.sourceCleanup( flowProcess, sourceCall );
      }

    @Override
    public void sinkConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, Void, OutputCollector> tap, JobConf conf )
      {
      scheme.sinkConfInit( flowProcess, tap, conf );
      }

    @Override
    public void sinkPrepare( FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector> sinkCall ) throws IOException
      {
      scheme.sinkPrepare( flowProcess, sinkCall );
      }

    @Override
    public void sink( FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector> sinkCall ) throws IOException
      {
      throw new UnsupportedOperationException( "should never be called" );
      }

    @Override
    public void sinkCleanup( FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector> sinkCall ) throws IOException
      {
      scheme.sinkCleanup( flowProcess, sinkCall );
      }
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   *
   * @param parent       of type Tap
   * @param pathTemplate of type String
   */
  @ConstructorProperties({"parent", "pathTemplate"})
  public TemplateTap( Hfs parent, String pathTemplate )
    {
    this( parent, pathTemplate, OPEN_TAPS_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * <p/>
   * openTapsThreshold limits the number of open files to be output to.
   *
   * @param parent            of type Hfs
   * @param pathTemplate      of type String
   * @param openTapsThreshold of type int
   */
  @ConstructorProperties({"parent", "pathTemplate", "openTapsThreshold"})
  public TemplateTap( Hfs parent, String pathTemplate, int openTapsThreshold )
    {
    super( new TemplateScheme( parent.getScheme() ) );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    this.openTapsThreshold = openTapsThreshold;
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   *
   * @param parent       of type Tap
   * @param pathTemplate of type String
   * @param sinkMode     of type SinkMode
   */
  @ConstructorProperties({"parent", "pathTemplate", "sinkMode"})
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
   * keepParentOnDelete, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(org.apache.hadoop.mapred.JobConf)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   *
   * @param parent             of type Tap
   * @param pathTemplate       of type String
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   */
  @ConstructorProperties({"parent", "pathTemplate", "sinkMode", "keepParentOnDelete"})
  public TemplateTap( Hfs parent, String pathTemplate, SinkMode sinkMode, boolean keepParentOnDelete )
    {
    this( parent, pathTemplate, sinkMode, keepParentOnDelete, OPEN_TAPS_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * <p/>
   * keepParentOnDelete, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(org.apache.hadoop.mapred.JobConf)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   * <p/>
   * openTapsThreshold limits the number of open files to be output to.
   *
   * @param parent             of type Tap
   * @param pathTemplate       of type String
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   * @param openTapsThreshold  of type int
   */
  @ConstructorProperties({"parent", "pathTemplate", "sinkMode", "keepParentOnDelete", "openTapsThreshold"})
  public TemplateTap( Hfs parent, String pathTemplate, SinkMode sinkMode, boolean keepParentOnDelete, int openTapsThreshold )
    {
    super( new TemplateScheme( parent.getScheme() ), sinkMode );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    this.keepParentOnDelete = keepParentOnDelete;
    this.openTapsThreshold = openTapsThreshold;
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
  @ConstructorProperties({"parent", "pathTemplate", "pathFields"})
  public TemplateTap( Hfs parent, String pathTemplate, Fields pathFields )
    {
    this( parent, pathTemplate, pathFields, OPEN_TAPS_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * The pathFields is a selector that selects and orders the fields to be used in the given pathTemplate.
   * <p/>
   * This constructor also allows the sinkFields of the parent Tap to be independent of the pathFields. Thus allowing
   * data not in the result file to be used in the template path name.
   * <p/>
   * openTapsThreshold limits the number of open files to be output to.
   *
   * @param parent            of type Hfs
   * @param pathTemplate      of type String
   * @param pathFields        of type Fields
   * @param openTapsThreshold of type int
   */
  @ConstructorProperties({"parent", "pathTemplate", "pathFields", "openTapsThreshold"})
  public TemplateTap( Hfs parent, String pathTemplate, Fields pathFields, int openTapsThreshold )
    {
    super( new TemplateScheme( parent.getScheme(), pathFields ) );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    this.openTapsThreshold = openTapsThreshold;
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
  @ConstructorProperties({"parent", "pathTemplate", "pathFields", "sinkMode"})
  public TemplateTap( Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode )
    {
    super( new TemplateScheme( parent.getScheme(), pathFields ), sinkMode );
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
   * keepParentOnDelete, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(org.apache.hadoop.mapred.JobConf)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   *
   * @param parent             of type Tap
   * @param pathTemplate       of type String
   * @param pathFields         of type Fields
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   */
  @ConstructorProperties({"parent", "pathTemplate", "pathFields", "sinkMode", "keepParentOnDelete"})
  public TemplateTap( Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode, boolean keepParentOnDelete )
    {
    this( parent, pathTemplate, pathFields, sinkMode, keepParentOnDelete, OPEN_TAPS_THRESHOLD_DEFAULT );
    }

  /**
   * /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * The pathFields is a selector that selects and orders the fields to be used in the given pathTemplate.
   * <p/>
   * This constructor also allows the sinkFields of the parent Tap to be independent of the pathFields. Thus allowing
   * data not in the result file to be used in the template path name.
   * <p/>
   * keepParentOnDelete, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(org.apache.hadoop.mapred.JobConf)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   * <p/>
   * openTapsThreshold limits the number of open files to be output to.
   *
   * @param parent             of type Hfs
   * @param pathTemplate       of type String
   * @param pathFields         of type Fields
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   * @param openTapsThreshold  of type int
   */
  @ConstructorProperties({"parent", "pathTemplate", "pathFields", "sinkMode", "keepParentOnDelete",
                          "openTapsThreshold"})
  public TemplateTap( Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode, boolean keepParentOnDelete, int openTapsThreshold )
    {
    super( new TemplateScheme( parent.getScheme(), pathFields ), sinkMode );
    this.parent = parent;
    this.pathTemplate = pathTemplate;
    this.keepParentOnDelete = keepParentOnDelete;
    this.openTapsThreshold = openTapsThreshold;
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
  public String getIdentifier()
    {
    return parent.getIdentifier();
    }

  /**
   * Method getOpenTapsThreshold returns the openTapsThreshold of this TemplateTap object.
   *
   * @return the openTapsThreshold (type int) of this TemplateTap object.
   */
  public int getOpenTapsThreshold()
    {
    return openTapsThreshold;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<JobConf> flowProcess, OutputCollector output ) throws IOException
    {
    return new TemplateCollector( flowProcess );
    }

  /** @see Tap#createResource(Object) */
  public boolean createResource( JobConf conf ) throws IOException
    {
    return parent.createResource( conf );
    }

  /** @see Tap#deleteResource(Object) */
  public boolean deleteResource( JobConf conf ) throws IOException
    {
    return keepParentOnDelete || parent.deleteResource( conf );
    }

  @Override
  public boolean commitResource( JobConf conf ) throws IOException
    {
    return parent.commitResource( conf );
    }

  @Override
  public boolean rollbackResource( JobConf conf ) throws IOException
    {
    return parent.rollbackResource( conf );
    }

  /** @see Tap#resourceExists(Object) */
  public boolean resourceExists( JobConf conf ) throws IOException
    {
    return parent.resourceExists( conf );
    }

  /** @see Tap#getModifiedTime(Object) */
  @Override
  public long getModifiedTime( JobConf conf ) throws IOException
    {
    return parent.getModifiedTime( conf );
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
