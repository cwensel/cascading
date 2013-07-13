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

package cascading.tap.local;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.tap.BaseTemplateTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.io.TapFileOutputStream;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntrySchemeCollector;

/**
 * Class TemplateTap can be used to write tuple streams out to files and sub-directories based on the values in the {@link cascading.tuple.Tuple}
 * instance.
 * <p/>
 * The constructor takes a {@link FileTap} {@link cascading.tap.Tap} and a {@link java.util.Formatter} format syntax String. This allows
 * Tuple values at given positions to be used as directory names.
 * <p/>
 * {@code openTapsThreshold} limits the number of open files to be output to. This value defaults to 300 files.
 * Each time the threshold is exceeded, 10% of the least recently used open files will be closed.
 * <p/>
 * TemplateTap will populate a given {@code pathTemplate} without regard to case of the values being used. Thus
 * the resulting paths {@code 2012/June/} and {@code 2012/june/} will likely result in two open files into the same
 * location. Forcing the case to be consistent with an upstream {@link cascading.operation.Function} is recommended, see
 * {@link cascading.operation.expression.ExpressionFunction}.
 */
public class TemplateTap extends BaseTemplateTap<Properties, OutputStream>
  {
  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   *
   * @param parent       of type Tap
   * @param pathTemplate of type String
   */
  @ConstructorProperties({"parent", "pathTemplate"})
  public TemplateTap( FileTap parent, String pathTemplate )
    {
    this( parent, pathTemplate, OPEN_TAPS_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * <p/>
   * {@code openTapsThreshold} limits the number of open files to be output to.
   *
   * @param parent            of type Hfs
   * @param pathTemplate      of type String
   * @param openTapsThreshold of type int
   */
  @ConstructorProperties({"parent", "pathTemplate", "openTapsThreshold"})
  public TemplateTap( FileTap parent, String pathTemplate, int openTapsThreshold )
    {
    super( parent, pathTemplate, openTapsThreshold );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   *
   * @param parent       of type Tap
   * @param pathTemplate of type String
   * @param sinkMode     of type SinkMode
   */
  @ConstructorProperties({"parent", "pathTemplate", "sinkMode"})
  public TemplateTap( FileTap parent, String pathTemplate, SinkMode sinkMode )
    {
    super( parent, pathTemplate, sinkMode );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * <p/>
   * {@code keepParentOnDelete}, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(Object)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   *
   * @param parent             of type Tap
   * @param pathTemplate       of type String
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   */
  @ConstructorProperties({"parent", "pathTemplate", "sinkMode", "keepParentOnDelete"})
  public TemplateTap( FileTap parent, String pathTemplate, SinkMode sinkMode, boolean keepParentOnDelete )
    {
    this( parent, pathTemplate, sinkMode, keepParentOnDelete, OPEN_TAPS_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * <p/>
   * {@code keepParentOnDelete}, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(Object)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   * <p/>
   * {@code openTapsThreshold} limits the number of open files to be output to.
   *
   * @param parent             of type Tap
   * @param pathTemplate       of type String
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   * @param openTapsThreshold  of type int
   */
  @ConstructorProperties({"parent", "pathTemplate", "sinkMode", "keepParentOnDelete", "openTapsThreshold"})
  public TemplateTap( FileTap parent, String pathTemplate, SinkMode sinkMode, boolean keepParentOnDelete, int openTapsThreshold )
    {
    super( parent, pathTemplate, sinkMode, keepParentOnDelete, openTapsThreshold );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link FileTap} Tap as the
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
  public TemplateTap( FileTap parent, String pathTemplate, Fields pathFields )
    {
    this( parent, pathTemplate, pathFields, OPEN_TAPS_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * The pathFields is a selector that selects and orders the fields to be used in the given pathTemplate.
   * <p/>
   * This constructor also allows the sinkFields of the parent Tap to be independent of the pathFields. Thus allowing
   * data not in the result file to be used in the template path name.
   * <p/>
   * {@code openTapsThreshold} limits the number of open files to be output to.
   *
   * @param parent            of type Hfs
   * @param pathTemplate      of type String
   * @param pathFields        of type Fields
   * @param openTapsThreshold of type int
   */
  @ConstructorProperties({"parent", "pathTemplate", "pathFields", "openTapsThreshold"})
  public TemplateTap( FileTap parent, String pathTemplate, Fields pathFields, int openTapsThreshold )
    {
    super( parent, pathTemplate, pathFields, openTapsThreshold );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link FileTap} Tap as the
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
  public TemplateTap( FileTap parent, String pathTemplate, Fields pathFields, SinkMode sinkMode )
    {
    super( parent, pathTemplate, pathFields, sinkMode );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * The pathFields is a selector that selects and orders the fields to be used in the given pathTemplate.
   * <p/>
   * This constructor also allows the sinkFields of the parent Tap to be independent of the pathFields. Thus allowing
   * data not in the result file to be used in the template path name.
   * <p/>
   * {@code keepParentOnDelete}, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(Object)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   *
   * @param parent             of type Tap
   * @param pathTemplate       of type String
   * @param pathFields         of type Fields
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   */
  @ConstructorProperties({"parent", "pathTemplate", "pathFields", "sinkMode", "keepParentOnDelete"})
  public TemplateTap( FileTap parent, String pathTemplate, Fields pathFields, SinkMode sinkMode, boolean keepParentOnDelete )
    {
    this( parent, pathTemplate, pathFields, sinkMode, keepParentOnDelete, OPEN_TAPS_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor TemplateTap creates a new TemplateTap instance using the given parent {@link FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the pathTemplate as the {@link java.util.Formatter} format String.
   * The pathFields is a selector that selects and orders the fields to be used in the given pathTemplate.
   * <p/>
   * This constructor also allows the sinkFields of the parent Tap to be independent of the pathFields. Thus allowing
   * data not in the result file to be used in the template path name.
   * <p/>
   * {@code keepParentOnDelete}, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(Object)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   * <p/>
   * {@code openTapsThreshold} limits the number of open files to be output to.
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
  public TemplateTap( FileTap parent, String pathTemplate, Fields pathFields, SinkMode sinkMode, boolean keepParentOnDelete, int openTapsThreshold )
    {
    super( parent, pathTemplate, pathFields, sinkMode, keepParentOnDelete, openTapsThreshold );
    }

  @Override
  protected TupleEntrySchemeCollector createTupleEntrySchemeCollector( FlowProcess<Properties> flowProcess, Tap parent, String path ) throws IOException
    {
    TapFileOutputStream output = new TapFileOutputStream( parent, path, isUpdate() ); // append if we are in update mode

    return new TupleEntrySchemeCollector<Properties, OutputStream>( flowProcess, parent, output );
    }
  }
