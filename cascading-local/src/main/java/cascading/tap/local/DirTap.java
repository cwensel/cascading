/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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
 *
 */

package cascading.tap.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import cascading.flow.FlowProcess;
import cascading.scheme.FileFormat;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.TapException;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class DirTap processes all files in the given directory that match the given glob or regex.
 * <p>
 * A DirTap can be used as a sink or a source.
 * <p>
 * When used as a source, the given pattern and depth are used to identify input files from the filesystem.
 * <p>
 * When used as a sink, a single file is created in the directory for all the output data. The file is name is
 * returned by {@link #getOutputIdentifier()} and can be overridden (see {@link #getOutputFilename()} and
 * {@link #getOutputFileBasename()}).
 * <p>
 * When deleting the resource identified by this Tap, the value of {@link #getOutputIdentifier()} will
 * be deleted, if it exists.
 * <p>
 * DirTap must be used with the {@link cascading.flow.local.LocalFlowConnector} to create
 * {@link cascading.flow.Flow} instances that run in "local" mode.
 * <p>
 * The given pattern must adhere to the syntax supported by {@link FileSystem#getPathMatcher(String)}.
 * <p>
 * Or a sub-class may override {@link #getPathMatcher()} and return a custom matcher.
 * <p>
 * The maxDepth parameter is the maximum number of levels of directories to visit. A value of 0 means that only the
 * starting file is visited, unless denied by the security manager.
 * <p>
 * A value of MAX_VALUE (the default) may be used to indicate that all levels should be visited.
 */
public class DirTap extends FileTap
  {
  private static final Logger LOG = LoggerFactory.getLogger( DirTap.class );

  int maxDepth = Integer.MAX_VALUE;
  String pattern;

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme} and file {@code directory}.
   *
   * @param scheme    of type Scheme
   * @param directory of type String
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String directory )
    {
    super( scheme, directory );

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme}, file {@code directory},
   * and {@code pattern}.
   *
   * @param scheme    of type Scheme
   * @param directory of type String
   * @param pattern   of type String
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String directory, String pattern )
    {
    super( scheme, directory );
    this.pattern = pattern;

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme}, file {@code directory},
   * {@code pattern}, and {@code maxDepth}
   *
   * @param scheme    of type Scheme
   * @param directory of type String
   * @param pattern   of type String
   * @param maxDepth  of type int
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String directory, String pattern, int maxDepth )
    {
    super( scheme, directory );
    this.maxDepth = maxDepth;
    this.pattern = pattern;

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme} and file {@code directory}.
   *
   * @param scheme    of type Scheme
   * @param directory of type String
   * @param sinkMode  of type SinkMode
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String directory, SinkMode sinkMode )
    {
    super( scheme, directory, sinkMode );

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme}, file {@code directory},
   * and {@code pattern}.
   *
   * @param scheme    of type Scheme
   * @param directory of type String
   * @param pattern   of type String
   * @param sinkMode  of type SinkMode
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String directory, String pattern, SinkMode sinkMode )
    {
    super( scheme, directory, sinkMode );
    this.pattern = pattern;

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme}, file {@code directory},
   * {@code pattern}, and {@code maxDepth}
   *
   * @param scheme    of type Scheme
   * @param directory of type String
   * @param pattern   of type String
   * @param maxDepth  of type int
   * @param sinkMode  of type SinkMode
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String directory, String pattern, int maxDepth, SinkMode sinkMode )
    {
    super( scheme, directory, sinkMode );
    this.maxDepth = maxDepth;
    this.pattern = pattern;

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme} and file {@code directory}.
   *
   * @param scheme    of type Scheme
   * @param directory of type Path
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, Path directory )
    {
    super( scheme, directory );

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme}, file {@code directory},
   * and {@code pattern}.
   *
   * @param scheme    of type Scheme
   * @param directory of type Path
   * @param pattern   of type String
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, Path directory, String pattern )
    {
    super( scheme, directory );
    this.pattern = pattern;

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme}, file {@code directory},
   * {@code pattern}, and {@code maxDepth}
   *
   * @param scheme    of type Scheme
   * @param directory of type Path
   * @param pattern   of type String
   * @param maxDepth  of type int
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, Path directory, String pattern, int maxDepth )
    {
    super( scheme, directory );
    this.maxDepth = maxDepth;
    this.pattern = pattern;

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme} and file {@code directory}.
   *
   * @param scheme    of type Scheme
   * @param directory of type Path
   * @param sinkMode  of type SinkMode
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, Path directory, SinkMode sinkMode )
    {
    super( scheme, directory, sinkMode );

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme}, file {@code directory},
   * and {@code pattern}.
   *
   * @param scheme    of type Scheme
   * @param directory of type Path
   * @param pattern   of type String
   * @param sinkMode  of type SinkMode
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, Path directory, String pattern, SinkMode sinkMode )
    {
    super( scheme, directory, sinkMode );
    this.pattern = pattern;

    verify();
    }

  /**
   * Constructor DirTap creates a new DirTap instance using the given {@link cascading.scheme.Scheme}, file {@code directory},
   * {@code pattern}, and {@code maxDepth}
   *
   * @param scheme    of type Scheme
   * @param directory of type Path
   * @param pattern   of type String
   * @param maxDepth  of type int
   * @param sinkMode  of type SinkMode
   */
  public DirTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, Path directory, String pattern, int maxDepth, SinkMode sinkMode )
    {
    super( scheme, directory, sinkMode );
    this.maxDepth = maxDepth;
    this.pattern = pattern;

    verify();
    }

  protected void verify()
    {
    super.verify();

    if( maxDepth < 0 )
      throw new IllegalArgumentException( "maxDepth must be greater than 0, given: " + maxDepth );

    try
      {
      getPathMatcher();
      }
    catch( RuntimeException exception )
      {
      throw new IllegalArgumentException( "could not parse pattern: " + getPattern(), exception );
      }
    }

  @Override
  public String getOutputIdentifier()
    {
    return getPath().resolve( getOutputFilename() ).toString();
    }

  public String getOutputFilename()
    {
    if( getScheme() instanceof FileFormat )
      return getOutputFileBasename() + "." + ( (FileFormat) getScheme() ).getExtension();

    return getOutputFileBasename() + ".tap";
    }

  protected String getOutputFileBasename()
    {
    return "output";
    }

  public String getPattern()
    {
    return pattern;
    }

  public int getMaxDepth()
    {
    return maxDepth;
    }

  @Override
  public boolean deleteResource( Properties conf ) throws IOException
    {
    return deleteDirTap( this, conf );
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Properties> flowProcess, InputStream input ) throws IOException
    {
    if( !Files.isDirectory( getPath() ) && getPattern() != null )
      throw new IllegalStateException( "a file pattern was provided and given path is not a directory: " + getPath() );

    if( !Files.isDirectory( getPath() ) )
      return super.openForRead( flowProcess, input );

    PathMatcher pathMatcher = getPathMatcher();

    CloseableIterator<InputStream> iterator = new CloseableIterator<InputStream>()
      {
      Stream<Path> stream = Files.walk( getPath(), maxDepth )
        .filter( path -> !Files.isDirectory( path ) )
        .filter( pathMatcher::matches );
      Iterator<Path> iterator = stream.iterator();
      InputStream lastInputStream = null;

      @Override
      public boolean hasNext()
        {
        return iterator.hasNext();
        }

      @Override
      public InputStream next()
        {
        safeClose();

        Path path = iterator.next();

        flowProcess.getFlowProcessContext().setSourcePath( path.toAbsolutePath().toString() );

        if( LOG.isDebugEnabled() )
          LOG.debug( "opening: {}", path );

        try
          {
          lastInputStream = Files.newInputStream( path );

          return lastInputStream;
          }
        catch( IOException exception )
          {
          throw new TapException( "unable to open path: " + path, exception );
          }
        }

      private void safeClose()
        {
        try
          {
          if( lastInputStream != null )
            lastInputStream.close();

          lastInputStream = null;
          }
        catch( IOException exception )
          {
          // do nothing
          }
        }

      @Override
      public void close() throws IOException
        {
        safeClose();

        if( stream != null )
          stream.close();
        }
      };

    return new TupleEntrySchemeIterator<Properties, InputStream>( flowProcess, this, getScheme(), iterator );
    }

  @Override
  public String[] getChildIdentifiers( Properties conf, int depth, boolean fullyQualified ) throws IOException
    {
    if( !resourceExists( conf ) )
      return new String[ 0 ];

    if( !Files.isDirectory( getPath() ) )
      throw new IllegalStateException( "given path is not a directory: " + getPath() );

    Set<String> results = new LinkedHashSet<String>();

    PathMatcher pathMatcher = getPathMatcher();

    try( final Stream<Path> pathStream = Files.walk( getPath(), depth ) )
      {
      pathStream
        .filter( path -> !Files.isDirectory( path ) )
        .filter( pathMatcher::matches )
        .forEach( path -> results.add( fullyQualified ? path.toAbsolutePath().toString() : path.toString() ) );
      }

    return results.toArray( new String[ results.size() ] );
    }

  protected PathMatcher getPathMatcher()
    {
    if( getPattern() == null )
      return path -> true;

    FileSystem fileSystem = getPath().getFileSystem();

    return fileSystem.getPathMatcher( getPattern() );
    }

  /**
   * Method deleteDirTap will recursively delete all files referenced by the given DirTap.
   *
   * @param dirTap the directory to delete
   */
  public static boolean deleteDirTap( DirTap dirTap, Properties conf ) throws IOException
    {
    deleteChildren( dirTap.getPath() , dirTap.getChildIdentifiers( conf ) );

    Files.deleteIfExists( dirTap.getPath() );

    return true;
    }

  /**
   * Deletes the child files and their directories. Does not delete the parent path.
   *
   * @param parentPath
   * @param childIdentifiers
   * @throws IOException
   */
  protected static void deleteChildren( Path parentPath, String[] childIdentifiers ) throws IOException
    {
    Set<Path> parents = new HashSet<>();

    for( String childIdentifier : childIdentifiers )
      {
      Path path = Paths.get( childIdentifier );

      parents.add( parentPath.resolve( parentPath.relativize( path ).subpath( 0, 1 ) ) );
      }

    for( Path subParent : parents )
      recursiveDelete( subParent );
    }

  private static void recursiveDelete( Path path ) throws IOException
    {
    if( path == null )
      return;

    if( Files.isDirectory( path ) )
      {
      try( DirectoryStream<Path> paths = Files.newDirectoryStream( path ) )
        {
        for( Path current : paths )
          recursiveDelete( current );
        }
      }

    Files.deleteIfExists( path );
    }
  }
