/*
 * Copyright (c) 2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.local.tap.aws.s3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Class S3FileCheckpointer persists a map of bucket names and last seen keys to disk
 * so multiple starts of a given application or new Flow instances relying on a {@link S3Tap}
 * will re-retrieve the same data as previous executions.
 * <p>
 * By default this class will write all checkpoints to the directory {@code [user.home]/.cascading/s3tap-checkpoints}
 * in files named {@code [bucket-name].txt}.
 * <p>
 * A {@link Function} can be supplied to override the filename creation.
 */
public class S3FileCheckpointer implements S3Checkpointer
  {
  Map<String, String> seenKeys = new LinkedHashMap<>();

  Path path = makeHidden( homeDir() );
  Function<String, String> filename = bucket -> bucket + ".txt";

  /**
   * Method homeDir uses the System property {@code user.home} to retrieve the user's home directory
   *
   * @return Path
   */

  public static Path homeDir()
    {
    return Paths.get( System.getProperty( "user.home" ) );
    }

  /**
   * Method currentDir uses the System property {@code user.dir} to retrieve the user's current working directory
   *
   * @return Path
   */
  public static Path currentDir()
    {
    return Paths.get( System.getProperty( "user.dir" ) );
    }

  /**
   * Method makeHidden will append {@code .cascading/s3tap-checkpoints} to the given Path instance.
   *
   * @param path of Path
   * @return Path
   */
  public static Path makeHidden( Path path )
    {
    return path.resolve( ".cascading" ).resolve( "s3tap-checkpoints" );
    }

  /**
   * Constructor S3FileCheckpointer creates a new S3FileCheckpointer instance.
   */
  public S3FileCheckpointer()
    {
    }

  /**
   * Constructor S3FileCheckpointer creates a new S3FileCheckpointer instance.
   *
   * @param path of String
   */
  public S3FileCheckpointer( String path )
    {
    this( Paths.get( path ) );
    }

  /**
   * Constructor S3FileCheckpointer creates a new S3FileCheckpointer instance.
   *
   * @param path of Path
   */
  public S3FileCheckpointer( Path path )
    {
    this.path = path;
    }

  /**
   * Constructor S3FileCheckpointer creates a new S3FileCheckpointer instance.
   *
   * @param filename of Function<String, String>
   */
  public S3FileCheckpointer( Function<String, String> filename )
    {
    this.filename = filename;
    }

  /**
   * Constructor S3FileCheckpointer creates a new S3FileCheckpointer instance.
   *
   * @param path     of Path
   * @param filename of Function<String, String>
   */
  public S3FileCheckpointer( Path path, Function<String, String> filename )
    {
    this.path = path;
    this.filename = filename;
    }

  @Override
  public String getLastKey( String bucketName )
    {
    Path input = getPathFor( bucketName );

    if( !Files.exists( input ) )
      return null;

    try
      {
      return Files.lines( input ).findFirst().orElse( null );
      }
    catch( IOException exception )
      {
      throw new UncheckedIOException( exception );
      }
    }

  private Path getPathFor( String bucketName )
    {
    return path.resolve( filename.apply( bucketName ) );
    }

  @Override
  public void setLastKey( String bucketName, String key )
    {
    seenKeys.put( bucketName, key );
    }

  @Override
  public void commit()
    {
    for( Map.Entry<String, String> entry : seenKeys.entrySet() )
      {
      try
        {
        Path bucketPath = getPathFor( entry.getKey() );

        Files.createDirectories( bucketPath.getParent() );

        Files.write( bucketPath, Collections.singleton( entry.getValue() ) );
        }
      catch( IOException exception )
        {
        throw new UncheckedIOException( exception );
        }
      }

    seenKeys.clear();
    }
  }
