/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.tap;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.function.Function;

import cascading.flow.FlowProcess;
import cascading.tap.type.FileType;

/**
 * Class FileAdaptorTap extends {@link AdaptorTap} to provide adapted {@link FileType} interface semantics.
 * <p>
 * The adapted {@link Tap} must be of type FileType.
 */
public class FileAdaptorTap<TConfig, TInput, TOutput, OConfig, OInput, OOutput> extends AdaptorTap<TConfig, TInput, TOutput, OConfig, OInput, OOutput> implements FileType<TConfig>
  {
  @ConstructorProperties({"original", "processProvider", "configProvider"})
  public FileAdaptorTap( Tap<OConfig, OInput, OOutput> original, Function<FlowProcess<? extends TConfig>, FlowProcess<? extends OConfig>> processProvider, Function<TConfig, OConfig> configProvider )
    {
    super( original, processProvider, configProvider );

    if( !( original instanceof FileType ) )
      throw new IllegalArgumentException( "original Tap must be of type: " + FileType.class.getName() );
    }

  protected FileType<TConfig> getFileOriginal()
    {
    return (FileType<TConfig>) getOriginal();
    }

  @Override
  public boolean isDirectory( FlowProcess<? extends TConfig> flowProcess ) throws IOException
    {
    return getFileOriginal().isDirectory( (FlowProcess<? extends TConfig>) processProvider.apply( flowProcess ) );
    }

  @Override
  public boolean isDirectory( TConfig conf ) throws IOException
    {
    return getFileOriginal().isDirectory( (TConfig) configProvider.apply( conf ) );
    }

  @Override
  public String[] getChildIdentifiers( FlowProcess<? extends TConfig> flowProcess ) throws IOException
    {
    return getFileOriginal().getChildIdentifiers( (TConfig) processProvider.apply( flowProcess ) );
    }

  @Override
  public String[] getChildIdentifiers( TConfig conf ) throws IOException
    {
    return getFileOriginal().getChildIdentifiers( (TConfig) configProvider.apply( conf ) );
    }

  @Override
  public String[] getChildIdentifiers( FlowProcess<? extends TConfig> flowProcess, int depth, boolean fullyQualified ) throws IOException
    {
    return getFileOriginal().getChildIdentifiers( (TConfig) processProvider.apply( flowProcess ), depth, fullyQualified );
    }

  @Override
  public String[] getChildIdentifiers( TConfig conf, int depth, boolean fullyQualified ) throws IOException
    {
    return getFileOriginal().getChildIdentifiers( (TConfig) configProvider.apply( conf ), depth, fullyQualified );
    }

  @Override
  public long getSize( FlowProcess<? extends TConfig> flowProcess ) throws IOException
    {
    return getFileOriginal().getSize( (TConfig) processProvider.apply( flowProcess ) );
    }

  @Override
  public long getSize( TConfig conf ) throws IOException
    {
    return getFileOriginal().getSize( (TConfig) configProvider.apply( conf ) );
    }
  }
