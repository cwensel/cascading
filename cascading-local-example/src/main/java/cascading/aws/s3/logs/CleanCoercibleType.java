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

package cascading.aws.s3.logs;

import java.lang.reflect.Type;
import java.util.function.Function;

import cascading.tuple.type.CoercibleType;

/**
 *
 */
public class CleanCoercibleType<Canonical> implements CoercibleType<Canonical>
  {
  CoercibleType<Canonical> coercibleType;
  Function<Object, Object> function;

  public CleanCoercibleType( CoercibleType<Canonical> coercibleType, Function<Object, Object> function )
    {
    this.coercibleType = coercibleType;
    this.function = function;
    }

  @Override
  public Class<Canonical> getCanonicalType()
    {
    return coercibleType.getCanonicalType();
    }

  @Override
  public Canonical canonical( Object value )
    {
    return coercibleType.canonical( function.apply( value ) );
    }

  @Override
  public <Coerce> Coerce coerce( Object value, Type to )
    {
    return coercibleType.coerce( value, to );
    }
  }
