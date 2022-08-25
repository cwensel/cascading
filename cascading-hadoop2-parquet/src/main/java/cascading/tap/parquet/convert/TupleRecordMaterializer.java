/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.tap.parquet.convert;

import cascading.tuple.Tuple;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;

public class TupleRecordMaterializer extends RecordMaterializer<Tuple>
  {

  private TupleConverter root;

  public TupleRecordMaterializer( GroupType parquetSchema )
    {
    this.root = new TupleConverter( parquetSchema );
    }

  @Override
  public Tuple getCurrentRecord()
    {
    return root.getCurrentTuple();
    }

  @Override
  public GroupConverter getRootConverter()
    {
    return root;
    }

  }
