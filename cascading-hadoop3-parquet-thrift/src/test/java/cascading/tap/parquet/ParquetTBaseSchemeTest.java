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

package cascading.tap.parquet;

import java.io.ByteArrayOutputStream;
import java.io.File;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop3.Hadoop3MRFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.parquet.hadoop.thrift.ThriftToParquetFileWriter;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.thrift.test.Name;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParquetTBaseSchemeTest
  {
  public static final String BUILD = System.getProperty( "test.output.root", "build/test/output" );

  final String txtInputPath = BUILD+"/../../../../cascading-hadoop3-parquet/src/test/resources/names.txt";
  final String parquetInputPath = BUILD+"/ParquetTBaseScheme/names-parquet-in";
  final String parquetOutputPath = BUILD+"/ParquetTBaseScheme/names-parquet-out";
  final String txtOutputPath = BUILD+"/ParquetTBaseScheme/names-txt-out";

  @Test
  public void testWrite() throws Exception
    {
    Path path = new Path( parquetOutputPath );
    JobConf jobConf = new JobConf();
    final FileSystem fs = path.getFileSystem( jobConf );
    if( fs.exists( path ) )
      fs.delete( path, true );

    Scheme sourceScheme = new TextLine( new Fields( "first", "last" ) );
    Tap source = new Hfs( sourceScheme, txtInputPath );

    Scheme sinkScheme = new ParquetTBaseScheme( Name.class );
    Tap sink = new Hfs( sinkScheme, parquetOutputPath );

    Pipe assembly = new Pipe( "namecp" );
    assembly = new Each( assembly, new PackThriftFunction() );
    Hadoop3MRFlowConnector hadoopFlowConnector = new Hadoop3MRFlowConnector();
    Flow flow = hadoopFlowConnector.connect( "namecp", source, sink, assembly );

    flow.complete();

    assertTrue( fs.exists( new Path( parquetOutputPath ) ) );
    assertTrue( fs.exists( new Path( parquetOutputPath + "/_metadata" ) ) );
    assertTrue( fs.exists( new Path( parquetOutputPath + "/_common_metadata" ) ) );
    }

  @Test
  public void testRead() throws Exception
    {
    doRead( new ParquetTBaseScheme( Name.class ) );
    }

  @Test
  public void testReadWithoutClass() throws Exception
    {
    doRead( new ParquetTBaseScheme() );
    }

  private void doRead( Scheme sourceScheme ) throws Exception
    {
    createFileForRead();

    Path path = new Path( txtOutputPath );
    final FileSystem fs = path.getFileSystem( new Configuration() );
    if( fs.exists( path ) )
      fs.delete( path, true );

    Tap source = new Hfs( sourceScheme, parquetInputPath );

    Scheme sinkScheme = new TextLine( new Fields( "first", "last" ) );
    Tap sink = new Hfs( sinkScheme, txtOutputPath );

    Pipe assembly = new Pipe( "namecp" );
    assembly = new Each( assembly, new UnpackThriftFunction() );
    Flow flow = new Hadoop3MRFlowConnector().connect( "namecp", source, sink, assembly );

    flow.complete();
    String result = FileUtils.readFileToString( new File( txtOutputPath + "/part-00000" ) );
    assertEquals( "Alice\tPractice\nBob\tHope\nCharlie\tHorse\n", result );
    }

  private void createFileForRead() throws Exception
    {
    final Path fileToCreate = new Path( parquetInputPath + "/names.parquet" );

    final Configuration conf = new Configuration();
    final FileSystem fs = fileToCreate.getFileSystem( conf );
    if( fs.exists( fileToCreate ) )
      fs.delete( fileToCreate, true );

    TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    TaskAttemptID taskId = new TaskAttemptID( "local", 0, true, 0, 0 );
    ThriftToParquetFileWriter w = new ThriftToParquetFileWriter( fileToCreate, ContextUtil.newTaskAttemptContext( conf, taskId ), protocolFactory, Name.class );

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final TProtocol protocol = protocolFactory.getProtocol( new TIOStreamTransport( baos ) );

    Name n1 = new Name();
    n1.setFirst_name( "Alice" );
    n1.setLast_name( "Practice" );
    Name n2 = new Name();
    n2.setFirst_name( "Bob" );
    n2.setLast_name( "Hope" );
    Name n3 = new Name();
    n3.setFirst_name( "Charlie" );
    n3.setLast_name( "Horse" );

    n1.write( protocol );
    w.write( new BytesWritable( baos.toByteArray() ) );
    baos.reset();
    n2.write( protocol );
    w.write( new BytesWritable( baos.toByteArray() ) );
    baos.reset();
    n3.write( protocol );
    w.write( new BytesWritable( baos.toByteArray() ) );
    w.close();
    }

  private static class PackThriftFunction extends BaseOperation implements Function
    {
    @Override
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      TupleEntry arguments = functionCall.getArguments();
      Tuple result = new Tuple();

      Name name = new Name();
      name.setFirst_name( arguments.getString( 0 ) );
      name.setLast_name( arguments.getString( 1 ) );

      result.add( name );
      functionCall.getOutputCollector().add( result );
      }
    }

  private static class UnpackThriftFunction extends BaseOperation implements Function
    {
    @Override
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      TupleEntry arguments = functionCall.getArguments();
      Tuple result = new Tuple();

      Name name = (Name) arguments.getObject( 0 );
      result.add( name.getFirst_name() );
      result.add( name.getLast_name() );
      functionCall.getOutputCollector().add( result );
      }
    }
  }
