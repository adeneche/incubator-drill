/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.vector.complex.writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.fn.JsonWriter;
import org.apache.drill.exec.vector.complex.impl.ComplexWriterImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Charsets;

public class TestRepeated {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestRepeated.class);

  private static BufferAllocator allocator;

  private final PrintStream dummystream = new PrintStream(new OutputStream() {
    @Override
    public void write(int b) throws IOException {
      // NO OP
    }
  });

  @BeforeClass
  public static void setupAllocator(){
    allocator = new TopLevelAllocator();
  }

  @AfterClass
  public static void destroyAllocator(){
    allocator.close();
  }
//
//  @Test
//  public void repeatedMap(){
//
//    /**
//     * We're going to try to create an object that looks like:
//     *
//     *  {
//     *    a: [
//     *      {x: 1, y: 2}
//     *      {x: 2, y: 1}
//     *    ]
//     *  }
//     *
//     */
//    MapVector v = new MapVector("", allocator);
//    ComplexWriter writer = new ComplexWriterImpl("col", v);
//
//    MapWriter map = writer.rootAsMap();
//
//    map.start();
//    ListWriter list = map.list("a");
//    MapWriter inner = list.map();
//
//    IntHolder holder = new IntHolder();
//    IntWriter xCol = inner.integer("x");
//    IntWriter yCol = inner.integer("y");
//
//    inner.start();
//
//    holder.value = 1;
//    xCol.write(holder);
//    holder.value = 2;
//    yCol.write(holder);
//
//    inner.end();
//
//    inner.start();
//
//    holder.value = 2;
//    xCol.write(holder);
//    holder.value = 1;
//    yCol.write(holder);
//
//    inner.end();
//
//    IntWriter numCol = map.integer("nums");
//    holder.value = 14;
//    numCol.write(holder);
//
//    map.end();
//
//
//    assert writer.ok();
//
//    dummyStream.println(v.getAccessor().getObject(0));
//
//  }

  @Test
  public void listOfList() throws IOException{
    /**
     * We're going to try to create an object that looks like:
     *
     *  {
     *    a: [
     *      [1,2,3],
     *      [2,3,4]
     *    ],
     *    nums: 14,
     *    b: [
     *      { c: 1 },
     *      { c: 2 , x: 15}
     *    ]
     *  }
     *
     */

    MapVector v = new MapVector("", allocator, null);
    ComplexWriterImpl writer = new ComplexWriterImpl("col", v);
    writer.allocate();

    {
      MapWriter map = writer.rootAsMap();
      ListWriter list = map.list("a");
      list.start();

      ListWriter innerList = list.list();
      IntWriter innerInt = innerList.integer();

      innerList.start();

      IntHolder holder = new IntHolder();

      holder.value = 1;
      innerInt.write(holder);
      holder.value = 2;
      innerInt.write(holder);
      holder.value = 3;
      innerInt.write(holder);

      innerList.end();
      innerList.start();

      holder.value = 4;
      innerInt.write(holder);
      holder.value = 5;
      innerInt.write(holder);

      innerList.end();
      list.end();

      IntWriter numCol = map.integer("nums");
      holder.value = 14;
      numCol.write(holder);

      MapWriter repeatedMap = map.list("b").map();
      repeatedMap.start();
      holder.value = 1;
      repeatedMap.integer("c").write(holder);
      repeatedMap.end();

      repeatedMap.start();
      holder.value = 2;
      repeatedMap.integer("c").write(holder);
      BigIntHolder h = new BigIntHolder();
      h.value = 15;
      repeatedMap.bigInt("x").write(h);
      repeatedMap.end();

      map.end();
    }

    {
      writer.setPosition(1);

      MapWriter map = writer.rootAsMap();
      ListWriter list = map.list("a");
      list.start();

      ListWriter innerList = list.list();
      IntWriter innerInt = innerList.integer();

      innerList.start();

      IntHolder holder = new IntHolder();

      holder.value = -1;
      innerInt.write(holder);
      holder.value = -2;
      innerInt.write(holder);
      holder.value = -3;
      innerInt.write(holder);

      innerList.end();
      innerList.start();

      holder.value = -4;
      innerInt.write(holder);
      holder.value = -5;
      innerInt.write(holder);

      innerList.end();
      list.end();

      IntWriter numCol = map.integer("nums");
      holder.value = -28;
      numCol.write(holder);

      MapWriter repeatedMap = map.list("b").map();
      repeatedMap.start();
      holder.value = -1;
      repeatedMap.integer("c").write(holder);
      repeatedMap.end();

      repeatedMap.start();
      holder.value = -2;
      repeatedMap.integer("c").write(holder);
      BigIntHolder h = new BigIntHolder();
      h.value = -30;
      repeatedMap.bigInt("x").write(h);
      repeatedMap.end();

      map.end();
    }


    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

    dummystream.println("Map of Object[0]: " + ow.writeValueAsString(v.getAccessor().getObject(0)));
    dummystream.println("Map of Object[1]: " + ow.writeValueAsString(v.getAccessor().getObject(1)));


    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    JsonWriter jsonWriter = new JsonWriter(stream, true, true);
    FieldReader reader = v.getChild("col", MapVector.class).getReader();
    reader.setPosition(0);
    jsonWriter.write(reader);
    reader.setPosition(1);
    jsonWriter.write(reader);
    dummystream.print("Json Read: ");
    dummystream.println(new String(stream.toByteArray(), Charsets.UTF_8));

    writer.clear();


  }
}
