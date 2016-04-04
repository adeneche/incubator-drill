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
package org.apache.drill.jdbc.test;

import org.apache.drill.exec.ExecTest;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a reproduction for DRILL-3763:<br>
 * <ul>
 *   <li>Start a drillbit</li>
 *   <li>Create 10 connections to the drillbit and run the same query on each connection</li>
 *   <li>Create another connection and close it as soon as any of the previous connections starts receiving results</li>
 *   <li>Without the fix, some of the connections will fail with a ChannelClosedException</li>
 * </ul>
 */
public class TestDrill3763 extends ExecTest {

  private static final int NUM_QUERIES = 10;
  private CountDownLatch done = new CountDownLatch(NUM_QUERIES + 1);
  private CountDownLatch dataReceived = new CountDownLatch(1);
  private AtomicInteger succeeded = new AtomicInteger(0);

  /**
   * Connect to the drillbit and run a specific query
   * @param query query that will be executed
   */
  private void connectAndRunQuery(final String query) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          final Connection conn = DriverManager.getConnection("jdbc:drill:drillbit=localhost:31010");
          final Statement stmt = conn.createStatement();
          final ResultSet rs = stmt.executeQuery(query);

          boolean first = true;
          while (rs.next()) {
            if (first) {
              dataReceived.countDown();
              first = false;
            }
          }
          succeeded.incrementAndGet();

          rs.close();
          stmt.close();
          conn.close();
        } catch (Exception e) {
          // no op
        } finally {
          dataReceived.countDown();
          done.countDown();
        }
      }
    }).start();
  }

  /**
   * Connect to the drillbit then close the connection as soon as any of the other queries
   * starts receiving data
   */
  private void connectThenClose() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          final Connection conn = DriverManager.getConnection("jdbc:drill:drillbit=localhost:31010");

          dataReceived.await();

          conn.close();
        } catch (Exception e) {
          // no op
        } finally {
          done.countDown();
        }
      }
    }).start();
  }

  @Test
  public void testFixChannelClosed() throws Exception {
    Class.forName("org.apache.drill.jdbc.Driver").newInstance();

    // start an embedded drillbit
    final Connection connection = DriverManager.getConnection("jdbc:drill:zk=local");

    connectThenClose();
    for (int i = 0; i < NUM_QUERIES; i++) {
      connectAndRunQuery("SELECT * FROM cp.`employee.json`");
    }

    done.await(); // wait for all connections to close first
    connection.close(); // stop the drillbit

    // all NUM_QUERIES queries should have succeeded
    Assert.assertEquals("Not all queries succeeded", NUM_QUERIES, succeeded.get());
  }
}
