/*
 *  Copyright 2021 CNM Ingenuity, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.cnm.deepdive.processing;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiThreadedWithReduction extends DataProcessor {

  private static final int NUM_TASKS = Runtime.getRuntime().availableProcessors();

  private final Object lock = new Object();

  private double logSum;

  @Override
  public double getGeometricMean() {
    return Math.exp(logSum / getData().length);
  }

  @Override
  public void process() throws InterruptedException {
    logSum = 0;
    int[] data = getData();
    int length = data.length;
    ExecutorService pool = Executors.newFixedThreadPool(NUM_TASKS);
    CountDownLatch latch = new CountDownLatch(NUM_TASKS);
    for (int i = 0; i < NUM_TASKS; i++) {
      int start = i * length / NUM_TASKS;
      int end = (i + 1) * length / NUM_TASKS;
      pool.submit(() -> {
        double sum = 0;
        for (int j = start; j < end; j++) {
          sum += Math.log(data[j]);
        }
        synchronized (lock) {
          logSum += sum;
        }
        latch.countDown();
      });
    }
    latch.await();
    pool.shutdown();
  }

}
