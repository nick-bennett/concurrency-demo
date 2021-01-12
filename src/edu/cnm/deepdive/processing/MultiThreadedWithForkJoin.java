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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class MultiThreadedWithForkJoin extends DataProcessor {

  private static final int FORK_THRESHOLD = 10_000;

  private final ForkJoinPool pool;

  private double logSum;

  public MultiThreadedWithForkJoin() {
    pool = new ForkJoinPool();
  }

  @Override
  public double getGeometricMean() {
    return Math.exp(logSum / getData().length);
  }

  @Override
  public void process() throws InterruptedException {
    int[] data = getData();
    Task task = new Task(data, 0, data.length);
    logSum = task.compute();
  }

  private static class Task extends RecursiveTask<Double> {

    private final int[] data;
    private final int start;
    private final int end;

    private Task(int[] data, int start, int end) {
      this.data = data;
      this.start = start;
      this.end = end;
    }

    @Override
    protected Double compute() {
      double sum;
      if (end - start < FORK_THRESHOLD) {
        sum = 0;
        for (int j = start; j < end; j++) {
          sum += Math.log(data[j]);
        }
      } else {
        int midpoint = (start + end) / 2;
        Task left = new Task(data, start, midpoint);
        left.fork();
        Task right = new Task(data, midpoint, end);
        sum = right.compute() + left.join();
      }
      return sum;
    }

  }

}
