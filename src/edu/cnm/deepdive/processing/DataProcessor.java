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

import edu.cnm.deepdive.source.DataProvider;

public abstract class DataProcessor {

  public static final String FINISH_PROCESSING_FORMAT =
      "%s.process() completed in %,d ms: geometric mean = %.2f%n";

  private final int[] data;

  public DataProcessor() {
    data = DataProvider.getInstance().getData();
  }

  public int[] getData() {
    return data;
  }

  public abstract void process() throws InterruptedException;

  public abstract double getGeometricMean();

  public void measureProcess() throws InterruptedException {
    long start = System.currentTimeMillis();
    process();
    System.out.printf(FINISH_PROCESSING_FORMAT,
        getClass().getSimpleName(), System.currentTimeMillis() - start, getGeometricMean());
  }

}
