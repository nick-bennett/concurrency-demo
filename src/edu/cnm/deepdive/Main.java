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
package edu.cnm.deepdive;

import edu.cnm.deepdive.processing.DataProcessor;
import edu.cnm.deepdive.processing.MultiThreadedWithCriticalSection;
import edu.cnm.deepdive.processing.MultiThreadedWithForkJoin;
import edu.cnm.deepdive.processing.MultiThreadedWithRaceCondition;
import edu.cnm.deepdive.processing.MultiThreadedWithReduction;
import edu.cnm.deepdive.processing.SingleThreaded;

public class Main {

  public static void main(String[] args) throws InterruptedException {
    DataProcessor processor;
    processor = new SingleThreaded();
    processor.measureProcess();
    processor = new MultiThreadedWithRaceCondition();
    processor.measureProcess();
    processor = new MultiThreadedWithCriticalSection();
    processor.measureProcess();
    processor = new MultiThreadedWithReduction();
    processor.measureProcess();
    processor = new MultiThreadedWithForkJoin();
    processor.measureProcess();
  }

}
