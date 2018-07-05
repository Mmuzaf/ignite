/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.ml.tree.boosting;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.boosting.GDBBinaryClassifierOnTreesTrainer;
import org.apache.ignite.thread.IgniteThread;

/**
 * Example represents a solution for the task of classification learning based on
 * Gradient Boosting on trees implementation. It shows an initialization of {@link org.apache.ignite.ml.tree.boosting.GDBBinaryClassifierOnTreesTrainer},
 * initialization of Ignite Cache, learning step and comparing of predicted and real values.
 *
 * In this example dataset is creating automatically by meander function f(x) = [sin(x) > 0].
 */
public class GDBOnTreesClassificationTrainerExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String... args) throws InterruptedException {
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                GDBBinaryClassifierOnTreesTrainer.class.getSimpleName(), () -> {

                // Create cache with training data.
                CacheConfiguration<Integer, double[]> trainingSetCfg = new CacheConfiguration<>();
                trainingSetCfg.setName("TRAINING_SET");
                trainingSetCfg.setAffinity(new RendezvousAffinityFunction(false, 10));

                IgniteCache<Integer, double[]> trainingSet = ignite.createCache(trainingSetCfg);
                for(int i = -50; i <= 50; i++) {
                    double x = ((double)i) / 10.0;
                    double y = Math.sin(x) < 0 ? 0.0 : 1.0;
                    trainingSet.put(i, new double[] {x, y});
                }

                // Create regression trainer.
                DatasetTrainer<Model<Vector, Double>, Double> trainer = new GDBBinaryClassifierOnTreesTrainer(1.0, 300, 2, 0.);

                // Train decision tree model.
                Model<Vector, Double> mdl = trainer.fit(
                    ignite,
                    trainingSet,
                    (k, v) -> new double[] { v[0] },
                    (k, v) -> v[1]
                );

                System.out.println(">>> ---------------------------------");
                System.out.println(">>> | Prediction\t| Valid answer\t|");
                System.out.println(">>> ---------------------------------");

                // Calculate score.
                for (int x = -5; x < 5; x++) {
                    double predicted = mdl.apply(VectorUtils.of(x));

                    System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", predicted, Math.sin(x) < 0 ? 0.0 : 1.0);
                }

                System.out.println(">>> ---------------------------------");

                System.out.println(">>> GDB classification trainer example completed.");
            });

            igniteThread.start();

            igniteThread.join();
        }
    }
}
