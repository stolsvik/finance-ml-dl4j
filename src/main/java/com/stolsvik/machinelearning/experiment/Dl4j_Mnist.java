package com.stolsvik.machinelearning.experiment;

import com.stolsvik.machinelearning.experiment.mnist.MnistImages;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.api.Updater;
import org.deeplearning4j.nn.conf.LearningRatePolicy;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.inputs.InputType;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.DropoutLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.AdaMax;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Dl4j_Mnist {

    private static final Logger log = LoggerFactory.getLogger(Dl4j_Mnist.class);

    public static void main(String[] args) throws Exception {
        // number of rows and columns in the input pictures
        final int numInputs = 28 * 28;

        // Output: 10 classes: digits 0-9
        int numOutputs = 10;

        int randomSeed = 123;
        double learningRate = 0.005d;  // Started with 0.005.
        int miniBatchSize = 128;
        int epochs = 30;

        MnistImages trainingImages = MnistImages.getTraining().randomize();
        MnistImages testImages = MnistImages.getTest();

        int roundsPerEpoch = trainingImages.getSize() / miniBatchSize;

//        Adam updater = new Adam();
//        updater.setLearningRate(5d);

        // learning rate schedule in the form of <Iteration #, Learning Rate>
        Map<Integer, Double> lrSchedule = new HashMap<>();
        lrSchedule.put(0, 0.05);
        lrSchedule.put(roundsPerEpoch * 5, 0.01);
        lrSchedule.put(roundsPerEpoch * 10, 0.005);
        lrSchedule.put(roundsPerEpoch * 15, 0.001);
        lrSchedule.put(roundsPerEpoch * 20, 0.0005);
        lrSchedule.put(roundsPerEpoch * 25, 0.0001);

        log.info("Build model....");
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(randomSeed) //include a random seed for reproducibility
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT) // use stochastic gradient descent as an optimization algorithm
                // .learningRate(learningRate) //specify the learning rate
                .learningRateDecayPolicy(LearningRatePolicy.Schedule)
                .learningRateSchedule(lrSchedule)
                .updater(new Nesterovs.Builder().momentum(0.98d).build())
                .regularization(true)
                //.l1(0.0001)
                .l2(0.01)
                .iterations(1)
                .activation(Activation.RELU)
                .weightInit(WeightInit.XAVIER)
                .list()
                // .layer(0, new DropoutLayer.Builder().build())
                .layer(0, new DenseLayer.Builder().nOut(1000).build())
                .layer(1, new DenseLayer.Builder().nOut(1000).build())
                .layer(2, new DenseLayer.Builder().nOut(1000).build())
                .layer(3, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD) //create hidden layer
                        .activation(Activation.SOFTMAX)
                        .nOut(numOutputs)
                        .build())
                .setInputType(InputType.feedForward(numInputs))
                .build();

        MultiLayerNetwork model = new MultiLayerNetwork(conf);
        model.init();

        model.setListeners(new ScoreIterationListener(100));

        for (int i = 0; i < epochs; i++) {
            log.info("Epoch "+i);
            for (int j = 0; j < roundsPerEpoch; j++) {
                DataSet featuresAndLabels = getNextFeatureAndLabelDataSet(trainingImages, miniBatchSize);
                model.fit(featuresAndLabels);
            }
        }

        System.out.println("Evaluate model....");
        Evaluation eval = new Evaluation(numOutputs);

        // Evaluate
        DataSet t = getNextFeatureAndLabelDataSet(testImages, testImages.getSize());
        INDArray features = t.getFeatureMatrix();
        INDArray labels = t.getLabels();
        INDArray predicted = model.output(features, false);
        eval.eval(labels, predicted);

        // Print the evaluation statistics
        System.out.println(eval.stats());
    }

    private static DataSet getNextFeatureAndLabelDataSet(MnistImages mnistImages, int miniBatchSize) {
        double[][] javaFeatureMatrix = new double[miniBatchSize][];
        double[][] javaLabelMatrix = new double[miniBatchSize][];
        for (int i = 0; i < miniBatchSize; i++) {
            double[][] featuresAndLabels = mnistImages.getNextImageWithLabels();
            javaFeatureMatrix[i] = featuresAndLabels[0];
            javaLabelMatrix[i] = featuresAndLabels[1];
        }
        return new DataSet(Nd4j.create(javaFeatureMatrix), Nd4j.create(javaLabelMatrix));
    }

}
