package com.stolsvik.machinelearning.experiment;


import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.inputs.InputType;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A slightly more involved multilayered (MLP) applied to digit classification for the MNIST dataset (http://yann.lecun.com/exdb/mnist/).
 * <p>
 * This example uses two input layers and one hidden layer.
 * <p>
 * The first input layer has input dimension of numRows*numColumns where these variables indicate the
 * number of vertical and horizontal pixels in the image. This layer uses a rectified linear unit
 * (relu) activation function. The weights for this layer are initialized by using Xavier initialization
 * (https://prateekvjoshi.com/2016/03/29/understanding-xavier-initialization-in-deep-neural-networks/)
 * to avoid having a steep learning curve. This layer sends 500 output signals to the second layer.
 * <p>
 * The second input layer has input dimension of 500. This layer also uses a rectified linear unit
 * (relu) activation function. The weights for this layer are also initialized by using Xavier initialization
 * (https://prateekvjoshi.com/2016/03/29/understanding-xavier-initialization-in-deep-neural-networks/)
 * to avoid having a steep learning curve. This layer sends 100 output signals to the hidden layer.
 * <p>
 * The hidden layer has input dimensions of 100. These are fed from the second input layer. The weights
 * for this layer is also initialized using Xavier initialization. The activation function for this
 * layer is a softmax, which normalizes all the 10 outputs such that the normalized sums
 * add up to 1. The highest of these normalized values is picked as the predicted class.
 */
public class MLPMnistTwoLayerExample {

    private static Logger log = LoggerFactory.getLogger(MLPMnistTwoLayerExample.class);

    public static void main(String[] args) throws Exception {
        // number of rows and columns in the input pictures
        final int numInputs = 28 * 28;
        int outputNum = 10; // number of output classes
        int batchSize = 64; // batch size for each epoch
        int rngSeed = 123; // random number seed for reproducibility
        int numEpochs = 15; // number of epochs to perform
        double rate = 0.0015; // learning rate

        //Get the DataSetIterators:
        DataSetIterator mnistTrain = new MnistDataSetIterator(batchSize, true, rngSeed);
        DataSetIterator mnistTest = new MnistDataSetIterator(batchSize, false, rngSeed);

        int roundsPerEpoch = mnistTrain.numExamples() / batchSize;

        log.info("Build model....");
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(rngSeed) //include a random seed for reproducibility
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT) // use stochastic gradient descent as an optimization algorithm
                .iterations(1)
                .activation(Activation.RELU)
                .weightInit(WeightInit.XAVIER)
                .learningRate(rate) //specify the learning rate
                .updater(new Nesterovs(0.98))
                .regularization(true).l2(rate * 0.005) // regularize learning model
                .list()
                .layer(0, new DenseLayer.Builder() //create the first input layer.
                        .nOut(500)
                        .dropOut(0.5d)
                        .build())
                .layer(1, new DenseLayer.Builder() //create the second input layer
                        .nOut(500)
                        .dropOut(0.5d)
                        .build())
                .layer(2, new OutputLayer.Builder(LossFunction.NEGATIVELOGLIKELIHOOD) //create hidden layer
                        .activation(Activation.SOFTMAX)
                        .nOut(outputNum)
                        .build())
                .setInputType(InputType.feedForward(numInputs))
                .pretrain(false).backprop(true) //use backpropagation to adjust weights
                .build();

        MultiLayerNetwork model = new MultiLayerNetwork(conf);
        model.init();
        model.setListeners(new ScoreIterationListener(100));  //print the score with every iteration

        log.info("Train model.... numExamples:[" + mnistTrain.numExamples() + "], roundsPerEpoch:[" + roundsPerEpoch + "], epochs:[" + numEpochs + "].");
        for (int i = 0; i < numEpochs; i++) {
            mnistTrain.reset();
            log.info("Epoch " + i);
            for (int j = 0; j < roundsPerEpoch; j++) {
                DataSet featuresAndLabels = mnistTrain.next();
                model.fit(featuresAndLabels);
            }
        }


        log.info("Evaluate model....");
        Evaluation eval = new Evaluation(outputNum); //create an evaluation object with 10 possible classes
        while (mnistTest.hasNext()) {
            DataSet next = mnistTest.next();
            INDArray output = model.output(next.getFeatureMatrix()); //get the networks prediction
            eval.eval(next.getLabels(), output); //check the prediction against the true class
        }

        log.info(eval.stats());
        log.info("****************Example finished********************");

    }

}
