package com.sparkProject

import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator


/**
  * Created by bertrrandBertrand on 27/10/2016.
  */
object JobML {

  def main(args: Array[String]): Unit = {

    // SparkSession configuration
    val spark = SparkSession
      .builder
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // load csv file in a dataFrame
    val df = spark
      .read
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("comment", "#")
      .csv("/Users/bertrrandBertrand/Desktop/MS_BIG_DATA/Spark/data/part-r-00000-ef17869c-3b36-48e2-a711-eff4e2028790.csv")

    println("#################### Mise en forme des données ####################")

    // drop rowid and separate labels and features
    val df_features = df.drop("rowid", "koi_disposition")

    // assemble features into one column of tuples
    val assembler = new VectorAssembler()
      .setInputCols(df_features.columns)
      .setOutputCol("features")

    // transform the output dataframe to use the VectorAssembler
    val output = assembler.transform(df)
      .select("features", "koi_disposition")

    //println(output.select("features", "koi_disposition").head())
    println("Display schema of the df using vectorAssembler")
    output.printSchema()

    // Now, index the labels
    val indexer = new StringIndexer()
      .setInputCol("koi_disposition")
      .setOutputCol("label")
      .fit(output)
    // apply to the dataset
    val indexed = indexer.transform(output)

    println("#################### Machine Learning ####################")

    // Split data into training (90%) and test (10%).
    val splits = indexed.randomSplit(Array(0.9, 0.1))
    val training = splits(0).cache() // mise en cache du training set
    val test = splits(1)

    // set logistic regression hyper-parameters
    val lr = new LogisticRegression()
      .setElasticNetParam(1.0)  // L1-norm regularization : LASSO
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setStandardization(true)  // to scale each feature of the model ("centrer & réduire")
      .setFitIntercept(true)  // we want an affine regression (with false, it is a linear regression)
      .setTol(1.0e-5)  // stop criterion of the algorithm based on its convergence
      .setMaxIter(300)  // a security stop criterion to avoid infinite loops

    // parameters for the Search Grid
    val array = -6.0 to (0.0, 0.5) toArray
    val arrayLog = array.map(x => math.pow(10,x))

    // penalties of features and other hyper-parameters are defined by the following grid search
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, arrayLog)
      .build()

    // Use a BinaryClassificationEvaluator
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")

    // Split data into training and test data
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.7)

    // test models hyper-parameters and choose the best configuration
    val lrModel = trainValidationSplit.fit(training)

    val df_WithPredictions = lrModel.transform(test).select("features", "label", "prediction")
    df_WithPredictions.show()
    df_WithPredictions.groupBy("label", "prediction").count.show()

    // Score with the best hyper-parameters
    evaluator.setRawPredictionCol("prediction")
    println(evaluator.evaluate(df_WithPredictions))

    // Saving the model
    lrModel.write.overwrite().save("/Users/bertrrandBertrand/Desktop/modelLR.csv")

  }

}
