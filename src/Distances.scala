import org.apache.spark.sql.functions.{abs, col, lit, mean, monotonically_increasing_id, stddev, udf}
import org.apache.spark.sql.types.{NumericType, StringType}
import org.apache.spark.sql.utils.FindJoinUtils.getColumnsNamesMetaFeatures
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.Map
import scala.collection.parallel.ParSeq
import scala.math.max

object Distances {

  def computeDistances(dfSource: DataFrame, dfCandidates: Seq[DataFrame]):  (Dataset[_],
    Dataset[_], Dataset[_], Dataset[_]) ={
    var (datasetMetaFeatures, numericMetaFeatures, nominalMetaFeatures) = dfSource.metaFeatures

    val fileName = dfSource.inputFiles(0).split("/").last

    // compute metaFeatures for all datasets and merge them in two dataframes: nominal and numeric
    for (i <- 0 to dfCandidates.size-1) {
      var (datasetMetaFeaturesTmp, numericMetaFeaturesTmp, nominalMetaFeaturesTmp) = dfCandidates(i).metaFeatures
      nominalMetaFeatures = nominalMetaFeatures.union(nominalMetaFeaturesTmp)
      numericMetaFeatures = numericMetaFeatures.union(numericMetaFeaturesTmp)
    }

    // normalization using z-score
    val nomZscore = normalize(nominalMetaFeatures, "nominal")
    val numZscore = normalize(numericMetaFeatures, "numeric")

    nomZscore.show

    // Get the attributes names and meta-features names
    // NOTE: This one differs from the source code in Spark since we got the names through logicalPlan().output instead of schema
    // I tried to use schema() in the source code but sometimes the attributes names are not correct since were modified
    // by transformations and at some point schema() does not have the right names, I believe this only happens inside the source code
    val attributesNominal = dfSource.schema.filter(
      a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
    val attributesNumeric = dfSource.schema.filter(
      a => a.dataType.isInstanceOf[NumericType]).map(a => a.name)

    //val columnsMetaFeaturesNominal = nomZscore.schema.map(_.name)
    //val columnsMetaFeaturesNumeric = numZscore.schema.map(_.name)

    val matchingNom = createPairs(attributesNominal, getColumnsNamesMetaFeatures("nominal"), nomZscore, fileName)
    val matchingNum = createPairs(attributesNumeric, getColumnsNamesMetaFeatures("numeric"), numZscore, fileName)

    (nomZscore, numZscore, matchingNom, matchingNum)
  }

  lazy val editDist = udf(levenshtein(_: String, _: String): Double)

  def createPairs(attributes: Seq[String], metaFeaturesNames: Seq[String],
                  zScoreDF: Dataset[_], fileName: String): Dataset[_] = {

    val columns = zScoreDF.schema.map(_.name)
    //rename all columns from datasources appending _2
    // Example: val_size_max to val_size_max_2
    val nomAttCandidates = zScoreDF.filter(col("ds_name") =!= fileName)
      .select(columns.map(x => col(x).as(s"${x}_2")): _*).cache()

    var attributesPairs: DataFrame = null

    var flag = true
    // select the normalized meta-features from the dataset source
    val nomSourceAtt = zScoreDF.filter(col("ds_name") === fileName).cache()

    // for each attribute we perform a cross join and compute distances from the pairs
    for (att <- attributes) {

      // select the normalized meta-features from one attribute from the dataset source and
      // perform a cross join with the attributes from other datasets
      var attributesPairsTmp = nomSourceAtt.filter(col("att_name") === att).crossJoin(nomAttCandidates)

      // compute the distances for each meta-feature of two attributes meta-features and save the value in the same metafeature column
      for (metafeature <- metaFeaturesNames) {
        attributesPairsTmp = attributesPairsTmp.withColumn(metafeature, abs(col(metafeature) - col(s"${metafeature}_2")))
      }
      // compute the distance name from the two attributes pair
      attributesPairsTmp = attributesPairsTmp.withColumn(
        "name_dist", editDist(col("att_name"), col("att_name_2")))

      // remove the meta-features columns from the second attribute pair
      attributesPairsTmp = attributesPairsTmp.drop(metaFeaturesNames.map(x => s"${x}_2"): _*)
      if (flag) {
        attributesPairs = attributesPairsTmp
        flag = false
      } else {
        attributesPairs = attributesPairs.union(attributesPairsTmp)
      }
    }

    attributesPairs
  }




  def normalize(metaFeatures: Dataset[_], metaType: String): DataFrame = {
    // get the meta-features names for the specified type
    val metaFeaturesNames = getColumnsNamesMetaFeatures(metaType)

    // compute mean and standard deviation for each meta-feature.
    // the map() will produce a Sequence of dataFrames having the mean and std for each meta-feature
    // we reduce the Sequence to have one dataframe with all the columns and select the first row since we know the
    // mean and stdv produce just one value which will be in the first row
    val meanAndStd = metaFeaturesNames.map(x => metaFeatures.select(
        mean(col(x)).as(s"${x}_avg"),
        stddev(col(x)).as(s"${x}_std")
      )
      .withColumn("id", monotonically_increasing_id))
      .reduce(_.join(_, "id")).take(1).head

    var zScoreDF = metaFeatures
    // we replace the value for each meta-feature by the z-score using the withColumn function
    for (metaFeature <- metaFeaturesNames) {
      zScoreDF = zScoreDF.withColumn(
        metaFeature,
        (col(metaFeature) - lit(meanAndStd.getAs(s"${metaFeature}_avg"))) /  lit(meanAndStd.getAs(s"${metaFeature}_std")))
    }

    val dataF = zScoreDF.toDF()
    // forcing cache of the distances
    metaFeatures.sparkSession.createDataFrame(dataF.rdd, dataF.schema).cache()

  }

  def levenshtein(s1: String, s2: String): Double = {
    val memorizedCosts = Map[(Int, Int), Int]()

    def lev: ((Int, Int)) => Int = {
      case (k1, k2) =>
        memorizedCosts.getOrElseUpdate((k1, k2), (k1, k2) match {
          case (i, 0) => i
          case (0, j) => j
          case (i, j) =>
            ParSeq(1 + lev((i - 1, j)),
              1 + lev((i, j - 1)),
              lev((i - 1, j - 1))
                + (if (s1(i - 1) != s2(j - 1)) 1 else 0)).min
        })
    }

    val leve = lev((s1.length, s2.length))
    leve/max(s1.length, s2.length).toDouble
  }


  val spark = SparkSession.builder.appName("SparkDistances")
    .master("local[*]")
    //.config("spark.driver.bindAddress","127.0.0.1")
    .config("spark.driver.maxResultSize","7g")
    .getOrCreate()

  def main(args: Array[String]): Unit = {


    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val path = "./resources/csv"
    val datasetSource = "countries_and_continents.csv"
    val datasets = Seq("ipums_la_98-small.csv","netflix_titles.csv","wikipedia-iso-country-codes.csv","movies.csv"
      ,"whr2015.csv","who_suicide_statistics.csv","cwurData.csv","Netflix_Shows.csv","Word_University_Rank_2020.csv"
      ,"phpRiBb0c.csv","edu.csv","fifa_ranking.csv","gender_development.csv","countries_of_the_world.csv"
      ,"dataset_1_anneal.csv","World_countries_env_vars.csv","song_data.csv","movie_metadata.csv","plasma_retinol.csv"
      ,"billboard_lyrics_1964-2015.csv")



//    val allDatasets = datasetSource +: datasets
//    computingMetaFeatures(allDatasets,path)

    val dsSource = spark.read.option("header", "true").option("inferSchema", "true").csv(s"${path}/${datasetSource}")
    var dsCandidates:Seq[DataFrame] = Nil
    // reading all datasets into a sequence of dataframes
    for(name <-  datasets){
      val ds = spark.read.option("header", "true").option("inferSchema", "true").csv(s"${path}/${name}")
      // appending dataframe to the sequence
      dsCandidates = dsCandidates :+ ds
    }
    val (nomZscore, numZscore, pairsNominal, pairsNumeric) = computeDistances(dsSource,dsCandidates)

    pairsNominal.write.mode("overwrite").option("header","true")
      .csv(s"${path}/resources/distances/countriesPairs.csv")

    spark.stop()
  }


//  def computingMetaFeatures(allDatasets:Seq[String], path:String): Unit ={
//    var timesExec:Seq[(String,String)] = Nil
//    for(name <-  allDatasets){
//      val t1 = System.nanoTime
//
//      val ds = spark.read.option("header", "true").option("inferSchema", "true").csv(s"${path}/${name}")
//      ds.metaFeatures
//
//      val duration = (System.nanoTime - t1) / 1e9d
//      val minutes = duration / 60d
//      timesExec = timesExec :+ (name -> s"${duration},${minutes},${ds.count()},${ds.schema.size}")
//      println(s"${name}:  ${duration} seconds")
//    }
//
//    println(timesExec)
//  }
}
