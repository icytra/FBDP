import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
//
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
object Main {
  //定义函数，读取csv文件
  def readCSV(filePath: String): DataFrame = {
    // 创建SparkSession
    val spark = SparkSession.builder.appName("Spark").master("local[*]").getOrCreate()

    // 读取CSV文件
    val df = spark.read
      .format("csv")
      .option("header", "true") // 如果CSV文件包含标题行，则设置为true
      .option("inferSchema", "true")
      .load(filePath)
    df
  }


  def Task1_1(df: DataFrame): DataFrame = {
    // 将AMT_CREDIT列转换为Double类型
    val dfWithAmount = df.withColumn("AMT_CREDIT", col("AMT_CREDIT").cast("Double"))

    // 统计 AMT_CREDIT 的分布情况
    val creditDistribution = dfWithAmount.groupBy(
        floor(col("AMT_CREDIT") / 10000) * 10000 as "credit_range"
      )
      .count()
      .orderBy("credit_range")
    //creditDistribution添加一列，计算每个区间的上界
    val creditDistribution1 = creditDistribution.withColumn("credit_range_upper",col("credit_range")+10000)
    //count列和credit_range_upper列交换位置
    val df1 = creditDistribution1.select("credit_range","credit_range_upper","count")
    // 显示结果
    df1.show(false)
    //将其写入csv文件,模式设置为overwrite,保存列名
    df1.write.mode(SaveMode.Overwrite).option("header", "true").csv("E:\\IDEA_Project\\exp4\\src\\output\\Task1_1")
    df1
  }


  def Task1_2(df: DataFrame): DataFrame = {
    //选取所需列
    val df1 = df.select("SK_ID_CURR","NAME_CONTRACT_TYPE","AMT_CREDIT","AMT_INCOME_TOTAL")
    //添加一列：AMT_CREDIT-AMT_INCOME_TOTAL
    val df2 = df1.withColumn("差值",col("AMT_CREDIT")-col("AMT_INCOME_TOTAL"))
    //选取差值最大和最小的10行
    val df3 = df2.orderBy(col("差值").desc).limit(10)
    val df4 = df2.orderBy(col("差值").asc).limit(10)
    //将两个DataFrame合并
    val df5 = df3.union(df4)
    df5.show(false)
    //保存结果
    df5.write.mode(SaveMode.Overwrite).option("header", "true").csv("E:\\IDEA_Project\\exp4\\src\\output\\Task1_2")
    df5

  }
  def Task2_1(df: DataFrame): DataFrame = {
    //统计所有男性客户（CODE_GENDER = M）的 ⼩ 孩个数（CNT_CHILDREN）类型占⽐情况
    val df1 = df.select("CODE_GENDER","CNT_CHILDREN")
    //选取df1中CODE_GENDER = M的行
    val df2 = df1.filter(col("CODE_GENDER") === "M")
    df2.show(false)
    //计算全部男性个数，也就是df2的行数
    val male_count = df2.count()
    //df2根据CNT_CHILDREN分组
    val df3 = df2.groupBy("CNT_CHILDREN").count()
    //CNT_CHILDREN转化成Int类型
    val df4 = df3.withColumn("CNT_CHILDREN",col("CNT_CHILDREN").cast("Int"))
    //根据CNT_CHILDREN升序排列
    val df5 = df4.orderBy(col("CNT_CHILDREN").asc)
    df5.show(false)
    //计算每个CNT_CHILDREN的占比
    val df6 = df5.withColumn("rate",col("count")/male_count)
    df6.show(false)
    df6.write.mode(SaveMode.Overwrite).option("header", "true").csv("E:\\IDEA_Project\\exp4\\src\\output\\Task2_1")
    df6
  }
  def Task2_2(df: DataFrame): DataFrame = {
    //选取所需列
    val df1 = df.select("SK_ID_CURR", "AMT_INCOME_TOTAL", "DAYS_BIRTH")
    //添加一列：AMT_INCOME_TOTAL/DAYS_BIRTH
    val df2 = df1.withColumn("avg_income", col("AMT_INCOME_TOTAL").cast("Double") / (-col("DAYS_BIRTH").cast("Double")))
    //筛选出avg_income大于1的行并降序排列
    val df3 = df2.filter(col("avg_income") > 1).orderBy(col("avg_income").desc)
    df3.show(false)
    df3
  }
  def Task3_get_preprocessed_df(df: DataFrame): DataFrame = {
    // 将所需的string类型转换为数值类
    val columnsToIndex = Array("NAME_CONTRACT_TYPE", "CODE_GENDER", "FLAG_OWN_CAR", "FLAG_OWN_REALTY", "TARGET") // 列名数组，你希望转换的列
    val columnsToIndex_output = Array("NAME_CONTRACT_TYPE_index", "CODE_GENDER_index", "FLAG_OWN_CAR_index", "FLAG_OWN_REALTY_index", "label")
    val indexedColumns = columnsToIndex.zip(columnsToIndex_output).map { case (colName, output) =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(s"${output}")
    }
    val indexedDF = indexedColumns.foldLeft(df) { (accDF, indexer) =>
      indexer.fit(accDF).transform(accDF)
    }
    // 构建特征向量
    val assembler = new VectorAssembler()
      .setInputCols(Array("CNT_CHILDREN", "AMT_INCOME_TOTAL", "AMT_CREDIT", "NAME_CONTRACT_TYPE_index", "DAYS_BIRTH", "DAYS_EMPLOYED", "DAYS_REGISTRATION", "DAYS_ID_PUBLISH", "CODE_GENDER_index",
        "FLAG_OWN_CAR_index", "FLAG_OWN_REALTY_index"))
      .setOutputCol("features")
    val assembledDF = assembler.transform(indexedDF)
    //打印features和label列
    assembledDF.select("features", "label").show(false)
    assembledDF
  }
  def Model(trainDF: DataFrame,testDF:DataFrame): DataFrame = {
    // 创建决策树模型
    val dt = new DecisionTreeClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxBins(15)
      .setImpurity("gini")
      .setSeed(10)
    //创建随机森林模型
//    val rf = new RandomForestClassifier()
//      .setFeaturesCol("features")
//      .setLabelCol("label")
//      .setNumTrees(10)
//      .setMaxBins(10)
//      .setImpurity("gini")
//      .setSeed(10)

    // 训练模型
    val dtModel = dt.fit(trainDF)

    // 在测试集上预测
    val testPredictions = dtModel.transform(testDF)
    testPredictions
  }
  def Evaluate(testPredictions: DataFrame): Unit = {
    //计算测试集召回率
    val evaluator1 = new MulticlassClassificationEvaluator()
      .setMetricName("weightedRecall")
    val recall = evaluator1.evaluate(testPredictions)
    //计算测试集精确率
    val evaluator2 = new MulticlassClassificationEvaluator()
      .setMetricName("weightedPrecision")
    val precision = evaluator2.evaluate(testPredictions)
    //计算测试集F1值
    val evaluator3 = new MulticlassClassificationEvaluator()
      .setMetricName("f1")
    val f1 = evaluator3.evaluate(testPredictions)
//    print("recall   : " + recall+"\n")
//    print("precision: " + precision+"\n")
//    print("f1-score : " + f1+"\n")
    //将上述结果写入csv文件
    //解决toDF方法找不到的问题
    import testPredictions.sparkSession.implicits._
    val df = Seq(("recall",recall),("precision",precision),("f1-score",f1)).toDF("指标","value")
    df.show(false)
    //写的时候合并成一个文件，不要多个文件写入
    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("E:\\IDEA_Project\\exp4\\src\\output\\Task3")

  }
  //定义主函数
  def main(args: Array[String]): Unit = {
    //调用readCSV函数，读取原始数据，并显示
    val filePath = "E:\\IDEA_Project\\exp4\\src\\application_data.csv"
    val df = readCSV(filePath)
    df.show()
   //显示df所有列的数据类型
    df.printSchema()
    val Taskid="3"
    if (Taskid == "1.1"){
      Task1_1(df)
    }
    else if (Taskid == "1.2"){
      Task1_2(df)
    }
    else if (Taskid == "2.1"){
      Task2_1(df)
    }
    else if (Taskid == "2.2"){
      Task2_2(df)
    }
    else if (Taskid == "3"){
      //Task3
      //数据预处理
      val preprocessed_df = Task3_get_preprocessed_df(df)
      //随机划分训练集和测试集
      val Array(trainDF, testDF) = preprocessed_df.randomSplit(Array(0.8, 0.2))

      // 训练模型
      val testPredictions = Model(trainDF, testDF)
      // 评估模型
      Evaluate(testPredictions)
    }
    else{
      print("输入错误")
    }
    //暂停进程
    println("暂停进程，请手动关闭")
    Thread.sleep(1000000)
  }
}
