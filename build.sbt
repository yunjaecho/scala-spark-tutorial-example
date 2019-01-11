name := "scala-spark-tutorial-example"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
// https://mvnrepository.com/artifact/postgresql/postgresql
libraryDependencies += "postgresql" % "postgresql" % "9.1-901-1.jdbc4"
// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.3.3"
