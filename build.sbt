name := "Spark-HandsOn"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
