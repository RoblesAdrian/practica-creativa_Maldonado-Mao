error id: file://<WORKSPACE>/flight_prediction/build.sbt:
file://<WORKSPACE>/flight_prediction/build.sbt
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -scalaVersion.
	 -scalaVersion#
	 -scalaVersion().
	 -scala/Predef.scalaVersion.
	 -scala/Predef.scalaVersion#
	 -scala/Predef.scalaVersion().
offset: 49
uri: file://<WORKSPACE>/flight_prediction/build.sbt
text:
```scala


name := "flight_prediction"

version := "0.1"

@@scalaVersion := "2.13.10"

val sparkVersion = "4.1.2"

mainClass in Compile := Some("es.upm.dit.ging.predictor.MakePrediction")

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.1"

)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 