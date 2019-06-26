//import sbt._
//import Keys._
//import sbtassembly.Plugin.AssemblyKeys._
//sbt -java-home /Library/Java/JavaVirtualMachines/jdk1.8.0_101.jdk/Contents/Home assembly
name := "sparkds"

version := "1.0"

scalaVersion := "2.11.12"

javacOptions ++= Seq("-source", "1.8")

scalacOptions := Seq("-deprecation", "-unchecked", "-feature", "-Ylog-classpath", "-target:jvm-1.8")

//For java only libraries, use "%" instead of "%%"
libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.6" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.3" % "provided",//  exclude("com.google.protobuf","protobuf-java"),
  "org.apache.spark" %% "spark-streaming" % "2.4.3" % "provided",//  exclude("com.google.protobuf","protobuf-java"),
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",//  exclude("com.google.protobuf","protobuf-java"),
  "org.apache.spark" %% "spark-hive" % "2.4.3" % "provided",
  "com.typesafe" % "config" % "1.3.2"//  exclude("com.google.protobuf","protobuf-java"),
)

resolvers ++= Seq(
  //"Local Maven Repository" at "file://"+"/Users/skoppar"+"/.m2/repository",
  "maven" at "https://mvnrepository.com",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/groups/public",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "scala-tools.org" at "http://scala-tools.org/repo-releases",
  "twitter" at "http://maven.twttr.com/"
)

//jarName := s"${name.value}-${version.value}.jar"

////below line skips test during assembly, comment it later when tests needs to be included during assembly
//test in assembly := {}
//parallelExecution in Test := false
//
//mergeStrategy in assembly := {
//  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
//  case m if m.toLowerCase.matches("meta-inf/.*\\.sf$") => MergeStrategy.discard
//  case _ => MergeStrategy.first
//}