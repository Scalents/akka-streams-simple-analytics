scalaVersion := "2.12.7"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.17"
libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.1.3"
libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.3"

scalacOptions += "-Ypartial-unification"
