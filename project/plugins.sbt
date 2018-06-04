logLevel := Level.Warn

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.5")
addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.3")
