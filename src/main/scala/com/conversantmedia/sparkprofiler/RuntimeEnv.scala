package com.conversantmedia.sparkprofiler

case class RuntimeEnv(jvmInformation: Map[String, String],
                      sparkProperties: Map[String, String],
                      systemProperties: Map[String, String],
                      classpathEntries: Map[String, String])

private case class SparkRuntimeEnv(`JVM Information`: Map[String, String],
                           `Spark Properties`: Map[String, String],
                           `System Properties`: Map[String, String],
                           `Classpath Entries`: Map[String, String])

