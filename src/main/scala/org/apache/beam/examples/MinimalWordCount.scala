package org.apache.beam.examples

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{KV, TypeDescriptors}

object MinimalWordCount {

  def main(args: Array[String]): Unit = {

    val options = PipelineOptionsFactory.create()
    val pipeline = Pipeline.create(options)

    pipeline.apply("ReadFiles", TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
      .apply("ExtractWords", ParDo.of(extractWords))
      .apply(Count.perElement[String]())
      .apply("FormatResults", MapElements.into(TypeDescriptors.strings()).via(formatResults))
      .apply("WriteWords", TextIO.write().to("/tmp/minimal-wordcount"))

    pipeline.run().waitUntilFinish()
  }

  // ======================================== UDFs ===============================================
  def extractWords = new DoFn[String, String] {
    @ProcessElement
    def processElement(c: ProcessContext): Unit = {
      for (word <- c.element().split("[^\\p{L}]+")) yield {
        if (!word.isEmpty) c.output(word)
      }
    }
  }

  def formatResults = new SimpleFunction[KV[String, java.lang.Long], String] {
    override def apply(input: KV[String, java.lang.Long]): String = input.getKey + ": " + input.getValue
  }
}



