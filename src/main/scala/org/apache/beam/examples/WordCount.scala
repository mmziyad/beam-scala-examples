package org.apache.beam.examples

import java.lang

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{KV, PCollection}

object WordCount {

  def main(args: Array[String]): Unit = {

    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[WordCountOptions])

    val pipeline = Pipeline.create(options)

    pipeline.apply("ReadFiles", TextIO.read().from(options.getInputFile))
      .apply(ParDo.of(new ExtractWords))
      .apply(Count.perElement())
      .apply(MapElements.via(new FormatResult))
      .apply("WriteWords", TextIO.write().to(options.getOutput))

    pipeline.run().waitUntilFinish()
  }
}

// ======================================= Options =============================================

trait WordCountOptions extends PipelineOptions {

  @Description("Path of the file to read from")
  @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
  def getInputFile: String
  def setInputFile(path: String)

  @Description("Path of the file to write to")
  @Required
  def getOutput: String
  def setOutput(path: String)

}

// ======================================== UDFs ===============================================

class FormatResult extends SimpleFunction[KV[String, java.lang.Long], String] {
  override def apply(input: KV[String, lang.Long]): String = {
    input.getKey + ": " + input.getValue
  }
}

class ExtractWords extends DoFn[String, String] {
  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    for (word <- c.element().split("[^\\p{L}]+")) yield {
      if (!word.isEmpty) c.output(word)
    }
  }
}

class CountWords extends PTransform[PCollection[String], PCollection[KV[String, java.lang.Long]]] {
  override def expand(input: PCollection[String]) = {
    input.apply(ParDo.of(new ExtractWords)) //Ignore IntelliJ error: "Cannot resolve apply". The code will compile.
      .apply(Count.perElement())
  }
}
