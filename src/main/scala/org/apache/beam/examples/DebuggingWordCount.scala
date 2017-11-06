package org.apache.beam.examples

import java.util.regex.Pattern

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.{Default, Description, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{Count, DoFn, MapElements, ParDo}
import org.apache.beam.sdk.values.KV
import org.slf4j.LoggerFactory

object DebuggingWordCount {

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[DebuggingWordCountOptions])

    val pipeline = Pipeline.create(options)

    pipeline.apply("ReadFiles", TextIO.read().from(options.getInputFile))
      .apply(ParDo.of(new ExtractWords))
      .apply(Count.perElement())
      .apply(ParDo.of(new FilterTextFn(options.getFilterPattern)))
      .apply(MapElements.via(new FormatResult))
      .apply("WriteWords", TextIO.write().to(options.getOutput))

    pipeline.run().waitUntilFinish()
  }
}

// ======================================== UDFs ===============================================

class FilterTextFn(pattern: String) extends DoFn[KV[String, java.lang.Long], KV[String, java.lang.Long]] {
  private val logger = LoggerFactory.getLogger(getClass)
  lazy val filter = Pattern.compile(pattern)
  lazy val matchedWords = Metrics.counter(classOf[FilterTextFn], "matchedWords")
  lazy val unmatchedWords = Metrics.counter(classOf[FilterTextFn], "unmatchedWords")

  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    filter.matcher(c.element.getKey).matches() match {
      case true => logger.debug("Matched: " + c.element.getKey)
        matchedWords.inc()
        c.output(c.element)
      case false => logger.trace("Did not match: " + c.element.getKey)
        unmatchedWords.inc()
    }
  }
}

// ======================================= Options ==============================================

trait DebuggingWordCountOptions extends WordCountOptions {
  @Description("Regex filter pattern to use in DebuggingWordCount. Only words matching this pattern will be counted.")
  @Default.String("Flourish|stomach")
  def getFilterPattern: String
  def setFilterPattern(value: String): Unit
}
