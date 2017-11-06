package org.apache.beam.examples

import java.util.concurrent.ThreadLocalRandom

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.apache.beam.sdk.transforms.{Count, DoFn, MapElements, ParDo}
import org.joda.time.{Duration, Instant}

object WindowedWordCount {

  def main(args: Array[String]): Unit = {

    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[WindowedWordCountOptions])

    val minTimestamp = new Instant(options.getMinTimestampMillis)
    val maxTimestamp = new Instant(options.getMaxTimestampMillis)

    val pipeline = Pipeline.create(options)

    pipeline.apply("ReadFiles", TextIO.read().from(options.getInputFile))
      .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)))
      .apply(Window.into[String](FixedWindows.of(Duration.standardMinutes(options.getWindowSize))))
      .apply(ParDo.of(new ExtractWords))
      .apply(Count.perElement())
      .apply(MapElements.via(new FormatResult))
      .apply("WriteWords", TextIO.write()
        .to(options.getOutput)
        .withWindowedWrites()
        .withNumShards(options.getNumShards))

    pipeline.run().waitUntilFinish()

  }
}

// ======================================= Options =============================================

trait WindowedWordCountOptions extends WordCountOptions {
  @Description("Fixed window duration, in minutes")
  @Default.Long(1)
  def getWindowSize: Long
  def setWindowSize(value: Long): Unit

  @Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
  @Default.InstanceFactory(classOf[DefaultToCurrentSystemTime])
  def getMinTimestampMillis: Long
  def setMinTimestampMillis(value: Long): Unit

  @Description("Maximum randomly assigned timestamp, in milliseconds-since-epoch")
  @Default.InstanceFactory(classOf[DefaultToMinTimestampPlusOneHour])
  def getMaxTimestampMillis: Long
  def setMaxTimestampMillis(value: Long): Unit

  @Description("Fixed number of shards to produce per window, or null for runner-chosen sharding")
  @Default.Integer(1)
  def getNumShards: Integer
  def setNumShards(numShards: Integer): Unit
}

// ======================================== UDFs ================================================

class AddTimestampFn(minTimestamp: Instant, maxTimestamp: Instant) extends DoFn[String, String] {
  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    val randomTS = new Instant(ThreadLocalRandom.current.nextLong(minTimestamp.getMillis, maxTimestamp.getMillis))
    c.outputWithTimestamp(c.element(), new Instant(randomTS))
  }
}

// ====================================== Defaults ==============================================

class DefaultToCurrentSystemTime extends DefaultValueFactory[Long] {
  override def create(options: PipelineOptions) = {
    System.currentTimeMillis()
  }
}

class DefaultToMinTimestampPlusOneHour extends DefaultValueFactory[Long] {
  override def create(options: PipelineOptions): Long = {
    options.as(classOf[WindowedWordCountOptions])
      .getMinTimestampMillis + Duration.standardHours(1).getMillis
  }
}
