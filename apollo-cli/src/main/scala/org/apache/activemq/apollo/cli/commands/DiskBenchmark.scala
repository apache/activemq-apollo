package org.apache.activemq.apollo.cli.commands

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import io.airlift.command.{Arguments, Command, Option}
import java.io.{PrintStream, InputStream, RandomAccessFile, File}
import java.util.concurrent.TimeUnit
import javax.management.ObjectName
import management.ManagementFactory
import org.apache.activemq.apollo.util.{MemoryPropertyEditor, IOHelper}
import MemoryPropertyEditor._


class Report {
  
  var block_size: Int = 0
  var async_writes: Int = 0
  var async_write_duration: Long = 0L
  var sync_writes: Int = 0
  var sync_write_duration: Long = 0L
  var reads: Int = 0
  var read_duration: Long = 0L

  def async_write_size_rate: Float = {
    var rc: Float = async_writes
    rc *= block_size
    rc /= (1024 * 1024)
    rc /= (async_write_duration / 1000.0f)
    return rc
  }

  def async_write_rate: Float = {
    var rc: Float = async_writes
    rc /= (async_write_duration / 1000.0f)
    return rc
  }

  def sync_write_size_rate: Float = {
    var rc: Float = sync_writes
    rc *= block_size
    rc /= (1024 * 1024)
    rc /= (sync_write_duration / 1000.0f)
    return rc
  }

  def sync_write_rate: Float = {
    var rc: Float = sync_writes
    rc /= (sync_write_duration / 1000.0f)
    return rc
  }

  def read_size_rate: Float = {
    var rc: Float = reads
    rc *= block_size
    rc /= (1024 * 1024)
    rc /= (read_duration / 1000.0f)
    return rc
  }

  def read_rate: Float = {
    var rc: Float = reads
    rc /= (read_duration / 1000.0f)
    return rc
  }
  
  override def toString: String = {
    return "Async writes: \n" + "  " + async_writes + " writes of size " + block_size + " written in " + (async_write_duration / 1000.0) + " seconds.\n" +
            "  " + async_write_rate + " writes/second.\n" +
            "  " + async_write_size_rate + " megs/second.\n" +
            "\n" +
            "Sync writes: \n" + "  " + sync_writes + " writes of size " + block_size + " written in " + (sync_write_duration / 1000.0) + " seconds.\n" +
            "  " + sync_write_rate + " writes/second.\n" +
            "  " + sync_write_size_rate + " megs/second.\n" +
            "\n" +
            "Reads: \n" + "  " + reads + " reads of size " + block_size + " read in " + (read_duration / 1000.0) + " seconds.\n" +
            "  " + read_rate + " reads/second.\n" +
            "  " + read_size_rate + " megs/second.\n" +
            "\n" + ""
  }

}

object DiskBenchmark {
  
  final val PHYSICAL_MEM_SIZE = format((try {
    val mbean_server = ManagementFactory.getPlatformMBeanServer()
    mbean_server.getAttribute(new ObjectName("java.lang:type=OperatingSystem"), "TotalPhysicalMemorySize") match {
      case x:java.lang.Long=> Some(x.longValue)
      case _ => None
    }
  } catch {
    case _ => None
  }).getOrElse(1024*1024*500L))

}


/**
 * The apollo encrypt command
 */
@Command(name = "disk-benchmark", description = "Benchmarks your disk's speed")
class DiskBenchmark extends BaseAction {
  import DiskBenchmark._
  
  
  @Option(name = Array("--verbose"), description = "Enable verbose output")
  var verbose: Boolean = false
  @Option(name = Array("--sample-interval"), description = "The number of milliseconds to spend mesuring perfomance.")
  var sampleInterval: Long = 30 * 1000

  @Option(name = Array("--block-size"), description = "The size of each IO operation.")
  var block_size_txt = "4k"
  def block_size = parse(block_size_txt).toInt

  @Option(name = Array("--file-size"), description = "The size of the data file to use, this should be big enough to flush the OS write cache.")
  var file_size_txt = PHYSICAL_MEM_SIZE
  def file_size = parse(file_size_txt)

  @Option(name = Array("--warm-up-size"), description = "The amount of data we should initial write before measuring performance samples (used to flush the OS write cache).")
  var warm_up_size_txt = format(parse("500M").min(parse(PHYSICAL_MEM_SIZE)/2))
  def warm_up_size = parse(warm_up_size_txt)

  @Arguments(description="The file that will be used to benchmark your disk (must NOT exist)")
  var file = new File("disk-benchmark.dat")

  def execute(in: InputStream, out: PrintStream, err: PrintStream): Int = {
    init_logging
    try {
      if (file.exists) {
        out.println("File " + file + " allready exists, will not benchmark.")
      } else {
        out.println("Benchmark using data file: " + file.getCanonicalPath)

        // Initialize the block /w data..
        var data = new Array[Byte](block_size)
        var i: Int = 0
        while (i < data.length) {
          data(i) = ('a' + (i % 26)).asInstanceOf[Byte]
          i += 1
        }

        val report = new Report
        report.block_size = block_size

        // Pre-allocate the file size..
        var raf = new RandomAccessFile(file, "rw")
        try {

          out.println("Pre-allocating data file of size: "+file_size_txt)
          raf.setLength(file_size)
          raf.seek(file_size-1)
          raf.writeByte(0);
          IOHelper.sync(raf.getFD)

          if( warm_up_size > 0 ) {
            val max = warm_up_size
            out.println("Warming up... writing async "+warm_up_size_txt+" so that async writes don't have that much of an advantage due to the OS write cache.")
            write(raf, data, (count)=>{
              count > max
            })
          }


          out.println("Benchmarking async writes")
          var start = System.nanoTime()
          var end = start
          report.async_writes = write(raf, data, (count)=>{
            end = System.nanoTime
            TimeUnit.NANOSECONDS.toMillis(end-start) >  sampleInterval
          })
          report.async_write_duration = TimeUnit.NANOSECONDS.toMillis(end-start)

          out.println("Syncing previous writes before measuring sync write performance.. (might take a while if your OS has a big write cache)")
          IOHelper.sync(raf.getFD)

          out.println("Benchmarking sync writes")
          start = System.nanoTime()
          end = start
          report.sync_writes = write(raf, data, (count)=>{
            IOHelper.sync(raf.getFD)
            end = System.nanoTime
            TimeUnit.NANOSECONDS.toMillis(end-start) >  sampleInterval
          })
          report.sync_write_duration = TimeUnit.NANOSECONDS.toMillis(end-start)

          if(!filled) {
            file_size_txt = ""+raf.getFilePointer
            out.println("File was not fully written, read benchmark will be operating against: "+(file_size/(1024 * 1024.0f))+" megs of data" )
            raf.seek(0);
          }
          out.println("Benchmarking reads")
          start = System.nanoTime()
          end = start
          report.reads = read(raf, data, (count)=>{
            end = System.nanoTime
            TimeUnit.NANOSECONDS.toMillis(end-start) >  sampleInterval
          })
          report.read_duration = TimeUnit.NANOSECONDS.toMillis(end-start)

        } finally {
          out.println("Closing.")
          raf.close
        }
        file.delete
        out.println(report)
      }
      0
    } catch {
      case x:Helper.Failure=>
        sys.error(x.getMessage)
        1
      case e: Throwable =>
        if (verbose) {
          err.println("ERROR:")
          e.printStackTrace(System.out)
        } else {
          err.println("ERROR: " + e)
        }
      1
    }
  }
  
  var filled = false

  private def write(raf: RandomAccessFile, data: Array[Byte], until: (Long)=>Boolean) = {
    var file_position = raf.getFilePointer
    var counter = 0
    while (!until(counter.toLong * data.length)) {
      if(  file_position + data.length >= file_size ) {
        filled = true
        file_position = 0;
        raf.seek(file_position)
      }
      raf.write(data)
      counter += 1;
      file_position += data.length
    }
    counter
  }

  private def read(raf: RandomAccessFile, data: Array[Byte], until: (Long)=>Boolean) = {
    var file_position = raf.getFilePointer
    var counter = 0
    while (!until(counter.toLong * data.length)) {
      if( file_position + data.length >= file_size ) {
        file_position = 0;
        raf.seek(file_position)
      }
      raf.readFully(data)
      counter += 1;
      file_position += data.length
    }
    counter
  }

}