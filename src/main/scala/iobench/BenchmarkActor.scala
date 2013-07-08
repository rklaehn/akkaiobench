package iobench

import akka.actor._
import akka.util.ByteString
import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel
import akka.io.Tcp._

abstract class BenchmarkActor extends Actor with ActorLogging {

  def printResult(size:Int, dt:Long) {
    val seconds = dt / 1e9
//    println(getClass.getSimpleName)
//    println(s"Transferred $size bytes in $seconds seconds.")
//    println(s"Transfer rate ${size/(seconds*1024*1024)} MBytes/s")
  }
}

object BenchmarkActor {
  case class SendFile(file:String, target:ActorRef, blockSize:Int)

  val validKind = Seq("bytestring_ack", "bytestring_nack", "writefile",  "writefile_ack" )

  def validate(kind:String) {
    require(validKind.contains(kind.toLowerCase))
  }

  def makeProps(kind:String):Props = {
    kind.toLowerCase match {
      case "bytestring_ack" => Props[MappedFileAckBenchmarkActor]
      case "bytestring_nack" => Props[MappedFileNAckBenchmarkActor]
      case "writefile" => Props[WriteFileBenchmarkActor]
      case "writefile_ack" => Props[WriteFileAckBenchmarkActor]
      case _ => ???
    }
  }
}