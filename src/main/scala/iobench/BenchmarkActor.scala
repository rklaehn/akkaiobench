package iobench

import akka.actor._
import akka.util.ByteString
import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel
import akka.io.Tcp._

sealed abstract class BenchmarkActor extends Actor with ActorLogging {

  def printResult(size:Int, dt:Long) {
    val seconds = dt / 1e9
    println(getClass.getSimpleName)
    println(s"Transferred $size bytes in $seconds seconds.")
    println(s"Transfer rate ${size/(seconds*1024*1024)} MBytes/s")
  }
}

object BenchmarkActor {
  case class SendFile(file:String, target:ActorRef, blockSize:Int)

  val validKind = Seq("bytestring_ack", "bytestring_nack", "writefile")

  def validate(kind:String) {
    require(validKind.contains(kind.toLowerCase))
  }

  def makeProps(kind:String):Props = {
    kind.toLowerCase match {
      case "bytestring_ack" => Props[MappedFileAckBenchmarkActor]
      case "bytestring_nack" => Props[MappedFileNAckBenchmarkActor]
      case "writefile" => Props[WriteFileBenchmarkActor]
      case _ => ???
    }
  }
}

class WriteFileBenchmarkActor extends BenchmarkActor {
  import BenchmarkActor._
  import WriteFileBenchmarkActor._

  def receive = {
    case SendFile(file, connection, blockSize) =>
      val t0 = System.nanoTime()
      val size = new File(file).length().toInt
      val command = WriteFile(file,0,size,Finished)
      connection ! command
      context.become {
        case Finished =>
          printResult(size, System.nanoTime() - t0)
          connection ! Close
        case msg@CommandFailed(cmd) =>
          log.debug(msg.toString)
        case PeerClosed | Closed =>
          context.stop(self)
        case msg =>
          log.debug(msg.toString)
      }
  }
}

object WriteFileBenchmarkActor {
  case object Finished extends Event
}

class MappedFileAckBenchmarkActor extends BenchmarkActor {
  import BenchmarkActor._
  import MappedFileAckBenchmarkActor._

  def receive = {
    case SendFile(file, connection, blockSize) =>
      val channel = new RandomAccessFile(file, "r").getChannel()
      val size = channel.size().toInt
      val buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size)
      channel.close()
      val t0 = System.nanoTime()
      var offset = 0
      self ! Ack(0)
      context.become {
        case msg@Ack(x) =>
          if(x==size) {
            printResult(size, System.nanoTime() - t0)
            connection ! Close
          } else {
            offset = x
            val min = offset
            val max = (offset + blockSize) min size
            val slice = buffer.slice()
            slice.position(min)
            slice.limit(max)
            connection ! Write(ByteString.fromByteBuffer(slice), Ack(max))
          }
        case msg@CommandFailed(cmd) =>
          log.debug(msg.toString)
        case msg@PeerClosed =>
          context.stop(self)
          log.debug(msg.toString)
        case msg@Closed =>
          log.debug(msg.toString)
          context.stop(self)
        case msg =>
          log.debug(msg.toString)
      }
  }
}

object MappedFileAckBenchmarkActor {
  case class Ack(offset:Int) extends Event
}

class MappedFileNAckBenchmarkActor extends BenchmarkActor {
  import BenchmarkActor._
  import MappedFileNAckBenchmarkActor._

  def receive = {
    case SendFile(file, connection, blockSize) =>
      val channel = new RandomAccessFile(file, "r").getChannel()
      val size = channel.size().toInt
      val buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size)
      channel.close()
      val t0 = System.nanoTime()
      var offset = 0
      var batch = 0
      var ackCount = 0
      self ! SendNext
      context.become {
        case msg@SendNext =>
          if(offset < size) {
            val ack = Ack(offset, (offset + blockSize) min size, batch)
            val slice = buffer.slice()
            slice.position(ack.min)
            slice.limit(ack.max)
            connection ! Write(ByteString.fromByteBuffer(slice), ack)
            offset = ack.max
            if(ackCount == 0)
              self ! SendNext
          }
        case msg@Ack(min, max, batch) =>
          log.debug(msg.toString)
          // success
          if(max==size) {
            // we are finished
            printResult(size, System.nanoTime() - t0)
            connection ! Close
          } else if(msg.batch==batch && ackCount>0) {
            // we are in ack mode, so decrease ack count and trigger a send
            // however, only do this if the ack is for the right batch!
            ackCount -= 1
            self ! SendNext
          }
        case msg@CommandFailed(Write(_, fail@Ack(_, _, _))) =>
          if(batch == fail.batch) {
            log.debug("Failed " + fail)
            // reset offset to where the write failed
            offset = fail.min
            // increase the batch so we ignore subsequent failures of this batch
            batch += 1
            // enter ack mode for 10 messages
            ackCount = 10
            // send ResumeWriting to the connection
            connection ! ResumeWriting
          }
        case msg@WritingResumed =>
          // trigger send of the next block
          self ! SendNext
        case msg@PeerClosed =>
          context.stop(self)
          log.debug(msg.toString)
        case msg@Closed =>
          log.debug(msg.toString)
          context.stop(self)
        case msg =>
          log.debug(msg.toString)
      }
  }
}

object MappedFileNAckBenchmarkActor {
  case object SendNext
  case class Ack(min:Int, max:Int, batch:Int) extends Event
}