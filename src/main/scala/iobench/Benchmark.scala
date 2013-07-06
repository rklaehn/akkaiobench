package iobench

import akka.actor._
import akka.util.ByteString
import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel
import akka.io.Tcp._

sealed abstract class Benchmark extends Actor with ActorLogging {

  def printResult(size:Int, dt:Long) {
    val seconds = dt / 1e9
    println(getClass.getSimpleName)
    println(s"Transferred $size bytes in $seconds seconds.")
    println(s"Transfer rate ${size/(seconds*1024*1024)} MBytes/s")
  }
}

object Benchmark {
  case class SendFile(file:String, target:ActorRef, blockSize:Int)
}

class WriteFileBenchmark extends Benchmark {
  import Benchmark._
  import WriteFileBenchmark._

  def receive = {
    case SendFile(file, target, blockSize) =>
      val t0 = System.nanoTime()
      val size = new File(file).length().toInt
      target ! WriteFile(file,0,size,Finished)
      context.become {
        case Finished =>
          printResult(size, System.nanoTime() - t0)
          target ! Close
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

object WriteFileBenchmark {
  case object Finished extends Event
}

class MappedFileAck extends Benchmark {
  import Benchmark._
  import MappedFileAck._

  def receive = {
    case SendFile(file, target, blockSize) =>
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
            target ! Close
          } else {
            offset = x
            val min = offset
            val max = (offset + blockSize) min size
            val slice = buffer.slice()
            slice.position(min)
            slice.limit(max)
            target ! Write(ByteString.fromByteBuffer(slice), Ack(max))
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

object MappedFileAck {
  case class Ack(offset:Int) extends Event
}

class MappedFileNAck extends Benchmark {
  import Benchmark._
  import MappedFileNAck._

  def receive = {
    case SendFile(file, target, blockSize) =>
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
            target ! Write(ByteString.fromByteBuffer(slice), ack)
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
            target ! Close
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
            // send ResumeWriting to the connection
            target ! ResumeWriting
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

object MappedFileNAck {
  case object SendNext
  case class Ack(min:Int, max:Int, batch:Int) extends Event
}