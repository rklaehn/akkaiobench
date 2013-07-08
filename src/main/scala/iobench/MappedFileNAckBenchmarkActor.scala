package iobench

import akka.io.Tcp._
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import akka.util.ByteString

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