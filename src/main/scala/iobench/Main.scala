package iobench

import akka.actor._
import java.net._
import akka.actor.Terminated
import scala.util.control.NonFatal

class MainActor extends Actor with ActorLogging {

  import akka.io._
  import akka.io.Tcp._
  import context.system

  val manager = IO(Tcp)

  def receive = {
    case MainActor.Benchmark(file, target, blockSize, kind) =>
      val server =  context.actorOf(kind match {
        case "bytestring_ack" => Props[MappedFileAck]
        case "bytestring_nack" => Props[MappedFileNAck]
        case "writefile" => Props[WriteFileBenchmark]
        case _ => ???
      }, "server")
      manager ! Connect(target)
      context.become {
        case Connected(remote, local) =>
          val connection = sender
          connection ! Tcp.Register(server)
          context.watch(server)
          server ! Benchmark.SendFile(file, connection, blockSize)
        case msg@CommandFailed(cmd) =>
          log.debug(msg.toString)
          println("Could not connect to " + target)
          system.shutdown()
        case msg@Terminated(`server`) =>
          log.debug(msg.toString)
          system.shutdown()
      }
  }
}

object MainActor {
  case class Benchmark(file:String, target:InetSocketAddress, blockSize:Int, kind:String)
}

object Main extends App {
  val validKind = Seq("bytestring_ack", "bytestring_nack", "writefile")
  try {
    require(args.length == 5)
    val file = args(0)
    val host = args(1)
    val port = args(2).toInt
    val target = new InetSocketAddress(host, port)
    val blockSize = args(3).toInt
    val kind = args(4).toLowerCase
    require(validKind.contains(kind))

    val system = ActorSystem("iobench")
    val actor = system.actorOf(Props[MainActor], "main")
    actor ! MainActor.Benchmark(file, target, blockSize, kind)
  } catch {
    case NonFatal(e) =>
      println("Usage java -jar iobench.jar <file> <host> <port> <blockSize> <kind>")
      println("kind = " + validKind.mkString("{", ",", "}"))
  }
}