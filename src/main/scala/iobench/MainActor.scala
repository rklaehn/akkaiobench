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
      var size = blockSize
      manager ! Connect(target)
      context.become {
        case Connected(remote, local) =>
          val connection = sender
          val server =  context.actorOf(BenchmarkActor.makeProps(kind), "server")
          connection ! Tcp.Register(server)
          context.watch(server)
          println(s"$kind\t$size")
          server ! BenchmarkActor.SendFile(file, connection, size)
        case msg@CommandFailed(cmd) =>
          log.debug(msg.toString)
          println("Could not connect to " + target)
          system.shutdown()
        case msg@Terminated(_) =>
          log.debug(msg.toString)
          if(size<=32)
            system.shutdown()
          else {
            size /= 2
            manager ! Connect(target)
          }
      }
  }
}

object MainActor {
  case class Benchmark(file:String, target:InetSocketAddress, blockSize:Int, kind:String)
}

object Main extends App {
  try {
    require(args.length == 5)
    val file = args(0)
    val host = args(1)
    val port = args(2).toInt
    val target = new InetSocketAddress(host, port)
    val blockSize = args(3).toInt
    val kind = args(4).toLowerCase
    BenchmarkActor.validate(kind)

    val system = ActorSystem("iobench")
    val actor = system.actorOf(Props[MainActor], "main")
    actor ! MainActor.Benchmark(file, target, blockSize, kind)
  } catch {
    case NonFatal(e) =>
      println("Usage java -jar iobench.jar <file> <host> <port> <blockSize> <kind>")
      println("kind = " + BenchmarkActor.validKind.mkString("{", ",", "}"))
  }
}