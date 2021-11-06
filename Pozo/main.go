package main

import (
  "log"
  str "strings"
  "os"
  "fmt"
  "time"
  "net"
  "strconv"
  "google.golang.org/grpc"
  "github.com/streadway/amqp"
  "context"
  pb "Pozo/proto"
)

var pozoAcumulado int = 0
var fechaPozo string

type server struct {
	pb.UnimplementedGetAmountServer
}


func failOnError(err error, msg string) {
  if err != nil {
    log.Fatalf("%s: %s", msg, err)
  }
}

func (s *server) AskAmount(ctx context.Context, msg *pb.Message) (*pb.Amount, error) {
	fmt.Println(msg.Msg)
	Toreturn := &pb.Amount{
		Amount: strconv.Itoa(pozoAcumulado),
	  }
	return Toreturn, nil
}

func actualizarPozo(data []byte){
	
	dataString := string(data)	
	jugadorRonda := str.Split(dataString, ";")
        log.Printf("El jugador %s a muerto en la ronda %s.", jugadorRonda[0], jugadorRonda[1])

	pozoAcumulado += 100000000 

	dataToWrite := []byte("Jugador_" + jugadorRonda[0] +" Ronda_" + jugadorRonda[1] + " " + strconv.Itoa(pozoAcumulado) + "\n")

	f, err := os.OpenFile("archivo_pozos/pozo_juego_" + fechaPozo + ".txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
        if err != nil {
        	log.Fatal(err)
        }
        if _, err := f.Write(dataToWrite); err != nil {
    	       	log.Fatal(err)
        }
        if err := f.Close(); err != nil {
                log.Fatal(err)
        }	
	fmt.Println("El Pozo ha sido actualizado, el monto acumulado ahora es " + strconv.Itoa(pozoAcumulado) + ".")

}

func InitServer() {
        lis, err := net.Listen("tcp", ":50051")
        if err != nil {
                log.Fatalf("failed to listen: %v", err)
        }
        s := grpc.NewServer()
	pb.RegisterGetAmountServer(s, &server{})
        log.Printf("server listening at %v", lis.Addr())

        if err := s.Serve(lis); err != nil {
                log.Fatalf("failed to serve: %v", err)
        }

        return
}

func main(){
	
	//Iniciamos la conexion con la cola RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	//Abrimos un canal
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
		
	q, err := ch.QueueDeclare(
	  "hello", // name
	  false,   // durable
	  false,   // delete when unused
	  false,   // exclusive
	  false,   // no-wait
	  nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")	
	
	//Definimos la variable que tendrá el mensaje
	

	msgs, err := ch.Consume(
	  q.Name, // queue
	  "",     // consumer
	  true,   // auto-ack
	  false,  // exclusive
	  false,  // no-local
	  false,  // no-wait
	  nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	
	forever := make(chan bool)
	
	go func() {
	  for d := range msgs {
		actualizarPozo(d.Body)
	  }
	}()
 
	go InitServer()

	//Definimos la fecha identificativa del archivo de registro
	fechaPozo = (time.Now()).Format("2006_01_02_T_15_04_05")
	
	log.Printf("#################################################")
	log.Printf("Pozo incializado correctamente, esperando dinero.")

        fmt.Println("##############################################################################\n")
        fmt.Println("          @@@@@               @@@@@@@@@@@@@@@@@@                              ")
        fmt.Println("      @@         @@           @                @                 @@@@         ")
        fmt.Println("    @@             @@         @                @                @@  &@        ")
        fmt.Println("   @@               @@        @                @              ,@      @@      ")
        fmt.Println("   @@               @@        @                &             @@        @@     ")
        fmt.Println("    @              @@         @                @            @            @@   ")
        fmt.Println("     @@.         @@@          @                @          @@              @@  ")
        fmt.Println("         @@@@@@@              @@@@@@@@@@@@@@@@@@         @@@@@@@@@@@@@@@@@@@@.")
        fmt.Println("\n##############################################################################")

	<-forever

}
