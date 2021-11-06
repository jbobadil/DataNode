package main

import (
	"context"
	"net"
	"fmt"
	"log"
	"math/rand"
	str "strings"
	"os"
	"google.golang.org/grpc"
	pb "NameNode/proto"
	"time"
)

const (
	port = ":50051"
)

var dataNode1 string = "dist193.inf.santiago.usm.cl"
var dataNode2 string = "dist195.inf.santiago.usm.cl"
var dataNode3 string = "dist196.inf.santiago.usm.cl"
var fechaLog string


// Server
type server struct {
	pb.UnimplementedStartServerServer
}


func InitServer(port string) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterStartServerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	return 
}

func enviarJugada(ronda string, jugador string, jugadas string, dataNode string){

	connDataNode, errDataNode := grpc.Dial(dataNode + ":50052", grpc.WithInsecure())
        if errDataNode != nil {
                log.Fatalf("Error al conectarse al DataNode %s : %v", dataNode, errDataNode)
        }

        defer connDataNode.Close()
        //retornamos la instancia con el NameNode

        cDataNode := pb.NewStartServerClient(connDataNode)
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        defer cancel()

        status, err := cDataNode.DataNodeStoreMove(ctx, &pb.Playermove{Moves: jugadas ,Round: ronda, Player: jugador})

        if err != nil {
                log.Fatalf("No se pudo ejecutar la función de almacenaje de movimientos: %v", err)
        }

        log.Printf("%s", status)

}

func (s *server) NameNodeStorePlayersMoves(ctx context.Context, datosMovidas *pb.Playersmoves ) (*pb.Status , error){
	
	fmt.Println("Fueron recibidos los movimientos de los jugadores.")
	fmt.Println("Inicializando proceso para almacenar movimientos")
	
	//Revisamos si moves es valido
	if len(datosMovidas.PlayerMoves) >= 0 {
		//Inicio del Parsing
		    
		//Separamos el string mediante ";", obteniendo una lista de jugadores con movimientos
		// con el formato
		// jugadorx-movimiento1,movimiento2,movimiento3...

		movidas := str.Split(datosMovidas.PlayerMoves, ";")
		ronda := datosMovidas.Round

		//Recorremos la lista de movidas
		for i := 0; i < len(movidas); i ++ {
			//Seleccionamos mediante azar el DataNode donde se almacenará la información
			
			choice := rand.Intn(3)
			datosJugador := str.Split(movidas[i], "-")
			
			dataToWrite := []byte("")
							
			if choice == 0{
				dataToWrite = []byte("Jugador_" + datosJugador[0] + " Ronda_" + ronda +  dataNode1 + "\n")
				enviarJugada(ronda, datosJugador[0], datosJugador[1], dataNode1)
			} else if choice == 1{
				dataToWrite = []byte("Jugador_" + datosJugador[0] + " Ronda_" + ronda +  dataNode2 + "\n")
				enviarJugada(ronda, datosJugador[0], datosJugador[1], dataNode2)
			} else{
				dataToWrite = []byte("Jugador_" + datosJugador[0] + " Ronda_" + ronda +  dataNode3 + "\n")
				enviarJugada(ronda, datosJugador[0], datosJugador[1], dataNode3)
			}

			f, err := os.OpenFile("index_jugadas/registro_jugadas_" + fechaLog + ".txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                        if err != nil {
        	                log.Fatal(err)
                        }
                        if _, err := f.Write(dataToWrite); err != nil {
                                log.Fatal(err)
                        }
                        if err := f.Close(); err != nil {
                                log.Fatal(err)
                        }
				
			
			fmt.Println(datosJugador)
		}
		retorno := &pb.Status{
			Status: "Jugadas almacenadas correctamente",
		}
		return retorno, nil
	} else{
		//Retornamos la falta de movimientos
		retorno := &pb.Status{
                        Status: "Ningún movimiento encontrado",
                }
                return retorno, nil
	}

}


func main() {
	//Iniciamos el servidor del NameNode
	go InitServer(port)

	fechaLog = (time.Now()).Format("2006_01_02_T_15_04_05")

	//Inicio de la interface
	fmt.Println("\n NameNode inicializado")

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


	//iniciamos la semilla

	rand.Seed(time.Now().UnixNano())

	var decision int
	fmt.Scan(&decision)

}



