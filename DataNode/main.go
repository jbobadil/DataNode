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
	pb "DataNode/proto"
	"time"
)

const (
	port = ":50052"
)

// Server
type server struct {
	pb.UnimplementedStartServerServer
}

var nombreCarpetaLogs string

func InitServer(port string){
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

func (s *server) DataNodeStoreMove(ctx context.Context, datosMovimiento *pb.Playermove ) (*pb.Status, error){

	if len(datosMovimiento.Moves) != 0{
		fmt.Println("Recibidos movimientos del jugador ", datosMovimiento.Player)
		fmt.Println("Generando registro jugador_" + datosMovimiento.Player +"__ronda_" +datosMovimiento.Round)
		
		movimientos := str.Split(datosMovimiento.Moves, ",")		
		stringToWrite := ""
		for i:=0; i<len(movimientos); i++ {
			stringToWrite = stringToWrite + movimientos[i] + "\n"
		}
		dataToWrite := []byte(stringToWrite)

		f, err := os.OpenFile(nombreCarpetaLogs + "/jugador_" + datosMovimiento.Player +"__ronda_" +datosMovimiento.Round + ".txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err != nil {
                	log.Fatal(err)
                }
               	if _, err := f.Write(dataToWrite); err != nil {
       	                log.Fatal(err)
                }
                if err := f.Close(); err != nil {
                	log.Fatal(err)
                }
		retorno := &pb.Status{
                        Status: "Movimientos almacenados correctamente",
                }
                return retorno, nil
	} else{
		retorno := &pb.Status{
                        Status: "No existen movimientos a guardar",
                }
                return retorno, nil
	}

}

func main() {
        //Iniciamos el servidor del NameNode
        go InitServer(port)
	
	nombreCarpetaLogs = "archivo_logs/data_from_" + (time.Now()).Format("2006_01_02_T_15_04_05")
        fmt.Println(nombreCarpetaLogs)
        err := os.Mkdir(nombreCarpetaLogs, 0754)

	if err != nil {
		panic(err)
	}       

	 //Inicio de la interface
        fmt.Println("DateNode inicializado")


        //iniciamos la semilla

        rand.Seed(time.Now().UnixNano())

        var closeCondition int
        fmt.Scan(&closeCondition)
}



