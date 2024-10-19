// super_node.go
package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var files = make(map[string]string)

var (
	superNodeID   = "SuperNode1"
	coordinatorIP = "192.168.100.11" // IP do master_node (modifique para o IP correto)
	coordinatorID = "Master"
	allSuperNodes = []string{"SuperNode1", "SuperNode2", "SuperNode3"}
)

func handleUpload(conn net.Conn, fileName string) {
	baseFileName := filepath.Base(fileName)
	serverFilePath := "./" + baseFileName

	file, err := os.Create(serverFilePath)
	if err != nil {
		fmt.Fprintf(conn, "Erro ao criar arquivo: %v\n", err)
		fmt.Printf("Erro ao criar arquivo '%s' no servidor: %v\n", baseFileName, err)
		return
	}
	defer file.Close()

	_, err = io.Copy(file, conn)
	if err != nil {
		fmt.Fprintf(conn, "Erro ao salvar o arquivo: %v\n", err)
		fmt.Printf("Erro ao salvar o arquivo '%s' no servidor: %v\n", baseFileName, err)
		return
	}

	files[baseFileName] = serverFilePath
	fmt.Fprintf(conn, "Upload concluído\n")
	fmt.Printf("Arquivo '%s' armazenado no servidor com caminho '%s'.\n", baseFileName, serverFilePath)
}

func handleDownload(conn net.Conn, fileName string) {
	baseFileName := filepath.Base(fileName)
	filePath, exists := files[baseFileName]
	if !exists {
		fmt.Fprintf(conn, "ERROR: Arquivo '%s' não encontrado\n", baseFileName)
		fmt.Printf("Arquivo '%s' solicitado, mas não encontrado no servidor.\n", baseFileName)
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Erro ao abrir arquivo: %v\n", err)
		fmt.Printf("Erro ao abrir o arquivo '%s' para download: %v\n", baseFileName, err)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Erro ao obter informações do arquivo: %v\n", err)
		fmt.Printf("Erro ao obter informações do arquivo '%s': %v\n", baseFileName, err)
		return
	}

	fileSize := fileInfo.Size()
	fmt.Fprintf(conn, "%d\n", fileSize)

	_, err = io.Copy(conn, file)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Erro ao enviar o arquivo: %v\n", err)
		fmt.Printf("Erro ao enviar o arquivo '%s': %v\n", baseFileName, err)
		return
	}

	fmt.Printf("Download do arquivo '%s' concluído com sucesso.\n", baseFileName)
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Erro ao ler do cliente:", err)
		return
	}

	input := strings.TrimSpace(string(buf[:n]))
	parts := strings.Split(input, " ")
	if len(parts) < 2 {
		fmt.Fprintf(conn, "Comando inválido\n")
		return
	}
	command, fileName := parts[0], parts[1]

	switch command {
	case "UPLOAD":
		handleUpload(conn, fileName)
	case "DOWNLOAD":
		handleDownload(conn, fileName)
	default:
		fmt.Fprintf(conn, "Comando inválido\n")
	}
}

func registerWithMaster() {
	conn, err := net.Dial("tcp", coordinatorIP+":8080")
	if err != nil {
		fmt.Println("Erro ao conectar ao nó coordenador:", err)
		return
	}
	defer conn.Close()

	nodeID := "SuperNode1"
	nodeAddr := "192.168.100.10:8081" // IP da máquina super nó (modifique para o IP correto)
	fmt.Fprint(conn, nodeID+" "+nodeAddr)
	fmt.Println("SuperNode registrado no coordenador.")
}

func startElection() {
	fmt.Println("Iniciando eleição do valentão...")

	highestID := superNodeID
	for _, nodeID := range allSuperNodes {
		if nodeID > highestID {
			highestID = nodeID
		}
	}

	if highestID == superNodeID {
		fmt.Println("SuperNode1 venceu a eleição e se tornou o novo coordenador.")
		coordinatorID = superNodeID
	}
}

func checkCoordinator() {
	for {
		time.Sleep(5 * time.Second)

		conn, err := net.Dial("tcp", coordinatorIP+":8080")
		if err != nil {
			fmt.Println("Coordenador não está respondendo. Iniciando eleição...")
			startElection()
		} else {
			conn.Close()
			fmt.Println("Coordenador está ativo.")
		}
	}
}

func main() {
	go checkCoordinator()

	registerWithMaster()

	ln, err := net.Listen("tcp", "0.0.0.0:8081") // Permitindo conexões externas
	if err != nil {
		fmt.Println("Erro ao iniciar o super nó:", err)
		return
	}
	defer ln.Close()

	fmt.Println("Super nó aguardando clientes...")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão:", err)
			continue
		}

		go handleClient(conn)
	}
}
