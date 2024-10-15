// super_node.go
package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

var files = make(map[string]string) // Nome do arquivo e caminho local
var (
	superNodeID   = "SuperNode1"
	coordinatorID = "Master"
	allSuperNodes = []string{"SuperNode1", "SuperNode2", "SuperNode3"}
)

// Função para tratar upload de arquivos
func handleUpload(conn net.Conn, fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Fprintf(conn, "Erro ao criar arquivo: %v\n", err)
		return
	}
	defer file.Close()

	// Receber o arquivo do cliente
	_, err = io.Copy(file, conn)
	if err != nil {
		fmt.Fprintf(conn, "Erro ao salvar o arquivo: %v\n", err)
		return
	}

	// Armazenar no índice de arquivos
	files[fileName] = fileName
	fmt.Fprintf(conn, "Upload concluído\n")
}

// Função para tratar download de arquivos
func handleDownload(conn net.Conn, fileName string) {
	filePath, exists := files[fileName]
	if !exists {
		fmt.Fprintf(conn, "Arquivo não encontrado\n")
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(conn, "Erro ao abrir arquivo: %v\n", err)
		return
	}
	defer file.Close()

	// Enviar o arquivo ao cliente
	_, err = io.Copy(conn, file)
	if err != nil {
		fmt.Fprintf(conn, "Erro ao enviar o arquivo: %v\n", err)
		return
	}

	fmt.Println("Download concluído:", fileName)
}

// Função para tratar as requisições dos clientes (upload/download)
func handleClient(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Erro ao ler do cliente:", err)
		return
	}

	// Analisar o comando recebido do cliente
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

// Função para registrar o Super Nó no Nó Coordenador
func registerWithMaster() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("Erro ao conectar ao nó coordenador:", err)
		return
	}
	defer conn.Close()

	nodeID := "SuperNode1"
	nodeAddr := "localhost:8081"
	fmt.Fprint(conn, nodeID+" "+nodeAddr)
	fmt.Println("SuperNode registrado no coordenador.")
}

// Função para iniciar a eleição do coordenador usando o algoritmo do Valentão
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

// Função para verificar se o coordenador está ativo
func checkCoordinator() {
	for {
		time.Sleep(5 * time.Second)

		conn, err := net.Dial("tcp", "localhost:8080")
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
	go checkCoordinator() // Verifica constantemente o status do coordenador

	registerWithMaster() // Registra o super nó no nó coordenador

	// Inicia o servidor do super nó
	ln, err := net.Listen("tcp", ":8081")
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

		go handleClient(conn) // Tratar clientes em goroutines
	}
}
