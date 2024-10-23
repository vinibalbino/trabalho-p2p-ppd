// super_node.go
package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	files         = make(map[string][]string)
	superNodeID   = "SuperNode1"
	coordinatorIP = "192.168.100.11" // IP do master_node (modifique para o IP correto)
	coordinatorID = "Master"
	allSuperNodes = []string{"SuperNode1", "SuperNode2", "SuperNode3"}

	mu sync.Mutex // Protege o mapa contra condições de corrida
)

func handleUpload(conn net.Conn, fileName string) {
	defer conn.Close() // Fecha a conexão ao final
	baseFileName := filepath.Base(fileName)

	// Captura o IP do cliente
	ipClient := strings.Split(conn.RemoteAddr().String(), ":")[0]

	// Bloqueia o acesso ao mapa antes de modificar
	mu.Lock()
	if files[baseFileName] == nil {
		files[baseFileName] = []string{}
	}

	files[baseFileName] = append(files[baseFileName], ipClient, fileName)
	mu.Unlock() // Desbloqueia o mapa após a modificação

	// Exibe os arquivos armazenados
	fmt.Println(files)

	// Envia a confirmação de upload para o cliente
	if _, err := fmt.Fprintf(conn, "Upload concluído\n"); err != nil {
		fmt.Printf("Erro ao enviar resposta para o cliente: %v\n", err)
	}
	fmt.Printf("Arquivo '%s' armazenado no servidor com caminho.\n", baseFileName)
}

func handleDownload(conn net.Conn, fileName string) {
	defer conn.Close() // Garante que a conexão será fechada no final

	baseFileName := filepath.Base(fileName)

	// Bloqueia o acesso ao mapa antes de ler
	mu.Lock()
	fileInfo, exists := files[baseFileName]
	mu.Unlock()

	if !exists || len(fileInfo) < 2 {
		// Verifica se o arquivo existe e se o caminho está presente no slice
		fmt.Fprintf(conn, "ERROR: Arquivo '%s' não encontrado\n", baseFileName)
		fmt.Printf("Arquivo '%s' solicitado, mas não encontrado no servidor.\n", baseFileName)
		return
	}

	// O segundo elemento do slice 'fileInfo' contém o caminho completo do arquivo
	filePath := fileInfo[1]

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Erro ao abrir arquivo: %v\n", err)
		fmt.Printf("Erro ao abrir o arquivo '%s' para download: %v\n", baseFileName, err)
		return
	}
	defer file.Close()

	// Obtenção de informações do arquivo
	fileInfoStat, err := file.Stat()
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Erro ao obter informações do arquivo: %v\n", err)
		fmt.Printf("Erro ao obter informações do arquivo '%s': %v\n", baseFileName, err)
		return
	}

	// Envia o tamanho do arquivo ao cliente
	fileSize := fileInfoStat.Size()
	fmt.Fprintf(conn, "%d\n", fileSize)

	// Envia o conteúdo do arquivo
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
	nodeAddr := "192.168.100.52:8081" // IP da máquina super nó (modifique para o IP correto)
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

	// go checkCoordinator()
	// Iniciar coordenador
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
