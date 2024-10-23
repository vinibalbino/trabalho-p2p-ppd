package main

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	files         = make(map[string]string) // Armazena o nome do arquivo e o IP do cliente que o possui
	superNodeID   = "SuperNode1"
	coordinatorIP = "172.26.4.220" // IP do master_node (modifique para o IP correto)
	coordinatorID = "Master"
	allSuperNodes = []string{"SuperNode1", "SuperNode2", "SuperNode3"}

	mu sync.Mutex // Protege o mapa contra condições de corrida
)

func handleUpload(conn net.Conn, fileName string) {
	baseFileName := filepath.Base(fileName)

	// Captura o IP do cliente
	ipClient := strings.Split(conn.RemoteAddr().String(), ":")[0]

	// Log para rastrear o início do upload
	fmt.Printf("Iniciando upload do arquivo '%s' do cliente %s\n", baseFileName, ipClient)

	// Bloqueia o acesso ao mapa antes de modificar
	mu.Lock()
	files[baseFileName] = ipClient // Armazena o IP do cliente, substituindo o valor antigo (se houver)
	mu.Unlock() // Desbloqueia o mapa após a modificação

	// Exibe os arquivos armazenados para conferência
	fmt.Printf("Arquivo '%s' registrado com o cliente %s. Estado atual dos arquivos: %v\n", baseFileName, ipClient, files)

	// Envia a confirmação de upload para o cliente e verifica se houve erro
	if _, err := fmt.Fprintf(conn, "Upload registrado no super nó.\n"); err != nil {
		fmt.Printf("Erro ao enviar resposta de confirmação ao cliente %s: %v\n", ipClient, err)
		return
	}

	fmt.Printf("Upload do arquivo '%s' do cliente %s concluído com sucesso.\n", baseFileName, ipClient)
}

func handleDownload(conn net.Conn, fileName string) {

	baseFileName := filepath.Base(fileName)

	// Bloqueia o acesso ao mapa antes de ler
	mu.Lock()
	ipClient, exists := files[baseFileName] // Pega o IP do cliente que possui o arquivo
	mu.Unlock()

	if !exists {
		// Verifica se o arquivo está registrado
		fmt.Fprintf(conn, "ERROR: Arquivo '%s' não encontrado\n", baseFileName)
		fmt.Printf("Arquivo '%s' solicitado, mas não encontrado no servidor.\n", baseFileName)
		return
	}

	// Envia o IP do cliente que possui o arquivo
	fmt.Fprintf(conn, "O arquivo '%s' está disponível no cliente com IP: %s\n", baseFileName, ipClient)
	fmt.Printf("Informações do arquivo '%s' enviadas para o cliente solicitante.\n", baseFileName)

}

func handleClient(conn net.Conn) {
	for conn != nil {
		buf := make([]byte, 1024) // Buffer para leitura de dados
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Erro ao ler do cliente:", err)
			return
		}

		// Extrai e trata a mensagem recebida do cliente
		input := strings.TrimSpace(string(buf[:n]))
		parts := strings.Split(input, " ")
		if len(parts) < 2 {
			fmt.Fprintf(conn, "Comando inválido\n")
			return
		}

		command, fileName := parts[0], parts[1]

		// Processa o comando recebido
		switch command {
		case "UPLOAD":
			handleUpload(conn, fileName)
		case "DOWNLOAD":
			handleDownload(conn, fileName)
		case "CLOSE":
			conn.Close()
		default:
			fmt.Fprintf(conn, "Comando inválido\n")
		}
	}
}

func registerWithMaster() {
	conn, err := net.Dial("tcp", coordinatorIP+":8081")
	if err != nil {
		fmt.Println("Erro ao conectar ao nó coordenador:", err)
		return
	}
	defer conn.Close()

	nodeID := "SuperNode1"
	nodeAddr := "172.26.4.202:8081" // IP da máquina super nó (modifique para o IP correto)
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

		conn, err := net.Dial("tcp", coordinatorIP+":8081")
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
