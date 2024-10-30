// super_node.go
package main

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	files         = make(map[string]map[string]bool)
	superNodeID   = ""
	coordinatorIP = "172.27.3.241" // IP do master_node
	coordinatorID = "Master"
	allSuperNodes = []string{"SuperNode1", "SuperNode2", "SuperNode3"}
	mu            sync.Mutex
	isSuperNO
)

// Remove todos os arquivos pertencentes a um cliente desconectado
func removeClientFiles(clientIP string) {
	mu.Lock()
	defer mu.Unlock()
	for fileName, clients := range files {
		if clients[clientIP] {
			delete(clients, clientIP)
			fmt.Printf("Cliente %s removido do mapa para o arquivo '%s'.\n", clientIP, fileName)
			if len(clients) == 0 {
				delete(files, fileName) // Remove a entrada do arquivo se nenhum cliente o possuir
			}
		}
	}
}

func handleUpload(conn net.Conn, fileName string) {
	baseFileName := filepath.Base(fileName)
	ipClient := strings.Split(conn.RemoteAddr().String(), ":")[0]

	fmt.Printf("Iniciando upload do arquivo '%s' do cliente %s\n", baseFileName, ipClient)

	mu.Lock()
	if files[baseFileName] == nil {
		files[baseFileName] = make(map[string]bool)
	}
	files[baseFileName][ipClient] = true
	mu.Unlock()

	fmt.Printf("Arquivo '%s' registrado com o cliente %s. Estado atual dos arquivos: %v\n", baseFileName, ipClient, files)

	if _, err := fmt.Fprintf(conn, "Upload registrado no super nó.\n"); err != nil {
		fmt.Printf("Erro ao enviar resposta de confirmação ao cliente %s: %v\n", ipClient, err)
		return
	}

	fmt.Printf("Upload do arquivo '%s' do cliente %s concluído com sucesso.\n", baseFileName, ipClient)
}

func handleDownload(conn net.Conn, fileName string) {
	baseFileName := filepath.Base(fileName)
	requestingIP := strings.Split(conn.RemoteAddr().String(), ":")[0]

	mu.Lock()
	clients, exists := files[baseFileName]
	if !exists || len(clients) == 0 {
		mu.Unlock()
		fmt.Fprintf(conn, "ERROR: Arquivo '%s' não encontrado\n", baseFileName)
		fmt.Printf("Arquivo '%s' solicitado, mas não encontrado no servidor.\n", baseFileName)
		return
	}

	// Pega o primeiro cliente que possui o arquivo para enviar seu IP
	var ipClient string
	for clientIP := range clients {
		ipClient = clientIP
		break
	}

	// Adiciona o IP do cliente solicitante ao mapa, indicando que ele agora possui o arquivo
	files[baseFileName][requestingIP] = true
	mu.Unlock()

	fmt.Fprintf(conn, "O arquivo '%s' está disponível no cliente com IP: %s\n", baseFileName, ipClient)
	fmt.Printf("Informações do arquivo '%s' enviadas para o cliente solicitante.\n", baseFileName)
}

func handleClient(conn net.Conn) {
	clientIP := strings.Split(conn.RemoteAddr().String(), ":")[0]

	defer func() {
		// Remove os arquivos do cliente ao desconectar
		fmt.Printf("Cliente %s desconectado, removendo seus arquivos.\n", clientIP)
		removeClientFiles(clientIP)
		conn.Close()
	}()

	for conn != nil {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("Conexão encerrada pelo cliente %s\n", clientIP)
			} else {
				fmt.Println("Erro ao ler do cliente:", err)
			}
			return
		}

		input := strings.TrimSpace(string(buf[:n]))
		parts := strings.Split(input, " ")
		if len(parts) < 2 {
			fmt.Fprintf(conn, "Comando inválido\n")
			continue
		}

		command, fileName := parts[0], parts[1]

		switch command {
		case "UPLOAD":
			handleUpload(conn, fileName)
		case "DOWNLOAD":
			handleDownload(conn, fileName)
		case "CLOSE":
			conn.Close()
			return
		default:
			fmt.Fprintf(conn, "Comando inválido\n")
		}
	}
}

func registerWithMaster() {
	conn, err := net.Dial("tcp", coordinatorIP+":8080")
	if err != nil {
		fmt.Println("Erro ao conectar ao nó coordenador:", err)
		return
	}

	defer conn.Close()

	// Recebe o identificador do super nó do coordenador
	buf := make([]byte, 1024)
	n, responseError := conn.Read(buf)
	if responseError != nil {
		fmt.Println("Erro ao receber chave identificadora")
		fmt.Fprint(conn, "NACK")
		return
	}

	superNodeID = strings.TrimSpace(string(buf[:n]))
	fmt.Println("SuperNode registrado com ID:", superNodeID)

	// Envia confirmação de registro ao coordenador
	fmt.Fprintf(conn, "%s", "ACK")
	return
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
		fmt.Println("SuperNode venceu a eleição e se tornou o novo coordenador.")
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

func awaitMasterRelease() bool {

	ln, err := net.Listen("tcp", ":8081") // Escuta na porta 8081 uma única vez
	if err != nil {
		fmt.Println("Erro ao iniciar listener para receber liberação do coordenador:", err)
		return false
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão de liberação:", err)
			time.Sleep(5 * time.Second)
			continue // Tenta novamente se houver um erro de aceitação
		}
		// Aguarda pela mensagem de liberação do coordenador
		newBuffer := make([]byte, 1024)
		n, messageError := conn.Read(newBuffer)
		if messageError != nil {
			fmt.Println("Erro ao ler mensagem do coordenador:", messageError)
			time.Sleep(5 * time.Second)
			continue // Tenta novamente se ocorrer erro de leitura
		}

		// Verifica se a mensagem recebida é "FINALIZED"
		if strings.TrimSpace(string(newBuffer[:n])) == "FINALIZED" {
			fmt.Println("SuperNode liberado pelo coordenador para iniciar comunicações.")
			return true
		}
		fmt.Println("Mensagem recebida diferente de 'FINALIZED'. Aguardando...")
		time.Sleep(5 * time.Second) // Aguardar antes de tentar novamente
	}
}

func main() {
	// Registra o super nó com o coordenador
	registerWithMaster()

	released := false
	for released == false {
		time.Sleep(5 * time.Second)
		released = awaitMasterRelease()
	}

	go checkCoordinator() // Inicia verificação do coordenador em uma goroutine

	// Inicia o servidor para aceitar clientes
	ln, err := net.Listen("tcp", "0.0.0.0:8082")
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
