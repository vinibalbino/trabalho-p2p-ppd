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
	files         = make(map[string]map[string]bool) // Armazena o nome do arquivo e os IPs dos clientes que o possuem
	superNodeID   = "SuperNode1"
	coordinatorIP = "172.26.4.150" // IP do master_node (modifique para o IP correto)
	coordinatorID = "Master"
	allSuperNodes = []string{"SuperNode1", "SuperNode2", "SuperNode3"}

	mu sync.Mutex // Protege o mapa contra condições de corrida
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
	conn, err := net.Dial("tcp", coordinatorIP+":8081")
	if err != nil {
		fmt.Println("Erro ao conectar ao nó coordenador:", err)
		return
	}
	defer conn.Close()

	nodeID := "SuperNode1"
	nodeAddr := "172.26.4.202:8081"
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
	registerWithMaster()

	ln, err := net.Listen("tcp", "0.0.0.0:8081")
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
