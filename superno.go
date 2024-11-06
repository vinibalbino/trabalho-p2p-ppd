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
	files           = make(map[string]map[string]bool)
	superNodeID     = ""
	coordinatorIP   = "172.27.3.241" // IP do master_node
	coordinatorID   = "Master"
	knownSuperNodes = []string{} // IPs dos SuperNodes
	mu              sync.Mutex
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
				delete(files, fileName)
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

// Função para fazer broadcast aos demais super nós em busca do arquivo
func broadcastRequest(fileName string) (string, bool) {
	for _, superNodeAddr := range knownSuperNodes {
		conn, err := net.Dial("tcp", superNodeAddr)
		if err != nil {
			fmt.Printf("Erro ao conectar ao SuperNode %s: %v\n", superNodeAddr, err)
			continue
		}

		defer conn.Close()

		fmt.Printf("Conexão estabelecida com o SuperNode %s para busca do arquivo '%s'.\n", superNodeAddr, fileName)

		// Envia a requisição de busca
		_, writeErr := fmt.Fprintf(conn, "SEARCH %s\n", fileName)
		if writeErr != nil {
			fmt.Printf("Erro ao enviar pedido de busca ao SuperNode %s: %v\n", superNodeAddr, writeErr)
			continue
		}

		// Lê a resposta
		response := make([]byte, 1024)
		n, readErr := conn.Read(response)
		if readErr != nil {
			fmt.Printf("Erro ao ler resposta do SuperNode %s: %v\n", superNodeAddr, readErr)
			continue
		}

		// Verifica se o arquivo foi encontrado
		resp := strings.TrimSpace(string(response[:n]))
		if strings.HasPrefix(resp, "FOUND") {
			// Divide a resposta para extrair o IP
			parts := strings.Split(resp, " ")
			if len(parts) == 2 {
				ip := parts[1]
				fmt.Printf("Arquivo '%s' encontrado no SuperNode %s. IP: %s\n", fileName, superNodeAddr, ip)
				return ip, true
			} else {
				fmt.Printf("Erro: Resposta de formato inesperado do SuperNode %s: %s\n", superNodeAddr, resp)
			}
		} else {
			fmt.Printf("Arquivo '%s' não encontrado no SuperNode %s.\n", fileName, superNodeAddr)
		}
	}
	return "", false
}

func handleDownload(conn net.Conn, fileName string) {
	baseFileName := filepath.Base(fileName)
	requestingIP := strings.Split(conn.RemoteAddr().String(), ":")[0]

	mu.Lock()
	clients, exists := files[baseFileName]
	fmt.Printf("Debug: Verificando existência do arquivo '%s' localmente...\n", baseFileName)
	if !exists || len(clients) == 0 {
		mu.Unlock()
		fmt.Printf("Debug: Arquivo '%s' não encontrado localmente. Iniciando broadcast.\n", baseFileName)

		// Tenta broadcast para outros super nós
		if otherSuperNodeIP, found := broadcastRequest(baseFileName); found {
			logMessage := fmt.Sprintf("O arquivo '%s' está disponível no cliente com IP: %s\n", baseFileName, otherSuperNodeIP)
			fmt.Print(logMessage)
			if _, err := fmt.Fprintf(conn, logMessage); err != nil {
				fmt.Printf("Erro ao enviar resposta ao cliente %s: %v\n", requestingIP, err)
			}
		} else {
			errorMessage := fmt.Sprintf("ERROR: Arquivo '%s' não encontrado em nenhum super nó\n", baseFileName)
			fmt.Print(errorMessage)
			fmt.Fprintf(conn, errorMessage)
		}
		return
	}

	// Arquivo encontrado localmente
	fmt.Printf("Debug: Arquivo '%s' encontrado localmente. Selecionando cliente para resposta.\n", baseFileName)
	var ipClient string
	for clientIP := range clients {
		ipClient = strings.TrimSpace(clientIP) // Sanitiza o IP removendo espaços extras
		break
	}

	// Adiciona o cliente solicitante ao mapa, indicando que agora possui o arquivo
	files[baseFileName][requestingIP] = true
	mu.Unlock()

	// Verifica e sanitiza o IP antes de enviar a resposta
	if net.ParseIP(ipClient) == nil {
		fmt.Printf("Erro: IP '%s' do cliente não é válido\n", ipClient)
		fmt.Fprintf(conn, "ERROR: IP do cliente com o arquivo é inválido\n")
		return
	}

	// Envia resposta ao cliente solicitante
	responseMessage := fmt.Sprintf("O arquivo '%s' está disponível no cliente com IP: %s\n", baseFileName, ipClient)
	fmt.Print(responseMessage)
	if _, err := fmt.Fprintf(conn, responseMessage); err != nil {
		fmt.Printf("Erro ao enviar resposta ao cliente %s: %v\n", requestingIP, err)
	}
}

func handleClient(conn net.Conn) {
	clientIP := strings.Split(conn.RemoteAddr().String(), ":")[0]

	defer func() {
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
		case "SEARCH":
			handleSearch(conn, fileName)
		case "CLOSE":
			conn.Close()
			return
		default:
			fmt.Fprintf(conn, "Comando inválido\n")
		}
	}
}

func handleSearch(conn net.Conn, fileName string) {
	mu.Lock()
	defer mu.Unlock()

	// Verifica se o arquivo existe localmente e retorna o IP do super nó
	if clients, exists := files[fileName]; exists {
		// Pega o primeiro cliente que possui o arquivo para enviar seu IP
		var ipClient string
		for clientIP := range clients {
			ipClient = clientIP
			break
		}
		fmt.Fprintf(conn, "FOUND %s\n", ipClient)
		fmt.Printf("Arquivo '%s' encontrado localmente, no cliente '%s' e respondido ao nó solicitante.\n", fileName, ipClient)
	} else {
		fmt.Fprintf(conn, "NOTFOUND\n")
		fmt.Printf("Arquivo '%s' não encontrado localmente.\n", fileName)
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
	for _, nodeID := range knownSuperNodes {
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
			defer conn.Close()
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
			go receiveBroadcast()
			return true
		}
		fmt.Println("Mensagem recebida diferente de 'FINALIZED'. Aguardando...")
		time.Sleep(5 * time.Second) // Aguardar antes de tentar novamente
	}
}

func receiveBroadcast() {
	for {
		ln, err := net.Listen("tcp", ":8084")

		if err != nil {
			fmt.Println("Erro ao iniciar listener para broadcast:", err)
			return
		}
		defer ln.Close()

		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão de broadcast:", err)
			return
		}
		defer conn.Close()

		// Lê a lista de super nós enviada pelo master node
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil && err != io.EOF {
			fmt.Println("Erro ao ler lista de super nós:", err)
			return
		}

		// Armazena a lista de super nós conhecidos
		mu.Lock()
		knownSuperNodes = strings.Split(strings.TrimSpace(string(buf[:n])), ",")
		mu.Unlock()

		fmt.Printf("SuperNode recebeu lista de super nós: %v\n", knownSuperNodes)

	}
}

func main() {
	registerWithMaster()
	released := false
	for released == false {
		time.Sleep(3 * time.Second)
		released = awaitMasterRelease()
	}

	go checkCoordinator() // Inicia verificação do coordenador em uma goroutine
	time.Sleep(2 * time.Second)

	// Inicia o servidor para aceitar clientes
	ln, err := net.Listen("tcp", "0.0.0.0:8082")
	if err != nil {
		fmt.Println("Erro ao iniciar o super nó:", err)
		return
	}
	defer ln.Close()

	fmt.Println("Super nó aguardando clientes...")

	time.Sleep(2 * time.Second)
	go receiveBroadcast()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão:", err)
			continue
		}
		go handleClient(conn)
	}
}
