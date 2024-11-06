// unified_node.go
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

// Estrutura para SuperNode
type SuperNode struct {
	ID   int
	Addr string
}

const registerPort = ":8080"

// Porta para enviar mensagem de liberação
const releasePort = ":8081"

const broadcastPort = ":8084"

// Variáveis Globais e Mutexes para sincronização
var (
	isMaster        = true
	superNodeID     = ""
	superNodes      = make(map[int]SuperNode)
	files           = make(map[string]map[string]bool)
	contSuperNodes  = 0
	contToSucess    = 0
	knownSuperNodes = []string{}
	coordinatorIP   = "172.27.3.241"
	coordinatorID   = "Master"
	mu              sync.Mutex
)

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

func handleSuperNodeRegistration(conn net.Conn, nodeId int) {
	defer conn.Close()

	// Extrai o endereço IP do SuperNode
	superNodeAddress := strings.Split(conn.RemoteAddr().String(), ":")[0]
	fmt.Printf("SuperNo: %d Addr: %s\n", nodeId, superNodeAddress)

	// Envia o ID do SuperNode como string
	_, err := fmt.Fprintf(conn, "%d", nodeId)
	if err != nil {
		fmt.Printf("Erro ao enviar nodeId para o SuperNode %d: %v\n", nodeId, err)
		return
	}

	// Guarda o endereço do SuperNode
	mu.Lock()
	mu.Unlock()

	// Recebe confirmação do SuperNode
	buf := make([]byte, 1024)
	n, responseError := conn.Read(buf)
	if responseError != nil {
		fmt.Printf("Erro ao ler confirmação do SuperNode %d: %v\n", nodeId, responseError)
		return
	}

	message := strings.TrimSpace(string(buf[:n]))

	// Verifica se o SuperNode enviou "ACK"
	if message == "ACK" {
		mu.Lock()
		contToSucess++
		superNodes[nodeId] = SuperNode{ID: nodeId, Addr: superNodeAddress}
		fmt.Printf("ACK recebido de NodeId %d\n", nodeId)
		// Se todos os SuperNodes confirmaram, libera a comunicação
		mu.Unlock()
	}
}

func freeSuperNodes() {
	time.Sleep(5 * time.Second)

	fmt.Println("Todos os super nós registrados. Liberando para comunicação...")

	mu.Lock()
	defer mu.Unlock()

	for _, superNode := range superNodes {
		freeNode(superNode)
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

func broadcastSuperNodes() {
	time.Sleep(2 * time.Second)
	mu.Lock()

	for _, superNode := range superNodes {
		conn, err := net.Dial("tcp", superNode.Addr+broadcastPort)

		if err != nil {
			fmt.Printf("Erro ao conectar ao SuperNode %d para enviar broadcast: %v\n", superNode.ID, err)
			continue
		}

		// Cria uma lista dos endereços IP de todos os super nós, separada por vírgulas
		var nodeList []string
		for _, node := range superNodes {
			nodeList = append(nodeList, node.Addr)
		}

		// Envia a lista de super nós para o super nó atual
		_, err = conn.Write([]byte(strings.Join(nodeList, ",")))
		if err != nil {
			fmt.Printf("Erro ao enviar lista para SuperNode %d: %v\n", superNode.ID, err)
		}

		_ = conn.Close()
	}
	mu.Unlock()
}

func freeNode(superNode SuperNode) {
	conn, err := net.Dial("tcp", superNode.Addr+releasePort)
	if err != nil {
		fmt.Printf("Erro ao conectar ao SuperNode %d: %v\n", superNode.ID, err)
		return
	}

	_, err = conn.Write([]byte("FINALIZED"))
	if err != nil {
		fmt.Printf("Erro ao enviar mensagem para SuperNode %d: %v\n", superNode.ID, err)
	} else {
		fmt.Printf("Mensagem de liberação enviada para SuperNode %d\n", superNode.ID)
	}
	_ = conn.Close()
}

// Funções para Registrar e Gerenciar Arquivos para Clientes
func removeClientFiles(clientIP string) {
	mu.Lock()
	defer mu.Unlock()
	for fileName, clients := range files {
		if clients[clientIP] {
			delete(clients, clientIP)
			if len(clients) == 0 {
				delete(files, fileName)
			}
		}
	}
}

func handleUpload(conn net.Conn, fileName string) {
	baseFileName := filepath.Base(fileName)
	ipClient := strings.Split(conn.RemoteAddr().String(), ":")[0]

	mu.Lock()
	if files[baseFileName] == nil {
		files[baseFileName] = make(map[string]bool)
	}
	files[baseFileName][ipClient] = true
	mu.Unlock()

	fmt.Printf("Upload do arquivo '%s' registrado.\n", baseFileName)
	if _, err := fmt.Fprintf(conn, "Upload registrado no super nó.\n"); err != nil {
		fmt.Printf("Erro ao enviar confirmação: %v\n", err)
	}
}

func handleDownload(conn net.Conn, fileName string) {
	baseFileName := filepath.Base(fileName)
	requestingIP := strings.Split(conn.RemoteAddr().String(), ":")[0]

	mu.Lock()
	clients, exists := files[baseFileName]
	if !exists || len(clients) == 0 {
		mu.Unlock()

		if otherSuperNodeIP, found := broadcastRequest(baseFileName); found {
			fmt.Fprintf(conn, "O arquivo '%s' está no cliente com IP: %s\n", baseFileName, otherSuperNodeIP)
		} else {
			fmt.Fprintf(conn, "ERROR: Arquivo '%s' não encontrado\n", baseFileName)
		}
		return
	}

	var ipClient string
	for clientIP := range clients {
		ipClient = strings.TrimSpace(clientIP)
		break
	}

	files[baseFileName][requestingIP] = true
	mu.Unlock()

	if net.ParseIP(ipClient) == nil {
		fmt.Fprintf(conn, "ERROR: IP do cliente é inválido\n")
		return
	}

	fmt.Fprintf(conn, "O arquivo '%s' está no cliente com IP: %s\n", baseFileName, ipClient)
}

func handleClient(conn net.Conn) {
	clientIP := strings.Split(conn.RemoteAddr().String(), ":")[0]
	defer func() {
		removeClientFiles(clientIP)
		conn.Close()
	}()

	for conn != nil {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
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
		default:
			fmt.Fprintf(conn, "Comando inválido\n")
		}
	}
}

// Funções de Registro e Eleição
func registerWithMaster() {
	conn, err := net.Dial("tcp", coordinatorIP+":8080")
	if err != nil {
		fmt.Println("Erro ao conectar ao nó coordenador:", err)
		return
	}
	defer conn.Close()

	buf := make([]byte, 1024)
	n, responseError := conn.Read(buf)
	if responseError != nil {
		fmt.Fprint(conn, "NACK")
		return
	}

	superNodeID = strings.TrimSpace(string(buf[:n]))
	fmt.Println("SuperNode registrado com ID:", superNodeID)
	fmt.Fprintf(conn, "%s", "ACK")
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

func startElection() {
	fmt.Println("Iniciando eleição...")
	highestID := superNodeID
	for _, nodeID := range knownSuperNodes {
		if nodeID > highestID {
			highestID = nodeID
		}
	}
	if highestID == superNodeID {
		fmt.Println("SuperNode venceu a eleição.")
		coordinatorID = superNodeID
	}
}

// Gerenciamento de Broadcast e Eleição
func broadcastRequest(fileName string) (string, bool) {
	for _, superNodeAddr := range knownSuperNodes {
		conn, err := net.Dial("tcp", superNodeAddr+":8084")
		if err != nil {
			continue
		}
		defer conn.Close()

		_, writeErr := fmt.Fprintf(conn, "SEARCH %s\n", fileName)
		if writeErr != nil {
			continue
		}

		response := make([]byte, 1024)
		n, readErr := conn.Read(response)
		if readErr != nil {
			continue
		}

		resp := strings.TrimSpace(string(response[:n]))
		if strings.HasPrefix(resp, "FOUND") {
			parts := strings.Split(resp, " ")
			if len(parts) == 2 {
				return parts[1], true
			}
		}
	}
	return "", false
}

func addressAlreadyExist(address string) bool {
	for _, node := range superNodes {
		if node.Addr == address {
			return true
		}
	}
	return false
}

// Funções de Inicialização e Main
func initializeNodeType() {
	if isMaster {
		ln, err := net.Listen("tcp", registerPort)
		if err != nil {
			fmt.Println("Erro ao iniciar o servidor de registro:", err)
			return
		}

		fmt.Println("Nó coordenador aguardando registros dos super nós...")

		for contSuperNodes < 3 {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println("Erro ao aceitar conexão de registro:", err)
				continue
			}
			go handleSuperNodeRegistration(conn, contSuperNodes)
			contSuperNodes++
		}

		go freeSuperNodes() // Executa freeSuperNodes em goroutine para evitar bloqueio
		time.Sleep(6 * time.Second)
		go broadcastSuperNodes()

		defer ln.Close()
		for {
			time.Sleep(1 * time.Second)
			conn, err := ln.Accept()
			nodeAddress := strings.Split(conn.RemoteAddr().String(), ":")[0]
			exist := addressAlreadyExist(nodeAddress)
			if !exist {
				if err != nil {
					fmt.Println("Erro ao aceitar conexão de registro:", err)
					continue
				}
				go handleSuperNodeRegistration(conn, contSuperNodes)
				time.Sleep(5 * time.Second)
				freeNode(superNodes[contSuperNodes])
				contSuperNodes++
				broadcastSuperNodes()
			}
		}
	} else {
		// Lógica específica para SuperNode
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
}

func main() {
	initializeNodeType()
}
