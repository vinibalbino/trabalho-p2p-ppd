// master_node.go
package main

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SuperNode struct {
	ID   int
	Addr string
}

const (
	registerPort  = ":8080"
	releasePort   = ":8081"
	clientPort    = ":8082"
	broadcastPort = ":8084"
	electionPort  = ":8085"
)

var (
	isMaster = false
	mu       sync.Mutex

	superNodes     = make(map[int]SuperNode)
	contSuperNodes = 0
	contToSucess   = 0

	files              = make(map[string]map[string]bool)
	superNodeID        = ""
	coordinatorIP      = "172.27.3.241" // IP do master_node
	coordinatorID      = "Master"
	knownSuperNodes    = []string{} // IPs dos SuperNodes
	electionInProgress = false
)

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

func freeSuperNodes() {
	time.Sleep(5 * time.Second)

	fmt.Println("Todos os super nós registrados. Liberando para comunicação...")

	mu.Lock()
	defer mu.Unlock()

	for _, superNode := range superNodes {
		freeNode(superNode)
	}
}

func addressAlreadyExist(address string) bool {
	for _, node := range superNodes {
		if node.Addr == address {
			return true
		}
	}
	return false
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
		conn, err := net.Dial("tcp", superNodeAddr+clientPort)
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
		if isMaster {
			conn.Close()
			return
		}

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
	conn, err := net.Dial("tcp", coordinatorIP+registerPort)
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

func handleElection() {
	ln, _ := net.Listen("tcp", electionPort)
	defer ln.Close()
	for electionInProgress {
		conn, err := ln.Accept()
		if err != nil {
			print("erro ao receber mensagem de um superno")
		}

		response := make([]byte, 1024)
		n, err := conn.Read(response)
		if err != nil {
			fmt.Println("Erro ao ler resposta de eleição:", err)
			continue
		}
		// Tratamento da mensagem de eleição
		myId, _ := strconv.Atoi(superNodeID)
		fmt.Println("\n" + string(response[:n]) + "\n")
		idNode, _ := strconv.Atoi(strings.Split(string(response[:n]), " ")[1])
		fmt.Printf("\nRecebido %d do superno %s\n", idNode, conn.RemoteAddr().String())

		if idNode > myId {
			fmt.Fprint(conn, "OUT")

		} else {
			fmt.Fprint(conn, "OK")
		}

		_ = conn.Close()
	}
}

func startElection() {
	mu.Lock()
	if electionInProgress {
		mu.Unlock()
		return
	}
	electionInProgress = true
	mu.Unlock()

	go handleElection()

	fmt.Println("Iniciando eleição...")
	electionDone := false

	// Envia mensagem de eleição para nós com IDs maiores
	nodeID, _ := strconv.Atoi(superNodeID)

	for id := nodeID + 1; id < len(knownSuperNodes); id++ {
		nodeAddr := knownSuperNodes[id]
		if nodeAddr == "" {
			continue
		}

		// Conecta ao nó de ID maior
		conn, err := net.Dial("tcp", nodeAddr+electionPort)
		if err != nil {
			fmt.Printf("Nó %d (%s) não respondeu. Continuando eleição...\n", id, nodeAddr)
			continue
		}
		defer conn.Close()

		// Envia mensagem de eleição
		_, err = fmt.Fprintf(conn, "ELECTION %d\n", nodeID)
		if err != nil {
			fmt.Println("Erro ao enviar mensagem de eleição:", err)
			continue
		}

		// Lê resposta do nó de ID maior
		response := make([]byte, 1024)
		n, err := conn.Read(response)
		if err != nil {
			fmt.Println("Erro ao ler resposta de eleição:", err)
			continue
		}

		// Se resposta for "OK", outro nó participará da eleição
		resp := strings.TrimSpace(string(response[:n]))
		if resp == "OK" {
			fmt.Println("OK recebido")
			electionDone = true
		}
	}
	time.Sleep(10 * time.Second)
	// Se nenhum nó respondeu, o nó assume a posição de coordenador
	if !electionDone {
		declareAsCoordinator()
	}
}

func declareAsCoordinator() {
	mu.Lock()
	nodeID, _ := strconv.Atoi(superNodeID)
	coordinatorIP = knownSuperNodes[nodeID]
	electionInProgress = false
	isMaster = true
	mu.Unlock()

	fmt.Printf("Nó %s agora é o novo coordenador\n", superNodeID)
	for _, superNodeAddr := range knownSuperNodes {
		if superNodeAddr == coordinatorIP {
			continue
		} else {
			conn, err := net.Dial("tcp", superNodeAddr+broadcastPort)
			if err != nil {
				fmt.Printf("Erro ao conectar ao SuperNode %s para informar novo coordenador: %v\n", superNodeAddr, err)
				continue
			}

			_, err = fmt.Fprintf(conn, "COORDINATOR %s\n", coordinatorIP)
			fmt.Println(coordinatorIP)
			if err != nil {
				fmt.Printf("Erro ao enviar mensagem de novo coordenador para SuperNode %s: %v\n", superNodeAddr, err)
			}
			_ = conn.Close()
		}

	}

	initializeNode()
}

func checkCoordinator() {
	for isMaster {
		time.Sleep(5 * time.Second)

		conn, err := net.Dial("tcp", coordinatorIP+registerPort)
		if err != nil {
			fmt.Println("Coordenador não está respondendo.")
			startElection()
		} else {
			conn.Close()
		}
	}
	return
}

func awaitMasterRelease() bool {
	ln, err := net.Listen("tcp", releasePort)
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
		if isMaster {
			return
		}
		ln, err := net.Listen("tcp", broadcastPort)

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

		if strings.Contains(string(buf[:n]), "COORDINATOR") {
			coordinatorIP = strings.Split(string(buf[:n]), " ")[1]
			fmt.Println(coordinatorIP)
		} else {
			// Armazena a lista de super nós conhecidos
			mu.Lock()
			knownSuperNodes = strings.Split(strings.TrimSpace(string(buf[:n])), ",")
			mu.Unlock()
			fmt.Printf("SuperNode recebeu lista de super nós: %v\n", knownSuperNodes)
		}

	}
}

func listnerOtherNodes(listener net.Listener) {
	for {
		time.Sleep(1 * time.Second)
		conn, err := listener.Accept()
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
		defer conn.Close()
	}
}

func initializeNode() {
	if isMaster {
		if len(superNodes) > 0 {
			ln, err := net.Listen("tcp", registerPort)
			if err != nil {
				fmt.Println("Erro ao iniciar o servidor de registro:", err)
				return
			}
			listnerOtherNodes(ln)
		} else {
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
			broadcastSuperNodes()
			listnerOtherNodes(ln)
		}

	} else {
		registerWithMaster()
		released := false
		for released == false {
			time.Sleep(3 * time.Second)
			released = awaitMasterRelease()
		}

		go checkCoordinator() // Inicia verificação do coordenador em uma goroutine
		time.Sleep(2 * time.Second)

		// Inicia o servidor para aceitar clientes
		ln, err := net.Listen("tcp", "0.0.0.0"+clientPort)
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
	initializeNode()
}
