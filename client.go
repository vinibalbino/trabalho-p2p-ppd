// client.go
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func uploadFile(conn net.Conn, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Erro ao abrir o arquivo: %v", err)
	}
	defer file.Close()

	baseFileName := filepath.Base(filePath)
	fmt.Fprintf(conn, "UPLOAD %s\n", baseFileName)

	_, err = io.Copy(conn, file)
	if err != nil {
		return fmt.Errorf("Erro ao enviar o arquivo: %v", err)
	}

	fmt.Printf("Arquivo '%s' enviado para o servidor.\n", baseFileName)
	return nil
}

func downloadFile(conn net.Conn, fileName string) error {
	fmt.Fprintf(conn, "DOWNLOAD %s\n", fileName)

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("Erro ao ler a resposta do servidor: %v", err)
	}

	if strings.HasPrefix(response, "ERROR") {
		return fmt.Errorf(response)
	}

	fileSizeStr := strings.TrimSpace(response)
	fileSize, err := strconv.ParseInt(fileSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("Erro ao converter o tamanho do arquivo: %v", err)
	}

	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("Erro ao criar o arquivo: %v", err)
	}
	defer file.Close()

	receivedBytes := int64(0)
	for receivedBytes < fileSize {
		n, err := io.CopyN(file, conn, 1024)
		if err != nil && err != io.EOF {
			return fmt.Errorf("Erro ao baixar o arquivo: %v", err)
		}
		receivedBytes += n
	}

	fmt.Printf("Download do arquivo '%s' concluído.\n", fileName)
	return nil
}

func main() {
	conn, err := net.Dial("tcp", "192.168.100.11:8081") // IP do super nó (modifique para o IP correto)
	if err != nil {
		fmt.Println("Erro ao conectar ao super nó:", err)
		return
	}
	defer conn.Close()

	var choice int
	fmt.Println("Escolha uma opção: 1 - Upload | 2 - Download")
	fmt.Scan(&choice)

	if choice == 1 {
		fmt.Println("Digite o caminho do arquivo para upload:")
		var filePath string
		fmt.Scan(&filePath)
		err := uploadFile(conn, filePath)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Upload concluído com sucesso.")
		}
	} else if choice == 2 {
		fmt.Println("Digite o nome do arquivo para download:")
		var fileName string
		fmt.Scan(&fileName)
		err := downloadFile(conn, fileName)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Download concluído com sucesso.")
		}
	} else {
		fmt.Println("Opção inválida")
	}
}
