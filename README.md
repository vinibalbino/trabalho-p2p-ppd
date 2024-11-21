Lincoln M Amorim
Vinicius S Balbino

Roteiro de execução:
> modificar no unified.go o valor de coordinatorIP para o IP da máquina que será o coordenador
> go run unified.go (arquivo com a variável isMaster = true) no coordenador
> go run unified.go (arquivo com a variável isMaster = false) nos 3 supernós
> aguardar liberação dos supernós
> go run client.go nos clientes (o ip no dial presente na main deve ser o ip do supernó que atenderá o cliente)
