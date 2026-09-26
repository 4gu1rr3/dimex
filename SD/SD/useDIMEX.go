// Construido como parte da disciplina: Sistemas Distribuidos - PUCRS - Escola Politecnica
//  Professor: Fernando Dotti  (https://fldotti.github.io/)
/* fmt.Println("go run useDIMEX.go 0 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
   fmt.Println("go run useDIMEX.go 1 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
   fmt.Println("go run useDIMEX.go 2 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
/*
  LANCAR N PROCESSOS EM SHELL's DIFERENTES, UMA PARA CADA PROCESSO.
  para cada processo: seu id único e a mesma lista de processos.
  o endereca de cada processo é o dado na lista, na posicao do seu id.
  no exemplo acima o processo com id=1  usa a porta 6001 para receber e as portas
  5000 e 7002 para mandar mensagens respectivamente para processos com id=0 e 2

  SNAPSHOT (Chandy-Lamport, ver DIMEX/Snapshot.go):
  o processo cujo id = DIMEX_SNAP_INIT inicia snapshots sucessivos, cada um com um
  identificador proprio (1, 2, 3 ...), independentemente dos acessos a SC.  Cada
  processo grava o seu estado e o estado dos seus canais de entrada em um arquivo
  proprio (snapshots/snap_<id>.txt, um snapshot por linha em JSON).  Avalie com:
      go build -o logs/snapCheck ./snapCheck && ./logs/snapCheck snapshots
  ou, para o teste completo,  ./runSnapshots.sh

  VARIAVEIS DE AMBIENTE
      DIMEX_SNAP_INIT=0     qual processo inicia os snapshots (padrao 0; -1 desliga)
      DIMEX_SNAP_EVERY=100ms  intervalo entre dois snapshots consecutivos
      DIMEX_SNAP_DIR=snapshots  diretorio onde cada processo grava o seu arquivo
      DIMEX_GAP_MS=6        espera de 0..GAP_MS-1 ms fora da SC (0 = sem espera)
      DIMEX_CS_MS=6         permanece de 1..CS_MS ms dentro da SC
      DIMEX_APP_VERBOSE=1   imprime o passo a passo da aplicacao (enche o log)
      DIMEX_FAULT=...       injeta falhas, ver DIMEX/Faults.go

  RESTAURACAO (ver DIMEX/Restore.go)
      DIMEX_RESTORE=snapshots  diretorio com os arquivos snap_<id>.txt do corte
      DIMEX_RESTORE_SN=42      qual snapshot (SnId) restaurar
      DIMEX_RESTORE_DELAY=1s   espera antes de mexer na rede, para os outros
                                processos ja estarem em pe
*/

package main

import (
	"SD/DIMEX"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func envInt(_nome string, _padrao int) int {
	if v := os.Getenv(_nome); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return _padrao
}

func envDur(_nome string, _padrao time.Duration) time.Duration {
	if v := os.Getenv(_nome); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return _padrao
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Please specify at least one address:port!")
		fmt.Println("go run useDIMEX.go 0 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
		fmt.Println("go run useDIMEX.go 1 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
		fmt.Println("go run useDIMEX.go 2 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
		return
	}

	id, _ := strconv.Atoi(os.Args[1])
	addresses := os.Args[2:]
	fmt.Print("id: ", id, "   ")
	fmt.Println(addresses)

	var dmx *DIMEX.DIMEX_Module = DIMEX.NewDIMEX(addresses, id, true)
	fmt.Println(dmx)

	// imprime o passo a passo da aplicacao somente se DIMEX_APP_VERBOSE estiver
	// definida, para nao encher o log
	verbose := os.Getenv("DIMEX_APP_VERBOSE") != ""
	logApp := func(_o ...interface{}) {
		if verbose {
			fmt.Print("[ APP id: ", id, " ] ")
			fmt.Println(_o...)
		}
	}

	// Se este modulo foi restaurado de um snapshot, a aplicacao volta ao ponto em que
	// parou: as suas proprias variaveis voltam (SetVar/Var) e, se o processo estava
	// dentro da SC no corte, ele NAO pede ENTER de novo - termina o que estava fazendo
	// e libera a secao critica com EXIT.
	restaurado := dmx.Restaurado()
	entradas := dmx.Var("entradasNaSC")
	if restaurado {
		fmt.Println("[ APP id: ", id, " ] *RESTAURADO* do snapshot - estado da SC: ",
			dmx.Estado(), " - ja tinha entrado na SC ", entradas, " vezes")
		// a partir daqui os snapshots continuam a numeracao do snapshot restaurado
		if envInt("DIMEX_SNAP_INIT", 0) == id {
			go func() {
				for {
					time.Sleep(2 * time.Second)
					dmx.Req <- DIMEX.SNAPSHOT
				}
			}()
		}
	}

	// este processo inicia os snapshots sucessivos, em paralelo com o seu uso da SC
	if !restaurado && envInt("DIMEX_SNAP_INIT", 0) == id {
		periodo := envDur("DIMEX_SNAP_EVERY", 100*time.Millisecond)
		logApp("INICIA SNAPSHOTS A CADA ", periodo)
		go func() {
			for {
				time.Sleep(periodo)
				dmx.Req <- DIMEX.SNAPSHOT
			}
		}()
	}

	// espera para facilitar a inicializacao de todos os processos.  Depois de restaurar
	// um snapshot nao ha nada para inicializar: o estado ja esta pronto, entao a espera
	// e curta (so para os outros processos levantarem) e o sistema ja continua.
	esperaInicial := 2 * time.Second
	if restaurado {
		esperaInicial = 500 * time.Millisecond
	}
	time.Sleep(esperaInicial)

	// quanto tempo fica na SC e quanto tempo espera fora dela.  Com o tempo na SC
	// bem pequeno os processos ficam mais tempo na fila, e os estados se cruzam o
	// maximo possivel nos snapshots.
	espera := envInt("DIMEX_GAP_MS", 6) // 0..espera-1 ms fora da SC
	dentro := envInt("DIMEX_CS_MS", 6)  // 1..dentro ms dentro da SC
	if dentro < 1 {
		dentro = 1
	}

	// O que fazer com o estado em que o processo ficou no corte:
	//   inMX   -> ele estava DENTRO da SC: nao pede ENTER, termina e libera (EXIT);
	//   wantMX -> ele tinha um PEDIDO EM ANDAMENTO: nao se pode pedir de novo, porque
	//             o modulo trataria isso como um pedido novo e o antigo se perderia.
	//             A aplicacao so espera a resposta (Ind) do pedido que ja estava em
	//             andamento - a resposta chega sozinha, pois os outros processos
	//             voltaram tambem ao estado do corte;
	//   noMX   -> nada em andamento: fluxo normal, comeca pedindo ENTER.
	dentroDaSC := restaurado && dmx.Estado() == "inMX"
	pedindo := restaurado && dmx.Estado() == "wantMX"

	for {
		if dentroDaSC {
			// ja estava na SC: termina o trabalho e libera, sem pedir ENTER
			logApp("ESTAVA NA SC (restaurado): termina e libera")
			dentroDaSC = false
		} else if pedindo {
			// tinha um pedido em andamento: espera a resposta dele
			logApp("TINHA PEDIDO EM ANDAMENTO (restaurado): espera a resposta")
			<-dmx.Ind
			logApp("RESPEDIU DO PEDIDO QUE ESTAVA EM ANDAMENTO")
			pedindo = false
		} else {
			// pequeno atraso aleatorio: cria contencao entre os processos
			if espera > 0 {
				time.Sleep(time.Duration(rand.Intn(espera)) * time.Millisecond)
			}
			logApp("PEDE   MX")
			dmx.Req <- DIMEX.ENTER
			logApp("ESPERA MX")
			<-dmx.Ind //
			logApp("*EM*   MX")
		}

		// permanece um instante na SC, para que os estados se cruzem nos snapshots
		time.Sleep(time.Duration(1+rand.Intn(dentro)) * time.Millisecond)

		entradas++
		dmx.SetVar("entradasNaSC", entradas)

		dmx.Req <- DIMEX.EXIT //
		logApp("FORA   MX")
	}
}
