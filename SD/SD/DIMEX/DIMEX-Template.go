/*  Construido como parte da disciplina: FPPD - PUCRS - Escola Politecnica
    Professor: Fernando Dotti  (https://fldotti.github.io/)
    Modulo representando Algoritmo de Exclusão Mútua Distribuída:
    Semestre 2023/1
	Aspectos a observar:
	   mapeamento de módulo para estrutura
	   inicializacao
	   semantica de concorrência: cada evento é atômico
	   							  módulo trata 1 por vez
	Q U E S T A O
	   Além de obviamente entender a estrutura ...
	   Implementar o núcleo do algoritmo ja descrito, ou seja, o corpo das
	   funcoes reativas a cada entrada possível:
	   			handleUponReqEntry()  // recebe do nivel de cima (app)
				handleUponReqExit()   // recebe do nivel de cima (app)
				handleUponDeliverRespOk(msgOutro)   // recebe do nivel de baixo
				handleUponDeliverReqEntry(msgOutro) // recebe do nivel de baixo
*/

package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

type State int // enumeracao dos estados possiveis de um processo
const (
	noMX   State = iota //nao sta acessando
	wantMX              // quer acessar, pedindo permissao
	inMX                //esta brincando
)

type dmxReq int // enumeracao dos estados possiveis de um processo
const (
	ENTER dmxReq = iota
	EXIT
	SNAPSHOT // pedido da aplicacao para iniciar um snapshot (Chandy-Lamport)
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

type DIMEX_Module struct {
	Req       chan dmxReq  // canal para receber pedidos da aplicacao (REQ e EXIT)
	Ind       chan dmxResp // canal para informar aplicacao que pode acessar
	processes []string     // endereco de todos, na mesma ordem
	id        int          // identificador do processo - é o indice no array de enderecos acima
	st        State        // estado deste processo na exclusao mutua distribuida
	waiting   []bool       // processos aguardando tem flag true
	lcl       int          // relogio logico local, "saber a ordem dos pedidos, quem pediu primeiro"
	reqTs     int          // timestamp local da ultima requisicao deste processo
	nbrResps  int
	dbg       bool

	// ---- estado do algoritmo de snapshot (Chandy-Lamport), ver Snapshot.go ----
	lastReqTsFrom   []int              // timestamp do ultimo reqEntry recebido de cada processo
	lastMkFrom      []int              // carimbo do ultimo reqEntry PROCESSADO de cada processo
	lastMkResp      []int              // carimbo da ultima respOK PROCESSADA de cada processo (-1 se nenhuma)
	nextSnId        int                // contador de identificadores de snapshot deste processo
	nMarcadores     int                // quantos marcadores de snapshot este processo JA enviou
	marcadorPerdido map[int]bool       // falhas injetadas: marcador descartado (por SnId)
	snaps           map[int]*snapLocal // snapshots em andamento, por identificador
	snapFile        *snapFileWriter    // arquivo onde este processo grava os seus snapshots

	// ---- restauracao de um snapshot e continuacao da execucao (ver Restore.go) ----
	restaurado     bool           // este modulo comecou de um estado restaurado?
	snapRestaurado SnapRecord     // o registro do corte que foi restaurado
	appMu          sync.Mutex     // protege as variaveis da aplicacao
	vars           map[string]int // variaveis da aplicacao que entram no snapshot

	Pp2plink *PP2PLink.PP2PLink // acesso aa comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind
}

// ------------------------------------------------------------------------------------
// ------- inicializacao
// ------------------------------------------------------------------------------------

// Se DIMEX_RESTORE estiver definida, o modulo e criado ja com o estado do snapshot
// DIMEX_RESTORE_SN restaurado (ver Restore.go) - restaurou e continua rodando.
func NewDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {
	if dir := os.Getenv("DIMEX_RESTORE"); dir != "" {
		snId := 0
		if s := os.Getenv("DIMEX_RESTORE_SN"); s != "" {
			if n, err := strconv.Atoi(s); err == nil {
				snId = n
			} else {
				fmt.Println("[ SNAPSHOT ] DIMEX_RESTORE_SN invalido: ", s)
			}
		}
		m, err := NewDIMEXRestaurado(_addresses, _id, _dbg, dir, snId)
		if err != nil {
			fmt.Println("[ SNAPSHOT ] nao foi possivel restaurar o snapshot ", snId, " de ", dir, ": ", err)
			os.Exit(1)
		}
		return m
	}
	return newDIMEX(_addresses, _id, _dbg)
}

func newDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)

	dmx := &DIMEX_Module{
		Req: make(chan dmxReq, 1),
		Ind: make(chan dmxResp, 1),

		processes: _addresses,
		id:        _id,
		st:        noMX,
		waiting:   make([]bool, len(_addresses)),
		lcl:       0,
		reqTs:     0,
		dbg:       _dbg,

		lastReqTsFrom: make([]int, len(_addresses)),
		lastMkFrom:    make([]int, len(_addresses)),
		lastMkResp:    make([]int, len(_addresses)),
		nextSnId:      0,
		snaps:         make(map[int]*snapLocal),
		vars:          make(map[string]int),

		Pp2plink: p2p}

	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
		dmx.lastMkResp[i] = -1 // ainda nao veio resposta de ninguem
	}
	dmx.snapFile = newSnapFileWriter(_id) // arquivo de snapshots DESTE processo
	dmx.Start()
	dmx.outDbg(" ---->>> Init DIMEX! <<<---- ")
	return dmx
}

// ------------------------------------------------------------------------------------
// ------- nucleo do funcionamento
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) Start() {

	go func() {
		for {
			select {
			case dmxR := <-module.Req: // vindo da aplicação
				if dmxR == ENTER {
					module.outDbg(" ---->>> PEDI PARA USAR A SC <<<---- ") //"quero acessar"
					module.handleUponReqEntry()                            // ENTRADA DO ALGORITMO

				} else if dmxR == EXIT {
					module.outDbg(" ---->>> SAI DA SC <<<---- ")
					module.handleUponReqExit() // ENTRADA DO ALGORITMO

				} else if dmxR == SNAPSHOT {
					module.outDbg(" ---->>> APLICACAO PEDIU UM SNAPSHOT <<<---- ")
					module.handleUponReqSnapshot() // ENTRADA DO ALGORITMO DE SNAPSHOT
				}

			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				//fmt.Printf("dimex recebe da rede: ", msgOutro)

				// O PP2PLink so informa o endereco do SOCKET de origem, e a porta
				// desse socket e a porta efemera do cliente, que nao e a porta de
				// escuta do processo.  Por isso o id do remetente viaja na propria
				// mensagem, e e ele que identifica o dono do canal de entrada.
				from := senderIdOf(msgOutro.Message)

				if strings.Contains(msgOutro.Message, snapMarkerPrefix) {
					module.handleUponSnapshotMarker(msgOutro) // ENTRADA DO ALGORITMO DE SNAPSHOT
					continue
				}

				// antes de tratar a mensagem, registra-a no estado do canal de
				// entrada de todo snapshot em andamento que ainda
				// esteja gravando este canal
				module.snapRecordIncoming(from, msgOutro.Message)

				if strings.Contains(msgOutro.Message, "respOK") {
					module.outDbg(" ---->>> ME RESPONDERAM OK <<<---- " + msgOutro.Message)
					module.handleUponDeliverRespOk(msgOutro) // ENTRADA DO ALGORITMO - "ok pode acessar"

				} else if strings.Contains(msgOutro.Message, "reqEntry") {
					module.outDbg(" ---->>> ME PEDIRAM PARA USAR A SC <<<---- " + msgOutro.Message)
					module.handleUponDeliverReqEntry(msgOutro) // ENTRADA DO ALGORITMO

				} else {
					module.outDbg(" ---->>> MSG DESCONHECIDA <<<---- " + msgOutro.Message)
				}
			}
		}
	}()
}

// ------------------------------------------------------------------------------------
// ------- tratamento de pedidos vindos da aplicacao
// ------- UPON ENTRY
// ------- UPON EXIT
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponReqEntry() { //PEDE ACESSO À SC
	module.outDbg(" ---->>> ENTREI NA handleUponReqEntry <<<----")
	module.outDbg(" ---->>> VOU PEDIR ACESSO <<<----")
	module.lcl = int(time.Now().UnixNano()) // Incrementa o relógio lógico local (marca a "hr")
	module.reqTs = module.lcl
	module.nbrResps = 0

	// Envia requisições para todos os outros processos
	for i, address := range module.processes {
		if i != module.id {
			msg := fmt.Sprintf("reqEntry,%d,%d", module.id, module.reqTs)
			module.sendToLink(address, msg, "")
		}
	}

	// Atualiza o estado para "quer entrar na região crítica"
	module.st = wantMX
	module.outDbg(" ---->>> MANDEI MSG DE PEDIDO PARA TODO MUNDO <<<----")
}

func (module *DIMEX_Module) handleUponReqExit() { //LIBERA O ACESSO À SC (SAI). Processo atual termina de acessar
	module.outDbg("ENTREI NO handleUponReqExit")
	module.outDbg(" ---->>> VOU SAIR DA SC <<<----")
	for i, isWaiting := range module.waiting {
		if isWaiting { //vê quem ta esperando e avisa a todos que saiu
			module.sendToLink(module.processes[i], respOkMessage(module.id), " ")
		}
	}
	module.waiting = make([]bool, len(module.processes))
	module.st = noMX // Atualiza o estado para "não está na região crítica"
	module.outDbg(" ---->>> LIBEREI A SC <<<----")
}

// ------------------------------------------------------------------------------------
// ------- tratamento de mensagens de outros processos
// ------- UPON respOk
// ------- UPON reqEntry
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) { //recebeu uma resposta, o "ok, pode usar"
	// Incrementa o número de respostas recebidas
	module.outDbg(" ---->>> ENTREI NA handleUponDeliverRespOk <<<----")
	module.nbrResps++
	// guarda qual resposta este processo ja aplicou, e o carimbo dela, para que a
	// ferramenta de avaliacao possa verificar que nao veio mensagem do "futuro"
	if _de, _mk, _ok := respostaCarimbada(msgOutro.Message); _ok {
		module.lastMkResp[_de] = _mk
	}
	module.outDbg(fmt.Sprintf(" ---->>> RESPOSTA OK RECEBIDA DE %s. NUM DE MGS: %d <<<---- ", msgOutro.From, module.nbrResps)) //"recebi ok de fulano"
	if module.nbrResps == len(module.processes)-1 {                                                                            //se o numero de respostas for igual ao numero de processos, pode entrar
		module.Ind <- dmxResp{} // Informa a aplicação que pode acessar
		module.st = inMX        // Atualiza o estado para "está na região crítica"
		module.outDbg(" ---->>> ENTRANDO NA SC <<<----")
	}

}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) { //alguem quer entrar e vê pelo relogio logico quem pediu primeiro se tiver mais gente querendo
	var id, ts int
	module.outDbg(" ---->>> ENTRANDO NA handleUponDeliverReqEntry <<<----")
	fmt.Sscanf(msgOutro.Message, "reqEntry,%d,%d", &id, &ts)
	module.lastReqTsFrom[id] = ts // guarda de qual requisicao este processo ja sabe
	if _mk, _ok := carimboDaMensagem(msgOutro.Message); _ok {
		module.lastMkFrom[id] = _mk
	}

	// ---------------- FALHAS INJETADAS (etapa 4 do trabalho) ----------------
	// ver DIMEX/Faults.go.  Desligadas quando DIMEX_FAULT nao esta definida.
	if module.falhaAtiva(faultViolarSC) {
		// FALHA: responde "ok" sem olhar o proprio estado nem o relogio logico,
		// ou seja, concede a SC mesmo estando dentro dela.
		module.outDbg(" ---->>> [FALHA violating SC] VOU DEIXAR ME PASSAR MESMO ESTANDO NA SC <<<---- ")
		module.sendToLink(module.processes[id], respOkMessage(module.id), "[FALHA] ")
		if ts > module.lcl {
			module.lcl = ts
		}
		return
	}
	if module.falhaAtiva(faultBloquear) {
		// FALHA: "esquece" o pedido.  Nao responde e nao enfileira, portanto quem
		// pediu vai ficar esperando para sempre.
		module.outDbg(" ---->>> [FALHA bloqueando] VOU IGNORAR ESTE PEDIDO <<<---- ")
		if ts > module.lcl {
			module.lcl = ts
		}
		return
	}
	// ---------------- fim das falhas injetadas ----------------

	// Se eu não quero entrar na SC ou se eu quero entrar na SC e o outro processo tem prioridade
	if module.st == noMX || (module.st == wantMX && before(id, ts, module.id, module.reqTs)) {
		module.outDbg(" ---->>> VOU DEIXAR ME PASSAR <<<---- ")
		module.sendToLink(module.processes[id], respOkMessage(module.id), " ") // Deixo passar
	} else if module.st == inMX || (module.st == wantMX && before(module.id, module.reqTs, id, ts)) {
		module.outDbg(" ---->>> VAI TER QUE ESPERAR <<<---- ")
		module.waiting[id] = true // Mando esperar na fila
	}
	if ts > module.lcl {
		module.outDbg("Entrou no ts > module.lcl")
		module.lcl = ts // Atualizo o relógio lógico local
	}
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) sendToLink(address string, content string, space string) { //envia mensagem para outros processos
	// toda mensagem que sai do modulo passa por aqui, e e carimbada com o numero de
	// marcadores de snapshot que este processo ja enviou.  Assim a ferramenta de
	// avaliacao consegue dizer se uma mensagem foi enviada antes ou depois da
	// gravacao do estado local.  Ver carimbaMensagem em DIMEX/Snapshot.go.
	content = module.carimbaMensagem(content)
	module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)
	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content}
}

func before(oneId, oneTs, othId, othTs int) bool { //compara para ver quem tem prioridade
	if oneTs < othTs {
		return true
	} else if oneTs > othTs {
		return false
	} else {
		return oneId < othId
	}
}

func (module *DIMEX_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}
