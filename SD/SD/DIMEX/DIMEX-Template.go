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
	"strings"
	"time"
)

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

type State int // enumeracao dos estados possiveis de um processo
const (
	noMX State = iota //nao sta acessando
	wantMX // quer acessar, pedindo permissao
	inMX //esta brincando
)

type dmxReq int // enumeracao dos estados possiveis de um processo
const (
	ENTER dmxReq = iota
	EXIT
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
	
	Pp2plink *PP2PLink.PP2PLink // acesso aa comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind
}

// ------------------------------------------------------------------------------------
// ------- inicializacao
// ------------------------------------------------------------------------------------

func NewDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

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

		Pp2plink: p2p}

	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
	}
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
					module.handleUponReqEntry() // ENTRADA DO ALGORITMO

				} else if dmxR == EXIT {
					module.outDbg(" ---->>> SAI DA SC <<<---- ")
					module.handleUponReqExit() // ENTRADA DO ALGORITMO
				}

			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				//fmt.Printf("dimex recebe da rede: ", msgOutro)
				if strings.Contains(msgOutro.Message, "respOK") {
					module.outDbg(" ---->>> ME RESPONDERAM OK <<<---- " + msgOutro.Message)
					module.handleUponDeliverRespOk(msgOutro) // ENTRADA DO ALGORITMO - "ok pode acessar"

				} else if strings.Contains(msgOutro.Message, "reqEntry") {
					module.outDbg(" ---->>> ME PEDIRAM PARA USAR A SC <<<---- " + msgOutro.Message)
					module.handleUponDeliverReqEntry(msgOutro) // ENTRADA DO ALGORITMO

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

func (module *DIMEX_Module) handleUponReqEntry() { //pede acesso
	// Incrementa o relógio lógico local
	module.outDbg(" ---->>> ENTREI NA handleUponReqEntry <<<----")
	module.outDbg(" ---->>> VOU PEDIR ACESSO <<<----")
	module.lcl = int(time.Now().UnixNano())
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

func (module *DIMEX_Module) handleUponReqExit() { //libera. Processo atual termina de acessar
	module.outDbg("ENTREI NO handleUponReqExit")
	module.outDbg(" ---->>> VOU SAIR DA SC <<<----")
    for i, isWaiting := range module.waiting {
        if isWaiting {
            module.sendToLink(module.processes[i], "respOK", " ")
        }
    }
    module.waiting = make([]bool, len(module.processes)) 
    module.st = noMX //4
    module.outDbg(" ---->>> LIBEREI A SC <<<----")
}

// ------------------------------------------------------------------------------------
// ------- tratamento de mensagens de outros processos
// ------- UPON respOk
// ------- UPON reqEntry
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) { //recebeu uma resposta, responde o "ok, pode usar"
	// Incrementa o número de respostas recebidas
	module.outDbg(" ---->>> ENTREI NA handleUponDeliverRespOk <<<----")
    module.nbrResps++								
	module.outDbg(fmt.Sprintf(" ---->>> RESPOSTA OK RECEBIDA DE %s. NUM DE MGS: %d <<<---- ", msgOutro.From, module.nbrResps))
    if module.nbrResps == len(module.processes)-1 { 
        module.Ind <- dmxResp{}						
        module.st = inMX							
        module.outDbg(" ---->>> ENTRANDO NA SC <<<----")
    }

}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) { //alguem quer entrar e vê pelo relogio logico quem pediu primeiro se tiver mais gente querendo
	// Quando outro processo quer entrar na SC
    var id, ts int
	module.outDbg(" ---->>> ENTRANDO NA handleUponDeliverReqEntry <<<----")
    fmt.Sscanf(msgOutro.Message, "reqEntry,%d,%d", &id, &ts)
    // Se eu não quero entrar na SC ou se eu quero entrar na SC e o outro processo tem prioridade
    if module.st == noMX || (module.st == wantMX && before(id, ts, module.id, module.reqTs)) {
		module.outDbg(" ---->>> VOU DEIXAR ME PASSAR <<<---- ")
        module.sendToLink(module.processes[id], "respOK", " ") // Deixo passar 
    } else if module.st == inMX || (module.st == wantMX && before(module.id, module.reqTs, id, ts)) {
		module.outDbg(" ---->>> VAI TER QUE ESPERAR <<<---- ")
		module.waiting[id] = true // Mando esperar na fila
    }
	if ts > module.lcl{
		module.outDbg("Entrou no ts > module.lcl")
        module.lcl = ts // Atualizo o relógio lógico local
    }
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) sendToLink(address string, content string, space string) { //envia mensagem para outros processos
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