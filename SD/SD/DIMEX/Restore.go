/*  RESTAURACAO DE UM SNAPSHOT E CONTINUACAO DA EXECUCAO A PARTIR DELE.

    Um snapshot so serve para alguma coisa se, gravado o estado global num
    instante qualquer, for possivel voltar a esse instante e continuar
    executando como se nada tivesse acontecido.  E o que o paper de Chandy-Lamport
    chama de "state restoration" e e o objetivo final do algoritmo.

    O estado global no corte e o conjunto
        { estado local de cada processo, estado de cada canal de entrada },
    e e isso que os arquivos snap_<id>.txt guardam.  Para restaurar:

      1. cada processo le o SEU registro no corte (o do seu proprio id);
      2. cada processo volta o seu estado local para aquele registro
         (st, lcl, reqTs, nbrResps, waiting, ultimocarimbo de cada mensagem
          processada, relogio dos marcadores);
      3. as mensagens que estavam EM TRANSITO no corte voltam para o canal de
         entrada de quem as tinha gravado, e sao entregues normalmente - a
         aplicacao de cada processo continua daquele ponto em diante.

    Detalhe importante sobre o passo 3: as mensagens em transito nao foram
    "contadas" por quem as tinha no canal, porque o corte e justamente o
    instante em que elas ainda nao tinham sido aplicadas.  Portanto entrego-las
    pelo caminho normal (Ind do PP2PLink) e elas voltam a ser aplicadas
    exatamente uma vez, como se estivessem chegando agora.  O carimbo delas e
    ajustado para o numero de marcadores que o emissor tinha no corte: assim a
    mensagem continua "sendo do passado do emissor", que e o que ela de fato e
    dentro do corte restaurado.

    Uso (por variavel de ambiente, sem mudar a API):
        DIMEX_RESTORE=snapshots     diretorio com os arquivos snap_<id>.txt
        DIMEX_RESTORE_SN=42         qual snapshot (SnId) restaurar
    A aplicacao consulta DIMEX.Restaurado() e DIMEX.Estado() para saber que
    comecou de um estado restaurado - em particular, se o processo estava dentro
    da secao critica no corte, ele nao pede ENTER de novo: ele termina o que
    estava fazendo e libera a SC com EXIT.
*/

package DIMEX

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	PP2PLink "SD/PP2PLink"
)

// ------------------------------------------------------------------------------------
// ------- o corte: os registros de todos os processos de um mesmo snapshot
// ------------------------------------------------------------------------------------

type corte struct {
	snId  int
	procs map[int]SnapRecord // registro de cada processo, indexado pelo id
}

// le de todos os arquivos snap_<id>.txt do diretorio os registros do snapshot _snId
func carregaCorte(_dir string, _snId int) (*corte, error) {
	cs := &corte{snId: _snId, procs: make(map[int]SnapRecord)}
	arqs, err := filepath.Glob(filepath.Join(_dir, "snap_*.txt"))
	if err != nil || len(arqs) == 0 {
		return nil, fmt.Errorf("nenhum arquivo de snapshot em %s", _dir)
	}
	for _, nome := range arqs {
		f, err := os.Open(nome)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024) // registros podem ter canais cheios
		for sc.Scan() {
			linha := strings.TrimSpace(sc.Text())
			if linha == "" {
				continue
			}
			var rec SnapRecord
			if err := json.Unmarshal([]byte(linha), &rec); err != nil {
				continue // linha truncada (o processo foi morto no meio da escrita)
			}
			if rec.SnId == _snId {
				cs.procs[rec.ProcId] = rec
			}
		}
		f.Close()
	}
	if len(cs.procs) == 0 {
		return nil, fmt.Errorf("o snapshot %d nao tem registro em %s", _snId, _dir)
	}
	return cs, nil
}

// imprime o estado global restaurado, para conference
func (c *corte) mostra() {
	fmt.Println("[ RESTAURA ] estado global do snapshot ", c.snId, " (", len(c.procs), " processos ):")
	for id := 0; id < len(c.procs); id++ {
		r, ok := c.procs[id]
		if !ok {
			continue
		}
		emTransito := 0
		for _, ch := range r.Chans {
			emTransito += len(ch.Msgs)
		}
		fmt.Printf("[ RESTAURA ]   proc %d: %-6s lcl=%-3d reqTs=%-3d resps=%d  em transito nos canais=%d\n",
			id, r.St, r.Lcl, r.ReqTs, r.NbrResps, emTransito)
	}
}

// ------------------------------------------------------------------------------------
// ------- restauracao do estado local de UM processo
// ------------------------------------------------------------------------------------

// volta o estado deste processo para o registro do corte
func (module *DIMEX_Module) restauraEstado(_rec SnapRecord) {
	n := len(module.processes)
	module.st = stateFromString(_rec.St)
	module.lcl = _rec.Lcl
	module.reqTs = _rec.ReqTs
	module.nbrResps = _rec.NbrResps

	module.waiting = make([]bool, n)
	copy(module.waiting, _rec.Waiting)
	module.lastReqTsFrom = make([]int, n)
	copy(module.lastReqTsFrom, _rec.LastReqTsFrom)
	module.lastMkFrom = make([]int, n)
	copy(module.lastMkFrom, _rec.LastMkFrom)
	module.lastMkResp = make([]int, n)
	copy(module.lastMkResp, _rec.LastMkResp)

	// o relogio dos marcadores volta ao valor do corte: os proximos marcadores
	// continuam a sequencia daquele instante, e nao comecam do zero
	module.nMarcadores = _rec.MkSeq
	// se este processo iniciar snapshots, o proximo id e o do corte + 1 (a
	// numeracao continua, em vez de repetir ids ja usados)
	module.nextSnId = _rec.SnId

	module.snaps = make(map[int]*snapLocal) // nenhum snapshot em andamento no corte
	module.restaurado = true
	module.snapRestaurado = _rec

	// variaveis da aplicacao tambem voltam ao que era no corte
	module.appMu.Lock()
	module.vars = make(map[string]int, len(_rec.Vars))
	for k, v := range _rec.Vars {
		module.vars[k] = v
	}
	module.appMu.Unlock()

	//Marcadores ja enviados NAO sao reenviados: eles pertencem ao snapshot que
	//acabou de ser restaurado.  O que volta sao as mensagens de aplicacao que
	//estavam em transito (ver reinjeta).
	fmt.Printf("[ RESTAURA ] proc %d: estado restaurado (%s, lcl=%d, reqTs=%d, resps=%d, MkSeq=%d)\n",
		module.id, module.st.String(), module.lcl, module.reqTs, module.nbrResps, module.nMarcadores)
}

// ------------------------------------------------------------------------------------
// ------- devolucao das mensagens que estavam em transito
// ------------------------------------------------------------------------------------

// recarimba a mensagem com o numero de marcadores que o emissor tinha no corte
func recarimba(_msg string, _mk int) string {
	var de, ts int
	if _, err := fmt.Sscanf(_msg, "reqEntry,%d,%d", &de, &ts); err == nil {
		return fmt.Sprintf("reqEntry,%d,%d,%d", de, ts, _mk)
	}
	if _, err := fmt.Sscanf(_msg, "respOK,%d", &de); err == nil {
		return respOkCarimbada(de, _mk)
	}
	return _msg // nao e mensagem de aplicacao: devolve como esta
}

// devolve as mensagens em transito do corte para os canais de entrada deste
// processo, entregando-as pelo caminho normal de chegada de mensagens.
func (module *DIMEX_Module) reinjeta(_c *corte) {
	rec := _c.procs[module.id]
	var fila []PP2PLink.PP2PLink_Ind_Message
	for _, ch := range rec.Chans {
		for _, m := range ch.Msgs {
			// o carimbo e o do emissor no corte: dentro do estado restaurado a
			// mensagem pertence ao passado do emissor
			mk := _c.procs[ch.From].MkSeq
			fila = append(fila, PP2PLink.PP2PLink_Ind_Message{
				From:    module.processes[ch.From],
				Message: recarimba(m, mk)})
		}
	}
	if len(fila) == 0 {
		fmt.Println("[ RESTAURA ] proc ", module.id, ": nenhum canal tinha mensagem em transito")
		return
	}
	fmt.Println("[ RESTAURA ] proc ", module.id, ": devolvendo ", len(fila), " mensagens que estavam em transito")
	go func() {
		// as mensagens voltam depois de um instante, para que os outros processos ja
		// estejam em pe para receber as respostas
		time.Sleep(envDur("DIMEX_RESTORE_DELAY", time.Second))
		// pequeno intervalo entre as mensagens: elas voltam como uma rajada de
		// mensagens que estavam em transito, nao uma a uma no ritmo do Traffic
		for _, m := range fila {
			module.Pp2plink.Ind <- m
			time.Sleep(2 * time.Millisecond)
		}
	}()
}

// ------------------------------------------------------------------------------------
// ------- o pedido que ficou em andamento no corte
// ------------------------------------------------------------------------------------

// Um processo pode ter um pedido em andamento no instante do corte (esta em wantMX) e
// ainda assim FALTAR alguma resposta: ou a resposta esta em transito no canal de quem
// respondeu, ou o pedido esta na fila de quem vai responder quando sair da SC.
//
// Mas existe um caso em que, dentro do corte, NINGUEM tem a obrigacao de responder a
// este pedido.  Se o corte de um processo p for ANTERIOR ao nosso pedido, o canal de p
// ja estava fechado quando p gravou o seu estado (ChClosedAtRecord) e, como o canal e
// FIFO, o marcador que enviamos para p naquele corte chega em p ANTES do nosso pedido.
// Portanto p nunca vera este pedido.
//
// Entao, ao restaurar, o pedido em andamento e reenviado para EXATAMENTE esses
// processos: os que, pelo proprio estado restaurado deles, ainda nao processaram o
// nosso pedido.  Para os outros nao se reenvia nada, e assim nenhum processo recebe a
// mesma resposta duas vezes.
//
// O reenvio nao e o que segura a exclusao mutua: e o que faz o estado restaurado
// continuar sem depender de uma coincidencia.  Sem ele, a resposta para aquele pedido so
// viria quando o outro processo fizesse um pedido NOVO - o que uma aplicacao em laco
// acaba fazendo logo em seguida, mas uma aplicacao que so pedisse uma vez ficaria
// esperando para sempre.
//
// ATENCAO: com um unico iniciador de snapshot este caso nao chega a ocorrer, e isso
// tambem e medido.  O pedido que consta do corte e sempre anterior ao marcador do mesmo
// processo, e como o canal e FIFO o marcador chega antes - ou seja, o outro processo ja
// processou o pedido antes de gravar o seu estado, e o pedido nao fica orfao.  A guarda
// acima vale para a topologia em que isso nao se aplica (mais de um processo iniciando
// snapshots, por exemplo).  Ela fica porque, se um dia um par (p,q) preencher a
// condicao, o pedido de q ficaria sem resposta nenhuma no sistema restaurado - e o
// proprio estado restaurado e que diz que o par (p,q) contribui zero.
func (module *DIMEX_Module) retomaPedido(_c *corte) {
	if module.st != wantMX {
		return
	}
	if os.Getenv("DIMEX_RESTORE_REENVIA") == "0" {
		return // so para conferir o contra-exemplo: sem o reenvio o pedido nao tem resposta
	}
	var para []int
	for p, rec := range _c.procs {
		if p == module.id {
			continue
		}
		if !rec.ChClosedAtRecord[module.id] {
			continue // o canal de p ainda estava gravando: p ainda pode ver o pedido
		}
		if rec.LastReqTsFrom[module.id] >= module.reqTs {
			continue // p ja tinha processado este pedido: nao se reenvia
		}
		para = append(para, p)
	}
	if len(para) == 0 {
		return
	}
	for _, p := range para {
		fmt.Printf("[ RESTAURA ] proc %d: reenvia o pedido (reqTs=%d) para o processo %d, cujo corte e anterior ao pedido\n",
			module.id, module.reqTs, p)
		module.sendToLink(module.processes[p], fmt.Sprintf("reqEntry,%d,%d", module.id, module.reqTs), "  ")
	}
}

// ------------------------------------------------------------------------------------
// ------- API publica: novo modulo ja com o estado do corte restaurado
// ------------------------------------------------------------------------------------

// cria o modulo e restaura nele o estado do snapshot _snId lido do diretorio _dir.
// E a funcao que permite "restaurar um snapshot e continuar a execucao de la".
func NewDIMEXRestaurado(_addresses []string, _id int, _dbg bool, _dir string, _snId int) (*DIMEX_Module, error) {
	c, err := carregaCorte(_dir, _snId)
	if err != nil {
		return nil, err
	}
	if _, ok := c.procs[_id]; !ok {
		return nil, fmt.Errorf("o snapshot %d nao tem registro do processo %d", _snId, _id)
	}
	if len(c.procs) != len(_addresses) {
		return nil, fmt.Errorf("o snapshot %d e incompleto: %d de %d processos",
			_snId, len(c.procs), len(_addresses))
	}
	if _id == 0 { // um dos processos mostra o estado global restaurado
		c.mostra()
	}
	m := newDIMEX(_addresses, _id, _dbg) // ja fica rodando
	m.restauraEstado(c.procs[_id])
	go func() {
		// espera os outros processos levantarem antes de mexer na rede
		time.Sleep(envDur("DIMEX_RESTORE_DELAY", time.Second))
		m.retomaPedido(c)
	}()
	m.reinjeta(c)
	return m, nil
}

// a aplicacao pergunta se este modulo comecou de um estado restaurado
func (module *DIMEX_Module) Restaurado() bool {
	return module.restaurado
}

// o estado da exclusao mutua neste instante ("noMX", "wantMX" ou "inMX")
func (module *DIMEX_Module) Estado() string {
	return module.st.String()
}

// ------------------------------------------------------------------------------------
// ------- variaveis da aplicacao que entram no snapshot e voltam com ele
// ------------------------------------------------------------------------------------

// a aplicacao poe aqui uma variavel sua para que ela entre no snapshot
func (module *DIMEX_Module) SetVar(_nome string, _valor int) {
	module.appMu.Lock()
	defer module.appMu.Unlock()
	module.vars[_nome] = _valor
}

// a aplicacao le uma variavel sua (util depois de restaurar um snapshot)
func (module *DIMEX_Module) Var(_nome string) int {
	module.appMu.Lock()
	defer module.appMu.Unlock()
	return module.vars[_nome]
}

// ------------------------------------------------------------------------------------
// ------- auxilios
// ------------------------------------------------------------------------------------

// le uma duracao do ambiente ("1s", "500ms"), com um valor padrao
func envDur(_nome string, _padrao time.Duration) time.Duration {
	if s := os.Getenv(_nome); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			return d
		}
	}
	return _padrao
}

// string -> State, para restaurar o estado a partir do arquivo
func stateFromString(_s string) State {
	switch _s {
	case "noMX":
		return noMX
	case "wantMX":
		return wantMX
	case "inMX":
		return inMX
	}
	return noMX
}
