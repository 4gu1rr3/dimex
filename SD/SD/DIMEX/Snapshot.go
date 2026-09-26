/*  Extensao do modulo DIMEX com o algoritmo de snapshot de Chandy-Lamport.

    Referencia:
      K. Mani Chandy e Leslie Lamport - "Distributed Snapshots: Determining Global
      States of Distributed Systems", ACM Transactions on Computer Systems, 3(1), 1985.
      https://lamport.azurewebsites.net/pubs/chandy.pdf

    Hipoteses usadas (satisfeitas pelo PP2PLink deste trabalho):
      - nao ha falhas, as mensagens chegam inteiras, uma unica vez, e em ordem (FIFO);
      - entre cada par de processos existe um canal em cada sentido;
      - qualquer processo pode iniciar um snapshot;
      - o algoritmo de snapshot nao atrapalha o funcionamento normal dos processos.
      Cada processo grava o seu estado local e o estado dos seus canais de ENTRADA.

    REGRA DE ENVIO DO MARCADOR (processo p):
      para cada canal de saida c de p, p envia um marcador por c DEPOIS de gravar o
      seu estado e ANTES de enviar qualquer outra mensagem por c.

    REGRA DE RECEBIMENTO DO MARCADOR (processo q), ao receber marcador pelo canal c:
      - se q ainda nao gravou o seu estado:
            q grava o seu estado;
            q grava o estado do canal c como sequencia vazia;
            q envia marcadores por todos os seus canais de saida;
      - se q ja tinha gravado o seu estado:
            q grava o estado do canal c como a sequencia de mensagens recebidas por c
            depois de q gravar o seu estado e antes de q receber o marcador por c;
      - nos dois casos q passa a NAO gravar mais mensagens no canal c.

    Termina o snapshot no processo q quando q tiver um marcador de TODOS os seus N-1
    canais de entrada (porque ai ja se tem o estado local e o estado de todos os canais).

    Varios snapshots podem estar em andamento ao mesmo tempo.  Cada um tem um
    identificador (SnId) gerado pelo proprio processo que o inicia.  Cada processo
    grava o seu estado em um arquivo proprio (um por processo), com um registro por
    linha, em formato JSON.
*/

package DIMEX

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	PP2PLink "SD/PP2PLink"
)

// ------------------------------------------------------------------------------------
// ------- constantes do protocolo de snapshot
// ------------------------------------------------------------------------------------

const (
	// prefixo da mensagem de marcador do algoritmo de Chandy-Lamport.
	// formato: "snap,<SnId>,<n de marcadores ja enviados>,<id do processo emissor>"
	snapMarkerPrefix  = "snap,"
	snapMarkerMessage = snapMarkerPrefix + "%d,%d,%d"
	// diretorio onde cada processo grava os seus snapshots (pode ser trocado pela
	// variavel de ambiente DIMEX_SNAP_DIR)
	snapDefaultDir = "snapshots"
)

// ------------------------------------------------------------------------------------
// ------- o que e gravado nos arquivos (um registro por snapshot, por processo)
// ------------------------------------------------------------------------------------

// estado de UM canal de entrada no instante do corte.
// msgs = mensagens recebidas por este canal depois de o processo gravar o seu
// estado local e antes de receber o marcador deste canal.
type SnapChannel struct {
	From int      `json:"from"` // processo emissor do canal
	Msgs []string `json:"msgs"` // mensagens que estavam em transito no corte
}

// estado local do processo + estado de todos os canais de entrada.
// e o que a ferramenta de avaliacao le para montar o estado global.
type SnapRecord struct {
	SnId     int    `json:"snId"`     // identificador do snapshot
	ProcId   int    `json:"procId"`   // processo que gravou este registro
	St       string `json:"st"`       // noMX | wantMX | inMX
	Lcl      int    `json:"lcl"`      // relogio logico local
	ReqTs    int    `json:"reqTs"`    // timestamp da requisicao em andamento
	NbrResps int    `json:"nbrResps"` // respostas "ok" ja recebidas
	Waiting  []bool `json:"waiting"`  // waiting[p][q] = processo p tem q na sua fila

	// timestamp do ultimo reqEntry recebido de cada processo.  Permite saber, olhando
	// apenas o estado de um processo, se ele ja havia processado o pedido de outro -
	// necessario para verificar a consistencia do corte e a contabilidade de progresso.
	LastReqTsFrom []int `json:"lastReqTsFrom"`

	// carimbo do ultimo reqEntry PROCESSADO de cada processo, ou seja, o numero de
	// marcadores que o emissor ja tinha enviado quando enviou aquele pedido.  E o que
	// permite verificar, sem depender de relogio, que o pedido nao veio do "futuro" do
	// emissor (ver LastMkResp para a mesma ideia aplicada as respostas).
	LastMkFrom []int `json:"lastMkFrom"`

	// carimbo da ultima resposta "ok" PROCESSADA de cada processo, ou -1 se ainda nao
	// veio nenhuma resposta daquele processo.  Mesmo sentido de LastMkFrom.
	LastMkResp []int `json:"lastMkResp"`

	// ChClosedAtRecord[q] = true quando, no instante em que este processo gravou o seu
	// estado, o marcador de q ja havia chegado, isto e, o canal de entrada q->este
	// processo JA NAO estava gravando.  Sem esta informacao nao da para saber, a
	// partir de um corte consistente, se um pedido que chegou depois do marcador e
	// parte do corte ou e uma mensagem "do futuro" (pos-corte nos dois extremos).
	ChClosedAtRecord []bool `json:"chClosedAtRecord"`

	// MkSeq = quantos marcadores de snapshot este processo JA TINHA ENVIADO no instante
	// em que gravou este estado.  O marcador deste proprio snapshot, que e enviado
	// logo em seguida, e o MkSeq+1.  Como toda mensagem enviada pelo modulo leva o
	// carimbo com o numero de marcadores que ja tinham sido enviados, da para saber
	// com precisao se aquela mensagem saiu antes ou depois desta gravacao - que e
	// justamente o criterio de consistencia de Chandy-Lamport.
	MkSeq int `json:"mkSeq"`

	RecTime int64         `json:"recTime"` // instante (local) da gravacao do estado
	Chans   []SnapChannel `json:"chans"`   // estado dos canais de entrada

	// variaveis que a APLICACAO pediu para que entrassem no snapshot (SetVar).
	// Elas voltam com o estado restaurado, entao a aplicacao tambem retoma de onde
	// parou - e nao so o modulo de exclusao mutua.
	Vars map[string]int `json:"vars,omitempty"`
}

// ------------------------------------------------------------------------------------
// ------- escrita nos arquivos: um arquivo por processo
// ------------------------------------------------------------------------------------

type snapFileWriter struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func snapDir() string {
	if d := os.Getenv("DIMEX_SNAP_DIR"); d != "" {
		return d
	}
	return snapDefaultDir
}

func newSnapFileWriter(_procId int) *snapFileWriter {
	dir := snapDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Println("[ SNAPSHOT ] nao foi possivel criar o diretorio ", dir, err)
		return nil
	}
	name := filepath.Join(dir, "snap_"+strconv.Itoa(_procId)+".txt")
	f, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("[ SNAPSHOT ] nao foi possivel abrir o arquivo ", name, err)
		return nil
	}
	fmt.Println("[ SNAPSHOT ] gravando os snapshots deste processo em ", name)
	return &snapFileWriter{f: f, w: bufio.NewWriter(f)}
}

// um registro por linha (JSON), para o arquivo poder ser lido enquanto o sistema roda
func (s *snapFileWriter) write(_rec SnapRecord) {
	if s == nil {
		return
	}
	b, err := json.Marshal(_rec)
	if err != nil {
		fmt.Println("[ SNAPSHOT ] erro serializando o snapshot ", _rec.SnId, err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.w.Write(b)
	s.w.WriteByte('\n')
	s.w.Flush() // um snapshot nao pode ficar preso no buffer
}

// ------------------------------------------------------------------------------------
// ------- estado interno do snapshot em andamento
// ------------------------------------------------------------------------------------

// um canal de entrada, enquanto o snapshot ainda esta em andamento neste processo
type snapChannel struct {
	closed bool     // true = marcador deste canal ja chegou (canal deixa de gravar)
	msgs   []string // mensagens recebidas neste canal desde a gravacao do estado local
}

type snapLocal struct {
	rec SnapRecord     // estado local ja gravado
	ch  []*snapChannel // um por processo emissor, ou seja, um por canal de entrada
}

// ------------------------------------------------------------------------------------
// ------- nucleo do algoritmo de Chandy-Lamport
// ------------------------------------------------------------------------------------

// pedido da aplicacao (via dmx.Req) para iniciar um novo snapshot.
// o identificador e criado AQUI, no processo que inicia.
func (module *DIMEX_Module) handleUponReqSnapshot() {

	module.nextSnId++
	snId := module.nextSnId

	// 1) grava o seu proprio estado local
	module.snapRecordLocalState(snId)

	// 2) envia um marcador por todos os canais de saida
	module.snapSendMarkers(snId)

	// 3) a partir daqui comeca a gravar o estado de todos os canais de entrada
	module.outDbg(fmt.Sprintf(" ---->>> INICIOU O SNAPSHOT %d <<<---- ", snId))
}

// REGRA DE ENVIO DO MARCADOR
// envia um marcador por todos os canais de saida, imediatamente depois de gravar o
// estado local e antes de qualquer outra mensagem do algoritmo de exclusao mutua.
// todas as mensagens deste modulo sao enfileiradas no MESMO canal FIFO (PP2PLink.Req)
// a partir desta mesma goroutine, entao a ordem exigida pela regra e garantida.
func (module *DIMEX_Module) snapSendMarkers(_snId int) {
	// envia um marcador por todos os canais de saida.  O numero de marcador e
	// acrescentado automaticamente em carimbaMensagem, no unico ponto por onde sai
	// toda mensagem do modulo, de modo que a ordem exigida pela regra fica garantida.
	for i, address := range module.processes {
		if i != module.id {
			module.sendToLink(address, fmt.Sprintf("snap,%d", _snId), "[SNAP] ")
		}
	}
}

// grava o estado local (as variaveis do processo) para o snapshot snId
func (module *DIMEX_Module) snapRecordLocalState(_snId int) *snapLocal {

	waiting := make([]bool, len(module.waiting))
	copy(waiting, module.waiting)

	lastReqTsFrom := make([]int, len(module.lastReqTsFrom))
	copy(lastReqTsFrom, module.lastReqTsFrom)

	lastMkFrom := make([]int, len(module.lastMkFrom))
	copy(lastMkFrom, module.lastMkFrom)

	lastMkResp := make([]int, len(module.lastMkResp))
	copy(lastMkResp, module.lastMkResp)

	s := &snapLocal{
		rec: SnapRecord{
			SnId:             _snId,
			ProcId:           module.id,
			St:               module.st.String(),
			Lcl:              module.lcl,
			ReqTs:            module.reqTs,
			NbrResps:         module.nbrResps,
			Waiting:          waiting,
			LastReqTsFrom:    lastReqTsFrom,
			LastMkFrom:       lastMkFrom,
			LastMkResp:       lastMkResp,
			ChClosedAtRecord: make([]bool, len(module.processes)),
			MkSeq:            module.nMarcadores,
			RecTime:          time.Now().UnixNano(),
		},
		ch: make([]*snapChannel, len(module.processes)),
	}
	// os canais de entrada deste snapshot comecam TODOS abertos: e o registro do estado
	// local que vem antes de qualquer marcador deste snapshot, entao nenhum marcador
	// pode ter chegado ainda.  A unica excecao e o canal cujo marcador acabou de
	// disparar este registro - e quem preenche essa flag e handleUponSnapshotMarker,
	// logo depois de chamar esta funcao (ver la).
	for i := range s.ch {
		s.ch[i] = &snapChannel{}
	}
	// variaveis da aplicacao que ela pediu para entrarem no snapshot
	module.appMu.Lock()
	if len(module.vars) > 0 {
		s.rec.Vars = make(map[string]int, len(module.vars))
		for k, v := range module.vars {
			s.rec.Vars[k] = v
		}
	}
	module.appMu.Unlock()

	module.snaps[_snId] = s
	return s
}

// REGRA DE RECEBIMENTO DO MARCADOR
func (module *DIMEX_Module) handleUponSnapshotMarker(_msgOutro PP2PLink.PP2PLink_Ind_Message) {

	// "snap,<SnId>,<n de marcadores do emissor>,<id do emissor>"
	var snId, mkSeq, from int
	if _, err := fmt.Sscanf(_msgOutro.Message, snapMarkerPrefix+"%d,%d,%d", &snId, &mkSeq, &from); err != nil {
		module.outDbg("[ SNAPSHOT ] marcador mal formado: " + _msgOutro.Message)
		return
	}
	if from < 0 || from >= len(module.processes) || from == module.id {
		return
	}

	if module.falhaAtiva(faultPerderMarcador) && !module.marcadorPerdido[snId] {
		// FALHA: descarta este marcador como se a rede o tivesse perdido.  O snapshot
		// nunca mais fica completo neste processo.
		if module.marcadorPerdido == nil {
			module.marcadorPerdido = map[int]bool{}
		}
		module.marcadorPerdido[snId] = true
		module.outDbg(fmt.Sprintf(" ---->>> [FALHA] PERDI O MARCADOR DO SNAPSHOT %d DE %d <<<---- ", snId, from))
		return
	}

	s, jaGravou := module.snaps[snId]

	if !jaGravou {
		// primeiro marcador deste snapshot neste processo:
		//   grava o estado local, grava o estado deste canal como vazio,
		//   e envia marcadores por todos os canais de saida.
		s = module.snapRecordLocalState(snId)
		s.ch[from].msgs = nil
		// O canal deste marcador JA ESTAVA FECHADO no instante em que o estado foi
		// gravado: foi a chegada dele que disparou a gravacao, e a partir de agora ele
		// nao grava mais nada.  Registrar isso no estado gravado e o que permite, so
		// olhando o corte, saber que um pedido que chegou depois NAO faz parte do corte
		// (ver o par "invisivel" do INV4 em snapCheck, e retomaPedido em Restore.go).
		s.rec.ChClosedAtRecord[from] = true
		module.snapSendMarkers(snId)
		module.outDbg(fmt.Sprintf(" ---->>> SNAPSHOT %d: MARCADOR DE %d, GRAVEI MEU ESTADO <<<---- ", snId, from))
	} else {
		// ja tinha gravado: o estado deste canal e a sequencia de mensagens
		// recebidas desde a gravacao do estado local, que ja foi acumulada
		// em s.ch[from].msgs pela funcao snapRecordIncoming.
		module.outDbg(fmt.Sprintf(" ---->>> SNAPSHOT %d: MARCADOR DE %d, FECHEI O CANAL (%d mensagens) <<<---- ", snId, from, len(s.ch[from].msgs)))
	}

	// nos dois casos: para de gravar neste canal de entrada
	s.ch[from].closed = true

	module.snapTryFinish(snId)
}

// toda mensagem recebida (que nao seja marcador) precisa ser registrada no estado do
// canal de entrada de todos os snapshots em que este canal ainda esteja gravando.
func (module *DIMEX_Module) snapRecordIncoming(_from int, _msg string) {
	if _from < 0 || _from >= len(module.processes) {
		return
	}
	if module.falhaAtiva(faultNaoGravarCanal) {
		// FALHA: "esquece" de guardar a mensagem no estado do canal de entrada.  O
		// corte passa a perder as mensagens que estavam em transito.
		module.outDbg(" ---->>> [FALHA] NAO GRAVEI A MENSAGEM NO CANAL <<<---- ")
		return
	}
	for _, s := range module.snaps {
		if s.ch[_from] != nil && !s.ch[_from].closed {
			s.ch[_from].msgs = append(s.ch[_from].msgs, _msg)
		}
	}
}

// quando chegou marcador de todos os N-1 canais de entrada, o snapshot esta completo
// neste processo: grava o arquivo e libera a memoria do snapshot.
func (module *DIMEX_Module) snapTryFinish(_snId int) {

	s, ok := module.snaps[_snId]
	if !ok {
		return
	}
	for i := range s.ch {
		if i != module.id && !s.ch[i].closed {
			return // ainda falta marcador de algum canal de entrada
		}
	}

	rec := s.rec
	rec.Chans = make([]SnapChannel, 0, len(s.ch))
	for i := range s.ch {
		if i == module.id {
			continue
		}
		msgs := s.ch[i].msgs
		if msgs == nil {
			msgs = []string{}
		}
		rec.Chans = append(rec.Chans, SnapChannel{From: i, Msgs: msgs})
	}

	module.snapFile.write(rec)
	if module.falhaAtiva(faultGravarDuasVezes) {
		// FALHA: grava o mesmo registro de novo, como se o processo nao soubesse que
		// ja tinha gravado este snapshot.
		module.outDbg(fmt.Sprintf(" ---->>> [FALHA] GRAVEI O SNAPSHOT %d DUAS VEZES <<<---- ", _snId))
		module.snapFile.write(rec)
	}
	delete(module.snaps, _snId)
	module.outDbg(fmt.Sprintf(" ---->>> SNAPSHOT %d GRAVADO <<<---- ", _snId))
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

// carimbaMensagem e chamada por sendToLink, ou seja, e o UNICO ponto por onde sai
// toda mensagem do modulo.  Isso garante que nenhuma mensagem escapa sem o carimbo.
//
// O carimbo e o numero de marcadores de snapshot que o modulo ja tinha enviado
// naquele instante:
//
//	"snap,<snId>,<carimbo>,<de>"        marcador (o proprio marcador conta como 1)
//	"reqEntry,<de>,<ts>,<carimbo>"       pedido de acesso a SC
//	"respOK,<de>,<carimbo>"              resposta "pode entrar"
//
// Se o carimbo de uma mensagem for MAIOR que o MkSeq gravado no estado local do
// emissor, essa mensagem saiu DEPOIS de ele ter gravado o seu estado, e portanto
// nao pode fazer parte do corte.  Se for MENOR ou IGUAL, ela saiu antes da
// gravacao, e se ainda assim aparecer no estado de algum canal, o corte e
// inconsistente.  E o criterio de consistencia do Chandy-Lamport, e e o que a
// invariante INV9 da ferramenta de avaliacao verifica.
func (module *DIMEX_Module) carimbaMensagem(_msg string) string {
	if strings.HasPrefix(_msg, snapMarkerPrefix) {
		// "snap,<snId>" -> "snap,<snId>,<carimbo>,<id do processo>"
		var snId int
		if _, err := fmt.Sscanf(_msg, snapMarkerPrefix+"%d", &snId); err != nil {
			return _msg // marcador mal formado: nao mexe
		}
		module.nMarcadores++
		return fmt.Sprintf(snapMarkerMessage, snId, module.nMarcadores, module.id)
	}
	// "reqEntry,<de>,<ts>" ou "respOK,<de>" -> acrescenta o carimbo no final
	return fmt.Sprintf("%s,%d", _msg, module.nMarcadores)
}

// O PP2PLink informa em Ind.Message o endereco do SOCKET de origem, e nao o endereco
// do processo: a porta desse socket e a porta efemera do cliente, que nunca coincide
// com a porta de escuta do processo.  Por isso o id do remetente viaja dentro da
// propria mensagem, e todas as mensagens do modulo o carregam:
//
//	"reqEntry,<de>,<ts>,<carimbo>"  requisição de acesso à SC
//	"respOK,<de>,<carimbo>"         resposta "pode entrar"
//	"snap,<snId>,<carimbo>,<de>"    marcador do Chandy-Lamport
//
// Assim o estado do canal de entrada é sempre atribuido ao processo certo.
func senderIdOf(_msg string) int {
	var a, b, c int
	if _, err := fmt.Sscanf(_msg, "reqEntry,%d,%d", &a, &b); err == nil {
		return a
	}
	if _, err := fmt.Sscanf(_msg, "respOK,%d", &a); err == nil {
		return a
	}
	if _, err := fmt.Sscanf(_msg, snapMarkerPrefix+"%d,%d,%d", &a, &b, &c); err == nil {
		return c
	}
	return -1
}

// a mensagem de resposta carrega o id de quem responde
func respOkMessage(_id int) string {
	return fmt.Sprintf("respOK,%d", _id)
}

// idem, ja com o carimbo: "respOK,<de>,<carimbo>"
func respOkCarimbada(_id, _carimbo int) string {
	return fmt.Sprintf("respOK,%d,%d", _id, _carimbo)
}

// carimboDaMensagem devolve o carimbo de qualquer mensagem do modulo
// ("reqEntry,<de>,<ts>,<carimbo>" ou "respOK,<de>,<carimbo>")
func carimboDaMensagem(_msg string) (int, bool) {
	var a, b, c int
	if _, err := fmt.Sscanf(_msg, "reqEntry,%d,%d,%d", &a, &b, &c); err == nil {
		return c, true
	}
	if _, err := fmt.Sscanf(_msg, "respOK,%d,%d", &a, &b); err == nil {
		return b, true
	}
	return 0, false
}

// respostaCarimbada devolve o id de quem respondeu e o carimbo da resposta
func respostaCarimbada(_msg string) (int, int, bool) {
	var de, mk int
	if _, err := fmt.Sscanf(_msg, "respOK,%d,%d", &de, &mk); err == nil {
		return de, mk, true
	}
	return -1, 0, false
}

func (s State) String() string {
	switch s {
	case noMX:
		return "noMX"
	case wantMX:
		return "wantMX"
	case inMX:
		return "inMX"
	}
	return "unknown"
}
