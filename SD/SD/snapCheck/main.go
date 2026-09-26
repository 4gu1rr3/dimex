/*  Ferramenta que avalia os snapshots gravados pelo DIMEX (ver DIMEX/Snapshot.go).

    Para cada snapshot (identificador SnId) ela le o estado gravado por CADA processo,
    monta o estado global (estado local de cada processo + estado de cada canal de
    entrada) e testa uma serie de INVARIANTES do sistema de exclusao mutua distribuida.

    Invariante = propriedade que deve ser verdadeira em qualquer estado do sistema.
    Cada invariante e uma funcao que recebe o estado global e devolve:
      - nil        se a invariante e satisfeita
      - []string   com o motivo da violacao

    A ferramenta nao altera o sistema: apenas le os arquivos e reporta.

    O enunciado das nove invariantes, com a justificativa de cada uma e a tabela de
    qual falha injetada cada uma detecta, esta em INVARIANTES.md.

    uso:  go run ./snapCheck [diretorio-de-snapshots]
          default: ./snapshots
*/

package main

import (
	"SD/DIMEX"
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ------------------------------------------------------------------------------------
// ------- o estado global de UM snapshot
// ------------------------------------------------------------------------------------

// um processo, como visto no corte
type procSnap struct {
	rec      DIMEX.SnapRecord
	channels map[int][]string // processo emissor -> mensagens em transito no corte
}

// o estado global de um snapshot: o estado local de cada processo mais o estado
// de cada canal de entrada.  E o "corte" (cut) produzido pelo Chandy-Lamport.
type snapGlobal struct {
	snId  int
	procs map[int]*procSnap
}

func (s *snapGlobal) n() int { return len(s.procs) }

// mensagens de um tipo, em transito no canal from->q
func (s *snapGlobal) countMsgs(_q, _from int, _match func(string) bool) int {
	q, ok := s.procs[_q]
	if !ok {
		return 0
	}
	n := 0
	for _, m := range q.channels[_from] {
		if _match(m) {
			n++
		}
	}
	return n
}

func (s *snapGlobal) waiting(_p, _q int) bool {
	p, ok := s.procs[_p]
	if !ok || _q >= len(p.rec.Waiting) {
		return false
	}
	return p.rec.Waiting[_q]
}

// carimbo do ultimo reqEntry que _p processou, vindo de _q
func (s *snapGlobal) lastMkFrom(_p, _q int) (int, bool) {
	p, ok := s.procs[_p]
	if !ok || _q >= len(p.rec.LastMkFrom) {
		return 0, false
	}
	if p.rec.LastReqTsFrom == nil || _q >= len(p.rec.LastReqTsFrom) || p.rec.LastReqTsFrom[_q] == 0 {
		return 0, false // p nunca processou pedido nenhum de _q
	}
	return p.rec.LastMkFrom[_q], true
}

// carimbo da ultima resposta que _p processou, vindo de _q
func (s *snapGlobal) lastMkResp(_p, _q int) (int, bool) {
	p, ok := s.procs[_p]
	if !ok || _q >= len(p.rec.LastMkResp) || p.rec.LastMkResp[_q] < 0 {
		return 0, false
	}
	return p.rec.LastMkResp[_q], true
}

func (s *snapGlobal) lastReqTsFrom(_p, _q int) int {
	p, ok := s.procs[_p]
	if !ok || _q >= len(p.rec.LastReqTsFrom) {
		return 0
	}
	return p.rec.LastReqTsFrom[_q]
}

// o canal de entrada q->_p ja estava fechado no instante em que _p gravou o seu
// estado para este snapshot?  Se sim, tudo o que _q enviar depois disso e uma
// mensagem "do futuro": chega depois do marcador e fica fora do corte.
func (s *snapGlobal) chClosedAtRecord(_p, _q int) bool {
	p, ok := s.procs[_p]
	if !ok || _q >= len(p.rec.ChClosedAtRecord) {
		return false
	}
	return p.rec.ChClosedAtRecord[_q]
}

func (s *snapGlobal) inMX() []int {
	var r []int
	for id, p := range s.procs {
		if p.rec.St == "inMX" {
			r = append(r, id)
		}
	}
	sort.Ints(r)
	return r
}

// ------------------------------------------------------------------------------------
// ------- classificacao das mensagens do protocolo de exclusao mutua
// ------------------------------------------------------------------------------------

func isReqEntry(m string) bool { return strings.HasPrefix(m, "reqEntry") }
func isRespOK(m string) bool   { return strings.HasPrefix(m, "respOK") }

// timestamp do reqEntry ("reqEntry,<id>,<ts>,<carimbo>")
func reqEntryTs(m string) (int, bool) {
	var id, ts int
	if _, err := fmt.Sscanf(m, "reqEntry,%d,%d", &id, &ts); err != nil {
		return 0, false
	}
	return ts, true
}

// carimbo da mensagem: quantos marcadores de snapshot o emissor ja tinha enviado
// quando enviou esta mensagem ("reqEntry,<id>,<ts>,<carimbo>" ou
// "respOK,<id>,<carimbo>").  E o que permite verificar o criterio de consistencia
// de Chandy-Lamport sem depender de relogio: comparar o carimbo da mensagem com o
// MkSeq que o emissor gravou no seu estado.
func msgMkAt(m string) (int, bool) {
	var a, b, c int
	if _, err := fmt.Sscanf(m, "reqEntry,%d,%d,%d", &a, &b, &c); err == nil {
		return c, true
	}
	if _, err := fmt.Sscanf(m, "respOK,%d,%d", &a, &b); err == nil {
		return b, true
	}
	return 0, false
}

// ------------------------------------------------------------------------------------
// ------- AS INVARIANTES
// ------------------------------------------------------------------------------------

type inv struct {
	nome  string
	desc  string
	testa func(*snapGlobal) []string
}

// A forma que o enunciado do trabalho pede: cada invariante e um teste sobre um
// snapshot, e a funcao devolve um bool com o resultado.
//
//	funcao_InvX(_s *snapGlobal) bool
//
// O bool e apenas o veredito. O enunciado tambem pede que a ferramenta avise QUAL
// invariante foi violada, em QUAL snapshot e por QUE, e por isso o mesmo teste
// devolve a lista de violacoes (vazia = invariante satisfeita). Quem so precisa do
// sim/nao usa o bool; a ferramenta usa a lista, que e mais informativa.
func (i inv) funcao_InvX(_s *snapGlobal) bool {
	return len(i.testa(_s)) == 0
}

// INV 1  no maximo um processo dentro da Secao Critica
// (exemplo dado no enunciado)
var inv1 = inv{
	"INV1", "no maximo um processo dentro da Secao Critica",
	func(s *snapGlobal) []string {
		if dentro := s.inMX(); len(dentro) > 1 {
			return []string{fmt.Sprintf("processos dentro da SC ao mesmo tempo: %v", dentro)}
		}
		return nil
	},
}

// INV 2  se todos os processos estao em "nao quero a SC", entao:
//   - nenhuma flag waiting e verdadeira
//   - nao ha nenhuma mensagem em transito
//     (exemplo dado no enunciado)
var inv2 = inv{
	"INV2", "se ninguem quer a SC: nenhum waiting e nenhuma mensagem em transito",
	func(s *snapGlobal) []string {
		for _, p := range s.procs {
			if p.rec.St != "noMX" {
				return nil // ha alguem querendo ou dentro da SC: nao se aplica
			}
		}
		var v []string
		for id, p := range s.procs {
			for q, w := range p.rec.Waiting {
				if w {
					v = append(v, fmt.Sprintf("waiting[%d][%d]=true mas todos os processos estao fora da SC", id, q))
				}
			}
			for from, msgs := range p.channels {
				for _, m := range msgs {
					v = append(v, fmt.Sprintf("mensagem %q em transito no canal %d->%d mas todos os processos estao fora da SC", m, from, id))
				}
			}
		}
		return v
	},
}

// INV 3  se um processo q esta marcado como waiting em p, entao p esta na
// Secao Critica ou quer a Secao Critica.  (exemplo dado no enunciado)
var inv3 = inv{
	"INV3", "waiting[p][q]=true implica p inMX ou p wantMX",
	func(s *snapGlobal) []string {
		var v []string
		for p, ps := range s.procs {
			for q, w := range ps.rec.Waiting {
				if w && ps.rec.St != "inMX" && ps.rec.St != "wantMX" {
					v = append(v, fmt.Sprintf("waiting[%d][%d]=true mas o processo %d esta em %s", p, q, p, ps.rec.St))
				}
			}
		}
		return v
	},
}

// INV 4  se um processo q quer a Secao Critica (ainda nao entrou), o somatorio de
// mensagens recebidas, de mensagens em transito e de flags waiting para q em outros
// processos deve ser igual a N-1.  (exemplo dado no enunciado)
//
// Para cada processo p != q, no instante do corte, exatamente uma destas situacoes
// vale, e cada uma contribui 1 para o somatorio:
//
//	(a) q ja recebeu a resposta de p  -> ela esta em nbrResps(q)
//	(b) a resposta de p esta em transito no canal p->q
//	(c) p ainda nao processou o pedido de q, que esta em transito no canal q->p
//	(d) p processou o pedido de q e o enfileirou, isto e, waiting[p][q] = true
//
// Existe ainda um caso em que o par contribui ZERO: se o corte de p e ANTERIOR ao
// proprio pedido de q, o pedido de q nao faz parte do corte.  Isso acontece quando o
// marcador de q foi o que disparou a gravacao do estado de p (ChClosedAtRecord), o
// pedido de q nao esta no estado do canal (o canal ja estava fechado) e p ainda nao
// processou um pedido de q tao novo quanto o de q.  O canal e FIFO, entao o marcador
// de q chega em p antes do pedido de q: p ainda vai receber e responder esse pedido,
// mas isso acontece DEPOIS do corte.  Neste caso o par nao tem resposta pendente a
// contar, e a invariante e:  somatorio + pares com corte anterior ao pedido = N-1.
//
// COM UM INICIADOR UNICO ESSE CASO NAO OCORRE, e isso e medido: nos cortes coletados,
// para todos os pares (p,q), o pedido de q que consta do corte ja tinha sido processado
// por p antes de p gravar o seu estado.  A razao e a ordem do protocolo: o pedido que
// consta do corte de q e SEMPRE anterior ao marcador de q (q grava o estado, so depois
// envia os marcadores, e so depois disso faz um pedido novo), e como o canal e FIFO o
// marcador chega em p antes de qualquer pedido posterior - logo p ja processou o pedido
// que consta do corte de q.  O ramo abaixo e uma guarda para a topologia em que isso
// nao vale (por exemplo, mais de um processo iniciando snapshots ao mesmo tempo, em que
// um processo pode ser avisado do snapshot por um peer e nao pelo iniciador).
var inv4 = inv{
	"INV4", "quem quer a SC tem exatamente N-1 respostas (recebidas + em transito + waiting)",
	func(s *snapGlobal) []string {
		var v []string
		for q, qs := range s.procs {
			if qs.rec.St != "wantMX" {
				continue
			}
			total := qs.rec.NbrResps
			invisiveis := 0
			var partes []string
			for p := range s.procs {
				if p == q {
					continue
				}
				respTransito := s.countMsgs(q, p, isRespOK)
				reqTransito := s.countMsgs(p, q, isReqEntry)
				fila := 0
				if s.waiting(p, q) {
					fila = 1
				}
				total += respTransito + reqTransito + fila
				// p nem vai ver o pedido de q: o canal de p ja estava fechado no corte
				fora := reqTransito == 0 && s.lastReqTsFrom(p, q) < qs.rec.ReqTs && s.chClosedAtRecord(p, q)
				if fora {
					invisiveis++
				}
				partes = append(partes, fmt.Sprintf("p%d:[respEmTransito=%d,pedidoEmTransito=%d,waiting=%d%s]",
					p, respTransito, reqTransito, fila, map[bool]string{true: ",corteAnterior", false: ""}[fora]))
			}
			if total+invisiveis != s.n()-1 {
				v = append(v, fmt.Sprintf("processo %d (reqTs=%d) somou %d + %d pares com corte anterior = %d, mas N-1=%d  %s",
					q, qs.rec.ReqTs, total, invisiveis, total+invisiveis, s.n()-1, strings.Join(partes, " ")))
			}
		}
		return v
	},
}

// INV 5  quem esta dentro da SC ja recebeu todas as respostas e
// nao tem nada mais pendente para ele
var inv5 = inv{
	"INV5", "quem esta na SC tem as N-1 respostas e nao ha pendencia para ele",
	func(s *snapGlobal) []string {
		var v []string
		for q, qs := range s.procs {
			if qs.rec.St != "inMX" {
				continue
			}
			if qs.rec.NbrResps != s.n()-1 {
				v = append(v, fmt.Sprintf("processo %d esta na SC com apenas %d de %d respostas", q, qs.rec.NbrResps, s.n()-1))
			}
			for p := range s.procs {
				if p == q {
					continue
				}
				if c := s.countMsgs(q, p, isRespOK); c > 0 {
					v = append(v, fmt.Sprintf("processo %d esta na SC mas ha %d respOK de %d ainda em transito", q, c, p))
				}
				if s.waiting(p, q) {
					v = append(v, fmt.Sprintf("processo %d esta na SC mas o processo %d ainda tem este pedido na fila", q, p))
				}
			}
		}
		return v
	},
}

// INV 6  consistencia do corte, que e a propriedade garantida pelo Chandy-Lamport:
// uma mensagem nao pode estar "no passado" de um processo e "no ether" (em
// transito) ao mesmo tempo.  Para isto cada processo guarda o timestamp do ultimo
// reqEntry que processou (LastReqTsFrom), alem do timestamp do seu proprio pedido
// (ReqTs).  Com os dois dados verifica-se que:
//
// (A) o reqEntry de ts esta em transito no processo p e p ja o tinha processado
//
//	antes de gravar o seu estado: a mesma mensagem aparece no estado de p e no
//	canal, ou seja, ha duplicidade e o corte e inconsistente;
//
// (B) p ja processou o pedido de ts antes de gravar o seu estado, mas esse pedido
//
//	ainda nao tinha saido de q quando q gravou o seu estado: a mensagem foi
//	entregue a partir de um futuro que nao existe, e o corte e inconsistente.
var inv6 = inv{
	"INV6", "o corte e consistente: nenhuma mensagem esta duplicada nem faltando",
	func(s *snapGlobal) []string {
		var v []string
		for p, ps := range s.procs {
			for from, msgs := range ps.channels {
				for _, m := range msgs {
					ts, ok := reqEntryTs(m)
					if !ok {
						continue
					}
					// (A) a mensagem esta em transito em p, logo p nao a processou
					//     antes de gravar o seu estado
					if s.lastReqTsFrom(p, from) >= ts {
						v = append(v, fmt.Sprintf("reqEntry(ts=%d) de %d esta em transito em %d mas %d ja o tinha processado: mensagem duplicada", ts, from, p, p))
					}
				}
			}
			for from, ts := range ps.rec.LastReqTsFrom {
				if ts == 0 || from == p {
					continue
				}
				// (B) p ja processou o pedido de ts: a mensagem foi entregue antes
				//     do corte de p, logo precisa ter saido antes do corte de from
				emissor, ok := s.procs[from]
				if !ok {
					continue
				}
				if emissor.rec.ReqTs < ts {
					v = append(v, fmt.Sprintf("processo %d ja tinha processado o pedido ts=%d de %d, mas %d gravou o estado com reqTs=%d: mensagem entregue do futuro", p, ts, from, from, emissor.rec.ReqTs))
				}
			}
		}
		return v
	},
}

// INV 7  um processo nao pode ter mais de uma resposta do mesmo processo em transito
// (isto seria o modulo respondendo duas vezes para o mesmo pedido)
var inv7 = inv{
	"INV7", "nenhum processo tem mais de uma resposta do mesmo processo em transito",
	func(s *snapGlobal) []string {
		var v []string
		for q := range s.procs {
			for p := range s.procs {
				if p == q {
					continue
				}
				if c := s.countMsgs(q, p, isRespOK); c > 1 {
					v = append(v, fmt.Sprintf("processo %d tem %d respostas de %d em transito (deveria ser no maximo 1)", q, c, p))
				}
			}
		}
		return v
	},
}

// INV 8  o mesmo processo nao pode, ao mesmo tempo, ter o pedido de outro em
// transito e o outro na sua fila de espera: ou o processo processou (e enfileirou),
// ou nao.
var inv8 = inv{
	"INV8", "waiting[p][q] e o pedido de q em transito em p sao mutuamente exclusivos",
	func(s *snapGlobal) []string {
		var v []string
		for p := range s.procs {
			for q := range s.procs {
				if p == q {
					continue
				}
				if s.waiting(p, q) && s.countMsgs(p, q, isReqEntry) > 0 {
					v = append(v, fmt.Sprintf("o processo %d tem %d na fila E o pedido de %d em transito", p, q, q))
				}
			}
		}
		return v
	},
}

// INV 9  consistencia do corte, no sentido do Chandy-Lamport: nao pode existir uma
// mensagem que um processo ja tenha PROCESSADO e que o emissor tenha enviado DEPOIS
// de gravar o seu proprio estado.  Essa e a definicao de "corte consistente": o que
// foi recebido em um processo foi tambem enviado no outro extremo antes do corte.
// (O contrario e legitimo: uma mensagem enviada depois do corte do emissor e que
// ainda esta em transito no canal do receptor faz parte do corte, e e justamente o
// que o estado dos canais existe para registrar.)
//
// A verificacao nao depende de relogio: toda mensagem do modulo leva o carimbo com o
// numero de marcadores que o emissor ja tinha enviado, e o estado gravado tem o campo
// MkSeq, que e o numero de marcadores enviados ATE a gravacao (o marcador do proprio
// snapshot e o MkSeq+1, enviado logo em seguida).  Logo:
//
//	carimbo(m) <= MkSeq( emissor )   =>  m saiu ANTES de o emissor gravar o estado
//	carimbo(m) >  MkSeq( emissor )   =>  m saiu DEPOIS
//
// Para cada processo p e cada processo emissor q, verificamos as duas mensagens que
// p tem Applyed de q: o ultimo reqEntry e a ultima resposta "ok".
var inv9 = inv{
	"INV9", "consistencia do corte: nada foi recebido antes de ser enviado",
	func(s *snapGlobal) []string {
		var v []string
		for p := range s.procs {
			for q, emissor := range s.procs {
				if p == q {
					continue
				}
				// o ultimo pedido de q que p processou
				if ts := s.lastReqTsFrom(p, q); ts != 0 {
					if mk, ok := s.lastMkFrom(p, q); !ok {
						v = append(v, fmt.Sprintf("processo %d processou o pedido ts=%d de %d mas o registro nao tem o carimbo da mensagem", p, ts, q))
					} else if mk > emissor.rec.MkSeq {
						v = append(v, fmt.Sprintf("processo %d processou o pedido ts=%d de %d, mas %d o enviou (carimbo=%d) DEPOIS de gravar o estado (MkSeq=%d): mensagem do futuro",
							p, ts, q, q, mk, emissor.rec.MkSeq))
					}
				}
				// a ultima resposta de q que p processou
				if mk, tem := s.lastMkResp(p, q); tem {
					if mk > emissor.rec.MkSeq {
						v = append(v, fmt.Sprintf("processo %d tem uma resposta de %d processada com carimbo=%d, enviada DEPOIS de %d gravar o estado (MkSeq=%d): mensagem do futuro",
							p, q, mk, q, emissor.rec.MkSeq))
					}
				}
			}
		}
		return v
	},
}

// todas as invariantes
var invariantes = []inv{inv1, inv2, inv3, inv4, inv5, inv6, inv7, inv8, inv9}

// ------------------------------------------------------------------------------------
// ------- invariantes que valem para a SEQUENCIA de snapshots, e nao para um so
// ------------------------------------------------------------------------------------

// Estas verificacoes precisam comparar varios snapshots entre si.  Elas pegam erros na
// propria maquina de snapshot (e nao do DIMEX): registro gravado duas vezes,
// registro faltando, gravado fora de ordem, ou um marcador a mais ou a menos por
// estado gravado.
func verificaSequencia(snaps []*snapGlobal, _n int) []string {
	var v []string
	nMarcadores := _n - 1 // um marcador por canal de saida, a cada estado gravado

	// (a) os identificadores sao consecutivos, sem buracos.  Nao se exige que comecem
	//     em 1: depois de restaurar um snapshot, a execucao continua a numeracao
	//     (o proximo snapshot e o SnId do restaurado + 1)
	for i := 1; i < len(snaps); i++ {
		if snaps[i].snId != snaps[i-1].snId+1 {
			v = append(v, fmt.Sprintf("os identificadores de snapshot tem um buraco: o snapshot %d vem logo apos o %d",
				snaps[i].snId, snaps[i-1].snId))
			break
		}
	}

	// (b) por processo: MkSeq cresce de 1 em 1 (um marcador por estado gravado),
	//     e o relogio logico e o instante da gravacao nunca andam para tras
	for id := range snaps[0].procs {
		var anterior *procSnap
		var snAnterior int
		for _, s := range snaps {
			p, ok := s.procs[id]
			if !ok {
				continue
			}
			if anterior != nil {
				// cada estado gravado envia um marcador por canal de saida, ou seja,
				// N-1 marcadores
				if p.rec.MkSeq != anterior.rec.MkSeq+nMarcadores {
					v = append(v, fmt.Sprintf("processo %d: no snapshot %d gravou MkSeq=%d, mas no snapshot %d tinha gravado MkSeq=%d (deveria ser +%d, um marcador por canal de saida)", id, p.rec.SnId, p.rec.MkSeq, snAnterior, anterior.rec.MkSeq, nMarcadores))
				}
				if p.rec.Lcl < anterior.rec.Lcl {
					v = append(v, fmt.Sprintf("processo %d: o relogio logico andou para tras (%d no snapshot %d, era %d no snapshot %d)", id, p.rec.Lcl, p.rec.SnId, anterior.rec.Lcl, snAnterior))
				}
				if p.rec.RecTime < anterior.rec.RecTime {
					v = append(v, fmt.Sprintf("processo %d: o instante da gravacao andou para tras (snapshot %d gravado antes do snapshot %d)", id, p.rec.SnId, snAnterior))
				}
			}
			anterior, snAnterior = p, p.rec.SnId
		}
	}
	return v
}

// ------------------------------------------------------------------------------------
// ------- leitura dos arquivos de snapshot
// ------------------------------------------------------------------------------------

// le todos os arquivos snap_<id>.txt do diretorio e monta, para cada SnId, o estado global
func carregar(_dir string) ([]*snapGlobal, int, int, error) {

	arquivos, err := filepath.Glob(filepath.Join(_dir, "snap_*.txt"))
	if err != nil {
		return nil, 0, 0, err
	}
	if len(arquivos) == 0 {
		return nil, 0, 0, fmt.Errorf("nenhum arquivo snap_*.txt em %q", _dir)
	}

	n := 0
	porId := map[int]*snapGlobal{}
	var ordem []int
	dups := 0 // registros repetidos (o mesmo processo gravando o mesmo snapshot 2x)

	for _, arq := range arquivos {
		f, err := os.Open(arq)
		if err != nil {
			return nil, 0, dups, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1024*1024), 1024*1024)
		linha := 0
		for sc.Scan() {
			linha++
			texto := strings.TrimSpace(sc.Text())
			if texto == "" {
				continue
			}
			var rec DIMEX.SnapRecord
			if err := json.Unmarshal([]byte(texto), &rec); err != nil {
				fmt.Printf("aviso: %s linha %d ignorada (JSON invalido): %v\n", arq, linha, err)
				continue
			}
			if rec.ProcId+1 > n {
				n = rec.ProcId + 1
			}
			s, ok := porId[rec.SnId]
			if !ok {
				s = &snapGlobal{snId: rec.SnId, procs: map[int]*procSnap{}}
				porId[rec.SnId] = s
				ordem = append(ordem, rec.SnId)
			}
			if _, repetido := s.procs[rec.ProcId]; repetido {
				dups++
				if dups <= 5 {
					fmt.Printf("!! snapshot %d gravado mais de uma vez pelo processo %d (%s)\n", rec.SnId, rec.ProcId, arq)
				}
			}
			ps := &procSnap{rec: rec, channels: map[int][]string{}}
			for _, c := range rec.Chans {
				ps.channels[c.From] = c.Msgs
			}
			s.procs[rec.ProcId] = ps
		}
		if err := sc.Err(); err != nil {
			f.Close()
			return nil, 0, dups, err
		}
		f.Close()
	}

	sort.Ints(ordem)
	var saida []*snapGlobal
	for _, id := range ordem {
		saida = append(saida, porId[id])
	}
	return saida, n, dups, nil
}

// ------------------------------------------------------------------------------------
// ------- analise de liveness (nao e invariante: e uma heuristica de bloqueio)
// ------------------------------------------------------------------------------------

// Um processo que aparece em wantMX com o MESMO reqTs numa sequencia longa de
// snapshots nunca avancou: e um forte indicio de que o sistema travou, por exemplo
// porque alguem alterou a condicao de resposta e nunca mais liberou a SC.
// Conta-se a partir do FIM da sequencia, porque o que importa e o que esta
// acontecendo agora, e nao o que aconteceu no comeco da execucao.
func suspectosDeBloqueio(snaps []*snapGlobal, _minimo int) []string {
	if len(snaps) == 0 {
		return nil
	}
	ultimo := snaps[len(snaps)-1]
	var v []string
	for id, ps := range ultimo.procs {
		if ps.rec.St != "wantMX" {
			continue
		}
		// quantos snapshots finais consecutivos mostram este processo parado
		parado := 0
		for i := len(snaps) - 1; i >= 0; i-- {
			p, ok := snaps[i].procs[id]
			if !ok {
				continue
			}
			if p.rec.St != "wantMX" || p.rec.ReqTs != ps.rec.ReqTs {
				break
			}
			parado++
		}
		if parado < _minimo {
			continue
		}
		// de quem ele esta esperando?  quem tem o pedido dele na fila, ou quem
		// simplesmente nunca respondeu
		naFila := []int{}
		semResposta := []int{}
		for p := range ultimo.procs {
			if p == id {
				continue
			}
			if ultimo.waiting(p, id) {
				naFila = append(naFila, p)
			} else if ultimo.lastReqTsFrom(p, id) == ps.rec.ReqTs {
				semResposta = append(semResposta, p) // processou o pedido e nao respondeu
			}
		}
		sort.Ints(naFila)
		sort.Ints(semResposta)
		v = append(v, fmt.Sprintf("processo %d esta travado em wantMX (reqTs=%d) nos %d ultimos snapshots; "+
			"pedido na fila de %v e SEM RESPOSTA de %v",
			id, ps.rec.ReqTs, parado, naFila, semResposta))
	}
	sort.Strings(v)
	return v
}

// Quantas vezes cada processo entrou na Secao Critica, do ponto de vista da
// sequencia de snapshots.  Serve para mostrar se o sistema fez progresso.
func entradasNaSC(snaps []*snapGlobal) map[int]int {
	entradas := map[int]int{}
	for _, s := range snaps {
		for _, id := range s.inMX() {
			entradas[id]++
		}
	}
	return entradas
}

// ------------------------------------------------------------------------------------
// ------- main
// ------------------------------------------------------------------------------------

func main() {

	dir := "./snapshots"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}

	// modo -lista: so os identificadores dos snapshots completos, um por linha.
	// e o que o runRestore.sh usa para escolher qual snapshot vai restaurar.
	if len(os.Args) > 2 && os.Args[2] == "-lista" {
		snaps, n, _, err := carregar(dir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "erro:", err)
			os.Exit(2)
		}
		for _, s := range snaps {
			if s.n() == n {
				fmt.Println(s.snId)
			}
		}
		return
	}

	fmt.Println("======================================================================")
	fmt.Println(" AVALIADOR DE SNAPSHOTS DO DIMEX  (Chandy-Lamport)")
	fmt.Println(" diretorio de snapshots:", dir)
	fmt.Println("======================================================================")

	snaps, n, dups, err := carregar(dir)
	if err != nil {
		fmt.Println("erro:", err)
		os.Exit(2)
	}

	fmt.Printf("snapshots lidos: %d   processos: %d   registros duplicados: %d\n", len(snaps), n, dups)
	fmt.Println()

	// snaps incompletos: algum processo nao gravou (o sistema pode ter parado no meio)
	completos := make([]*snapGlobal, 0, len(snaps))
	incompletos := 0
	for _, s := range snaps {
		if s.n() == n {
			completos = append(completos, s)
		} else {
			incompletos++
		}
	}
	if incompletos > 0 {
		fmt.Printf("aviso: %d snapshot(s) incompletos (nao gravados por todos os %d processos) foram ignorados\n", incompletos, n)
		fmt.Println()
	}
	if len(completos) == 0 {
		fmt.Println("nenhum snapshot completo para avaliar")
		os.Exit(2)
	}

	// avaliacao
	type invResult struct {
		inv       inv
		violacoes int
		primeiro  int    // primeiro SnId violado
		exemplo   string // exemplo de mensagem de violacao
	}
	resultados := make([]invResult, len(invariantes))
	for i, iv := range invariantes {
		resultados[i].inv = iv
	}
	violados := map[int]bool{}

	for _, s := range completos {
		for i, iv := range invariantes {
			// o bool e o caminho rapido: no caso saudavel (e no caso normal, em todos
			// os cortes) o teste roda uma unica vez. So quando o bool e falso o mesmo
			// teste e refeito, para obter a lista de violacoes que vai ser mostrada.
			if iv.funcao_InvX(s) {
				continue
			}
			msgs := iv.testa(s)
			if !violados[i] {
				resultados[i].primeiro = s.snId
				resultados[i].exemplo = msgs[0]
				violados[i] = true
			}
			resultados[i].violacoes++
			if resultados[i].violacoes <= 3 { // mostra algumas violacoes de cada
				for _, m := range msgs {
					fmt.Printf("!! snapshot %d  %s: %s\n", s.snId, iv.nome, m)
				}
			}
		}
	}

	// relatorio
	fmt.Println()
	fmt.Println("----------------------------------------------------------------------")
	fmt.Printf(" RELATORIO DE INVARIANTES  (%d snapshots avaliados)\n", len(completos))
	fmt.Println("----------------------------------------------------------------------")
	totalInv, totalSeq := 0, 0
	for _, r := range resultados {
		if r.violacoes == 0 {
			fmt.Printf("  %-6s OK      %s\n", r.inv.nome, r.inv.desc)
		} else {
			totalInv += r.violacoes
			fmt.Printf("  %-6s FALHOU  %s  (%d snapshots com violacao, primeiro SnId=%d)\n",
				r.inv.nome, r.inv.desc, r.violacoes, r.primeiro)
			fmt.Printf("         exemplo: %s\n", r.exemplo)
		}
	}

	// verificacoes que valem para a sequencia de snapshots
	fmt.Println()
	fmt.Println("----------------------------------------------------------------------")
	fmt.Println(" VERIFICACOES DA SEQUENCIA DE SNAPSHOTS")
	fmt.Println("----------------------------------------------------------------------")
	problemas := verificaSequencia(completos, n)
	if len(problemas) == 0 {
		fmt.Println("  identificadores consecutivos sem buracos (a execucao original comeca em 1;")
		fmt.Println("  numa execucao restaurada, continua a partir do snapshot restaurado + 1),")
		fmt.Println("  um marcador por estado gravado, e relogio logico / instante da gravacao")
		fmt.Println("  sempre em ordem")
	} else {
		for _, s := range problemas {
			fmt.Printf("  !! %s\n", s)
		}
		totalSeq += len(problemas)
	}

	// progresso: quantas vezes cada processo aparece dentro da SC na sequencia
	fmt.Println()
	fmt.Println("----------------------------------------------------------------------")
	fmt.Println(" PROGRESSO DO SISTEMA (contagem de snapshots em que o processo esta na SC)")
	fmt.Println("----------------------------------------------------------------------")
	entradas := entradasNaSC(completos)
	var linhas []string
	for id := 0; id < n; id++ {
		linhas = append(linhas, fmt.Sprintf("  processo %d: %d de %d snapshots dentro da SC", id, entradas[id], len(completos)))
	}
	if len(linhas) == 0 || len(entradas) == 0 {
		fmt.Println("  NENHUM processo entrou na Secao Critica em nenhum dos snapshots: o sistema nao fez progresso")
	}
	for _, l := range linhas {
		fmt.Println(l)
	}

	// liveness
	fmt.Println()
	fmt.Println("----------------------------------------------------------------------")
	fmt.Println(" HEURISTICA DE BLOQUEIO (nao e invariante)")
	fmt.Println("----------------------------------------------------------------------")
	// um processo travado e aquele que nao sai de wantMX com o mesmo reqTs por
	// pelo menos 20% dos snapshots (e no minimo 5)
	minimo := len(completos) / 5
	if minimo < 5 {
		minimo = 5
	}
	suspeitos := suspectosDeBloqueio(completos, minimo)
	if len(suspeitos) == 0 {
		fmt.Printf("  nenhum processo ficou travado em wantMX por %d ou mais snapshots seguidos\n", minimo)
	} else {
		for _, s := range suspeitos {
			fmt.Printf("  !! %s\n", s)
		}
	}

	fmt.Println()
	fmt.Println("----------------------------------------------------------------------")
	if totalInv == 0 && totalSeq == 0 && dups == 0 && len(suspeitos) == 0 {
		fmt.Printf(" RESULTADO: todos os %d snapshots satisfeitos todas as %d invariantes.\n", len(completos), len(invariantes))
		os.Exit(0)
	}
	fmt.Printf(" RESULTADO: %d violacao(oes) de invariante(s), %d problema(s) na sequencia, %d registro(s) duplicado(s) e %d suspeita(s) de bloqueio.\n",
		totalInv, totalSeq, dups, len(suspeitos))
	os.Exit(1)
}
