/*  Injeção de falhas no DIMEX, para a etapa 4 do trabalho:
    alterar o comportamento do módulo e verificar que a análise dos snapshots
    DETECTA a alteração.

    Como usar (as variáveis de ambiente são lidas por processo):
        DIMEX_FAULT=violarSC DIMEX_FAULT_ID=1   o processo 1 passa a responder "ok"
                                                  mesmo estando na Seção Crítica
        DIMEX_FAULT=bloquear  DIMEX_FAULT_ID=1   o processo 1 "esquece" todos os
                                                  pedidos: não responde e não enfileira
        DIMEX_FAULT=naoGravarCanal DIMEX_FAULT_ID=1
                                                o processo 1 para de guardar as
                                                mensagens que chegam no estado dos
                                                canais de entrada do snapshot
        DIMEX_FAULT=perderMarcador DIMEX_FAULT_ID=1
                                                o processo 1 descarta o primeiro
                                                marcador de cada snapshot que recebe
        DIMEX_FAULT=gravarDuasVezes DIMEX_FAULT_ID=1
                                                o processo 1 grava o mesmo registro
                                                duas vezes no seu arquivo
        DIMEX_FAULT_ID=-1 (padrão)               a falha vale para todos os processos

    As duas primeiras são falhas do ALGORITMO DE EXCLUSÃO MÚTUA, e as três últimas
    são falhas do próprio ALGORITMO DE SNAPSHOT: são justamente elas que a
    ferramenta de avaliação precisa enxergar.

    Com DIMEX_FAULT vazio (padrão) nada muda: o DIMEX roda normalmente.
*/

package DIMEX

import (
	"os"
	"strconv"
	"strings"
)

const (
	// responde "ok" mesmo estando na SC, ignorando o relógio lógico
	faultViolarSC = "violarsc"
	// nunca responde: nem concede nem enfileira o pedido
	faultBloquear = "bloquear"
	// não guarda as mensagens que chegam no estado dos canais de entrada
	faultNaoGravarCanal = "naogravarcanal"
	// descarta (como se a rede tivesse perdido) o primeiro marcador de cada snapshot
	faultPerderMarcador = "perdermarcador"
	// grava o mesmo registro duas vezes
	faultGravarDuasVezes = "gravarduasvezes"
)

var (
	faultModo = strings.ToLower(strings.TrimSpace(os.Getenv("DIMEX_FAULT")))
	faultDe   = envIntOrDefault("DIMEX_FAULT_ID", -1)
)

func envIntOrDefault(_nome string, _padrao int) int {
	if v := os.Getenv(_nome); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return _padrao
}

// a falha programada vale para ESTE processo?
func (module *DIMEX_Module) falhaAtiva(_modo string) bool {
	return faultModo == _modo && (faultDe == -1 || faultDe == module.id)
}
