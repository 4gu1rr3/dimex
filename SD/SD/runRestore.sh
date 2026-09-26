#!/usr/bin/env bash
#
# Demonstra o ciclo completo do Chandy-Lamport:
#
#   1. roda o sistema, coletando uma sequencia de snapshots;
#   2. escolhe um dos snapshots;
#   3. RESTAURA o estado global daquele corte (estado local de cada processo +
#      estado dos canais de entrada) e CONTINUA a execucao dali;
#   4. avalia, com a mesma ferramenta, os snapshots da execucao restaurada.
#
# O que se espera ver:
#   - o snapshot restaurado e um estado global consistente (9/9 invariantes);
#   - a execucao restaurada CONTINUA a numeracao: o primeiro snapshot gravado
#     depois da restauracao tem SnId = (snapshot restaurado) + 1;
#   - a execucao restaurada tambem passa nas 9 invariantes, ou seja, o sistema
#     restabeleceu a exclusao mutua e o proprio mecanismo de snapshot.
#
#   ./runRestore.sh                    3 processos, 15s + 15s, restaura o do meio
#   N=4 DUR=20s DUR2=20s SN=100 ./runRestore.sh
#   N=3 DUR=15s ESCOLHER=ultimo ./runRestore.sh
#
set -u

N=${N:-3}                      # numero de processos
DUR=${DUR:-15s}                # tempo de execucao antes de restaurar
DUR2=${DUR2:-15s}              # tempo de execucao depois de restaurar
BASE=${BASE:-6000}             # portas da 1a execucao: 6000+1000*i
BASE2=${BASE2:-7000}           # portas da 2a execucao (nao pode conflitar)
SN=${SN:-}                     # qual snapshot restaurar (vazio = escolhido abaixo)
ESCOLHER=${ESCOLHER:-meio}     # meio | ultimo | primeiro
DIR=snapshots                  # execucao original
DIR2=snapshots_restaurados     # execucao que parte do estado restaurado
LOGS=./logs
export DIMEX_SNAP_INIT=${DIMEX_SNAP_INIT:-0}
export DIMEX_GAP_MS=${DIMEX_GAP_MS:-6}
export DIMEX_CS_MS=${DIMEX_CS_MS:-6}
export GOFLAGS=-mod=mod
export PATH="$HOME/.local/go/bin:$PATH"

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE" || exit 1

# N tem de ser um numero inteiro: com um valor invalido o "for ((i=0;i<N;i++))"
# aborta o script com um erro de sintaxe do bash, e como nao ha "set -e" a execucao
# continuaria até o fim sem ter rodado nada
case "$N" in
  '' | *[!0-9]*)
    echo "!! N tem de ser um numero inteiro: N='$N'" >&2
    echo "   uso: N=3 DUR=20s DUR2=20s $0" >&2
    exit 1
    ;;
esac
if [ "$N" -lt 2 ]; then
  echo "!! N tem de ser pelo menos 2 (o trabalho pede no minimo 3): N=$N" >&2
  exit 1
fi

PIDS=()

sobe_e_espera() {   # $1 = tag dos logs, $2 = tempo de execucao
  local tag=$1 dur=$2 i
  for ((i = 0; i < N; i++)); do
    "$LOGS/dimex" "$i" "${ADDRS[@]}" >"$LOGS/proc${tag}$i.log" 2>&1 &
    PIDS+=($!)
  done
  sleep "$dur"
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  wait 2>/dev/null
  PIDS=()
  sleep 0.3   # espera a ultima escrita nos arquivos
}

monta_enderecos() { # $1 = porta base
  local i
  ADDRS=()
  for ((i = 0; i < N; i++)); do
    ADDRS+=("127.0.0.1:$(( $1 + 1000 * i ))")
  done
}

RECOLHER=${RECOLHER:-1}         # 1 = coleta snapshots novos; 0 = reaproveita os de $DIR

if [ "$RECOLHER" = "1" ]; then
  echo "==> limpando $DIR, $DIR2 e $LOGS"
  rm -rf "$DIR" "$DIR2" "$LOGS"
  mkdir -p "$DIR" "$DIR2" "$LOGS"
else
  echo "==> reaproveitando os snapshots que ja estao em $DIR"
  rm -rf "$DIR2"
  mkdir -p "$DIR2" "$LOGS"
fi

echo "==> compilando"
go build -o "$LOGS/dimex" ./useDIMEX.go || exit 1
go build -o "$LOGS/snapCheck" ./snapCheck || exit 1

# ------------------------------------------------------------------ execucao 1
if [ "$RECOLHER" = "1" ]; then
  monta_enderecos "$BASE"
  echo "==> execucao 1: $N processos em ${ADDRS[*]} por $DUR, snapshots a cada ${DIMEX_SNAP_EVERY:-100ms}"
  DIMEX_SNAP_DIR="$DIR" DIMEX_SNAP_EVERY=${DIMEX_SNAP_EVERY:-100ms} sobe_e_espera a "$DUR"
fi

echo
echo "==> avaliando os snapshots da execucao original"
"$LOGS/snapCheck" "$DIR" >"$LOGS/relatorio_original.txt" 2>&1
grep -E "snapshots lidos|  (OK|FALHOU)|buraco" "$LOGS/relatorio_original.txt" | sed 's/^/    /'

# escolhe o snapshot a restaurar
LISTA=$("$LOGS/snapCheck" "$DIR" -lista)
if [ -z "$LISTA" ]; then
  echo "!! nao ha snapshot completo para restaurar" >&2
  exit 2
fi
if [ -z "$SN" ]; then
  case "$ESCOLHER" in
    ultimo)   SN=$(echo "$LISTA" | tail -1) ;;
    primeiro) SN=$(echo "$LISTA" | head -1) ;;
    *)        SN=$(echo "$LISTA" | awk 'NR==int((NR_TOTAL+1)/2){print}' NR_TOTAL=$(echo "$LISTA" | wc -l)) ;;
  esac
fi
TOTAL=$(echo "$LISTA" | wc -l)
echo
echo "==> $TOTAL snapshots completos; restaurando o SnId=$SN"

# ------------------------------------------------------------------ execucao 2
monta_enderecos "$BASE2"
echo "==> execucao 2 (restaurada): $N processos em ${ADDRS[*]} por $DUR2"
DIMEX_SNAP_DIR="$DIR2" DIMEX_SNAP_EVERY=2s \
DIMEX_RESTORE="$DIR" DIMEX_RESTORE_SN="$SN" sobe_e_espera b "$DUR2"

echo
echo "==> como cada processo voltou do snapshot $SN:"
for ((i = 0; i < N; i++)); do
  grep -h "RESTAURA\|RESTAURADO" "$LOGS/procb$i.log" | head -6 | sed 's/^/    /'
done

echo
echo "==> avaliando os snapshots da execucao RESTAURADA"
"$LOGS/snapCheck" "$DIR2" >"$LOGS/relatorio_restaurado.txt" 2>&1
VERIF=$?
sed -n '/RELATORIO/,$p' "$LOGS/relatorio_restaurado.txt" | grep -vE "^$"

PRIMEIRO=$("$LOGS/snapCheck" "$DIR2" -lista | head -1)
ULTIMO=$("$LOGS/snapCheck" "$DIR2" -lista | tail -1)
echo
echo "==> continuacao da numeracao: restaurado o snapshot $SN, a execucao gravou $PRIMEIRO..$ULTIMO"
if [ -n "$PRIMEIRO" ] && [ "$PRIMEIRO" -eq $((SN + 1)) ]; then
  echo "    OK: a execucao restaurada continuou a partir do snapshot $((SN + 1))"
else
  echo "    ATENCAO: o primeiro snapshot da execucao restaurada foi $PRIMEIRO (esperado $((SN + 1)))" >&2
  [ "$VERIF" -eq 0 ] && VERIF=1
fi

exit "$VERIF"
