#!/usr/bin/env bash
#
# Sobe N processos DIMEX, coleta uma sequencia de snapshots e roda a ferramenta
# de avaliacao sobre eles.
#
#   ./runSnapshots.sh            3 processos, 20s de execucao
#   N=4 DUR=30s ./runSnapshots.sh
#   N=3 DUR=20s DIMEX_SNAP_INIT=1 ./runSnapshots.sh
#
set -u

N=${N:-3}                      # numero de processos
DUR=${DUR:-20s}                # tempo de execucao do sistema
DIR=${DIMEX_SNAP_DIR:-snapshots}
LOGS=./logs
BASE=${BASE:-5000}             # porta do processo 0 ; processo i usa BASE+1000*i
export DIMEX_SNAP_DIR="$DIR"
export DIMEX_SNAP_INIT=${DIMEX_SNAP_INIT:-0}
export DIMEX_SNAP_EVERY=${DIMEX_SNAP_EVERY:-100ms}
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
    echo "   uso: N=3 DUR=20s $0" >&2
    exit 1
    ;;
esac
if [ "$N" -lt 2 ]; then
  echo "!! N tem de ser pelo menos 2 (o trabalho pede no minimo 3): N=$N" >&2
  exit 1
fi

echo "==> limpando $DIR e $LOGS"
rm -rf "$DIR" "$LOGS"
mkdir -p "$DIR" "$LOGS"

echo "==> compilando"
go build -o "$LOGS/dimex" ./useDIMEX.go || exit 1
go build -o "$LOGS/snapCheck" ./snapCheck || exit 1

ADDRS=()
for ((i = 0; i < N; i++)); do
  ADDRS+=("127.0.0.1:$((BASE + 1000 * i))")
done
echo "==> $N processos: ${ADDRS[*]}"

PIDS=()
for ((i = 0; i < N; i++)); do
  "$LOGS/dimex" "$i" "${ADDRS[@]}" >"$LOGS/proc$i.log" 2>&1 &
  PIDS+=($!)
done

cleanup() {
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  wait 2>/dev/null
}
trap cleanup EXIT

echo "==> executando por $DUR"
sleep "$DUR"
cleanup
trap - EXIT
sleep 0.3   # espera a ultima escrita nos arquivos

echo
echo "==> arquivos de snapshot gerados:"
ls -l "$DIR"
echo "    total de registros: $(cat "$DIR"/snap_*.txt 2>/dev/null | wc -l)"
echo "    por processo:"
for f in "$DIR"/snap_*.txt; do
  echo "      $f: $(wc -l <"$f") registros"
done

echo
echo "==> avaliando os snapshots"
"$LOGS/snapCheck" "$DIR"
VERIF=$?
echo "==> saida da verificacao: $VERIF  (0 = todas as invariantes ok)"

# a falha da verificacao tem que aparecer na saida do script: um pipeline de
# verificacao que engole o erro nao serve para nada
case "$VERIF" in
  0) ;;
  2) echo "!! a verificacao nao pode ser feita: nao ha snapshots completos" >&2 ;;
  *) echo "!! a verificacao achou problemas (veja o relatorio acima)" >&2 ;;
esac
exit "$VERIF"
