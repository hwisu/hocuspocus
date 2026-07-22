#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
port="${HOCUSPOCUS_JVM_PORT:-19876}"
java_home="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home}"
gradle_args=()

export JAVA_HOME="$java_home"
export PATH="$JAVA_HOME/bin:$PATH"

if [[ -n "${YKS_LOCAL_PATH:-}" ]]; then
  gradle_args+=("-Pyks.localPath=${YKS_LOCAL_PATH}")
elif [[ -f "${repo_root}/../yks/settings.gradle.kts" ]]; then
  gradle_args+=("-Pyks.localPath=${repo_root}/../yks")
fi

cd "$repo_root"
./gradlew "${gradle_args[@]}" :hocuspocus-ktor-example:installDist --no-daemon
pnpm install --frozen-lockfile

PORT="$port" ./hocuspocus-ktor-example/build/install/hocuspocus-ktor-example/bin/hocuspocus-ktor-example \
  >"${TMPDIR:-/tmp}/hocuspocus-jvm-oracle.log" 2>&1 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true' EXIT

for _ in $(seq 1 100); do
  if curl --fail --silent "http://127.0.0.1:${port}/health" >/dev/null; then
    break
  fi
  sleep 0.1
done
curl --fail --silent "http://127.0.0.1:${port}/health" >/dev/null
HOCUSPOCUS_JVM_PORT="$port" node ./interop/provider-v4.mjs
