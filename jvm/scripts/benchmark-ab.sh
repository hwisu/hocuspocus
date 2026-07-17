#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
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
pnpm install --frozen-lockfile
pnpm build:packages
./jvm/gradlew -p jvm "${gradle_args[@]}" \
  :hocuspocus-benchmark:installDist \
  --no-daemon
node ./jvm/benchmark/compare-provider.mjs "$@"
