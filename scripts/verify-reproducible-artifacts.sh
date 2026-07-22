#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release_version="${1:-0.0.0-reproducible}"
revision="${BUILD_REVISION:-$(git -C "$repo_root" rev-parse HEAD)}"
temporary_directory="$(mktemp -d)"
trap 'rm -rf "$temporary_directory"' EXIT

gradle_arguments=(
  -p "$repo_root"
  clean jar sourcesJar javadocJar
  --no-daemon
  "-PreleaseVersion=$release_version"
  "-PbuildRevision=$revision"
)
if [[ -n "${YKS_LOCAL_PATH:-}" ]]; then
  gradle_arguments+=("-Pyks.localPath=$YKS_LOCAL_PATH")
fi

build_artifacts() {
  "$repo_root/gradlew" "${gradle_arguments[@]}"
}

copy_artifacts() {
  local destination="$1"
  while IFS= read -r artifact; do
    relative_path="${artifact#"$repo_root"/}"
    mkdir -p "$destination/$(dirname "$relative_path")"
    cp "$artifact" "$destination/$relative_path"
  done < <(
    find "$repo_root" \
      -path "$repo_root/yks" -prune -o \
      -path '*/build/libs/*.jar' -type f -print |
      sort
  )
}

build_artifacts
copy_artifacts "$temporary_directory/baseline"
if [[ -z "$(find "$temporary_directory/baseline" -type f -print -quit)" ]]; then
  echo "No JAR artifacts were produced" >&2
  exit 1
fi

build_artifacts
copy_artifacts "$temporary_directory/current"

diff -qr "$temporary_directory/baseline" "$temporary_directory/current"
echo "Reproducible artifacts verified for revision $revision"
