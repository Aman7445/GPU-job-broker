#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ] || { [ "$#" -eq 2 ] && [ "$2" != "--push" ]; }; then
  echo "Usage: $0 IMAGE[:TAG] [--push]" >&2
  exit 2
fi

project_dir=$(cd "$(dirname "$0")" && pwd)
engine=${CONTAINER_ENGINE:-docker}
engine_name=$(basename "$engine")
image=$1
build_args=(-f "$project_dir/Dockerfile" -t "$image")
if [ "$engine_name" = "podman" ]; then
  build_args+=(--format docker --ignorefile "$project_dir/Dockerfile.dockerignore")
fi

"$engine" build "${build_args[@]}" "$project_dir"
"$engine" run --rm -v "$project_dir/tests:/app/tests:ro" "$image" python -m unittest discover -s tests -v
CONTAINER_ENGINE="$engine" bash "$project_dir/smoke.sh" "$image"

if [ "${2:-}" = "--push" ]; then
  if [ "$engine_name" = "podman" ]; then
    digest_file=$(mktemp)
    trap 'rm -f "$digest_file"' EXIT
    "$engine" push --digestfile "$digest_file" "$image"
    repository=$image
    if [[ ${image##*/} == *:* ]]; then
      repository=${image%:*}
    fi
    printf '%s@%s\n' "$repository" "$(cat "$digest_file")"
  else
    "$engine" push "$image"
    "$engine" image inspect "$image" --format '{{range .RepoDigests}}{{println .}}{{end}}'
  fi
fi
