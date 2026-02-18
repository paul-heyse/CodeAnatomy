#!/usr/bin/env bash
# Run conformance integration tests across selected backend lanes.

set -Eeuo pipefail

lanes="${1:-${CODEANATOMY_CONFORMANCE_BACKENDS:-fs,minio,localstack}}"
IFS=',' read -r -a backend_lanes <<< "${lanes}"

if [ "${#backend_lanes[@]}" -eq 0 ]; then
  echo "No conformance backends selected." >&2
  exit 1
fi

validate_lane() {
  local lane="$1"
  case "${lane}" in
    fs|minio|localstack) ;;
    *)
      echo "Unsupported conformance backend lane: ${lane}" >&2
      exit 1
      ;;
  esac
}

require_env() {
  local name="$1"
  local lane="$2"
  if [ -z "${!name:-}" ]; then
    echo "Missing required env var for ${lane} lane: ${name}" >&2
    exit 1
  fi
}

for lane in "${backend_lanes[@]}"; do
  lane="$(echo "${lane}" | xargs)"
  validate_lane "${lane}"
done

for lane in "${backend_lanes[@]}"; do
  lane="$(echo "${lane}" | xargs)"
  echo "==> Running conformance lane: ${lane}"

  case "${lane}" in
    minio)
      require_env "CODEANATOMY_MINIO_ENDPOINT" "minio"
      ;;
    localstack)
      require_env "CODEANATOMY_LOCALSTACK_ENDPOINT" "localstack"
      ;;
  esac

  CODEANATOMY_CONFORMANCE_BACKEND="${lane}" \
    uv run pytest tests/integration/conformance -m integration -v
done
