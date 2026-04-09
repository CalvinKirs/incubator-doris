#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
OVERRIDE_OIDC_HTTP_PORT="${OIDC_HTTP_PORT:-}"
. "${ROOT}/oidc.env"
if [[ -n "${OVERRIDE_OIDC_HTTP_PORT}" ]]; then
    export OIDC_HTTP_PORT="${OVERRIDE_OIDC_HTTP_PORT}"
fi

ISSUER="http://127.0.0.1:${OIDC_HTTP_PORT}/realms/${OIDC_REALM}"
TOKEN_URL="${ISSUER}/protocol/openid-connect/token"
JWKS_URI="${ISSUER}/protocol/openid-connect/certs"
TOKEN_DIR="${ROOT}/tokens"
MANIFEST_FILE="${TOKEN_DIR}/manifest.json"

mkdir -p "${TOKEN_DIR}"
rm -f "${TOKEN_DIR}"/*.access_token "${MANIFEST_FILE}"

if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required to generate OIDC access tokens" >&2
    exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is required to decode generated OIDC access tokens" >&2
    exit 1
fi

curl --noproxy '*' -fsS "${ISSUER}/.well-known/openid-configuration" >/dev/null

declare -a manifest_entries=()
entries_file="$(mktemp)"
trap 'rm -f "${entries_file}"' EXIT

generate_case() {
    local name="$1"
    local client_id="$2"
    local client_secret="$3"
    local username="$4"
    local password="$5"
    local requested_scope="$6"
    local output_path="${TOKEN_DIR}/${name}.access_token"
    local response_file
    response_file="$(mktemp)"

    if [[ -n "${requested_scope}" ]]; then
        curl --noproxy '*' -fsS -X POST "${TOKEN_URL}" \
            -H 'Content-Type: application/x-www-form-urlencoded' \
            --data-urlencode "client_id=${client_id}" \
            --data-urlencode "client_secret=${client_secret}" \
            --data-urlencode "username=${username}" \
            --data-urlencode "password=${password}" \
            --data-urlencode 'grant_type=password' \
            --data-urlencode "scope=${requested_scope}" \
            >"${response_file}"
    else
        curl --noproxy '*' -fsS -X POST "${TOKEN_URL}" \
            -H 'Content-Type: application/x-www-form-urlencoded' \
            --data-urlencode "client_id=${client_id}" \
            --data-urlencode "client_secret=${client_secret}" \
            --data-urlencode "username=${username}" \
            --data-urlencode "password=${password}" \
            --data-urlencode 'grant_type=password' \
            >"${response_file}"
    fi

    local access_token
    access_token="$(
        python3 - "${response_file}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
token = payload.get("access_token")
if not token:
    raise SystemExit(1)
print(token)
PY
    )"
    if [[ -z "${access_token}" || "${access_token}" == "null" ]]; then
        echo "Failed to fetch access token for ${name}" >&2
        cat "${response_file}" >&2
        rm -f "${response_file}"
        exit 1
    fi

    printf '%s\n' "${access_token}" >"${output_path}"
    manifest_entries+=("$(
        python3 - "${name}" "${output_path}" "${username}" "${client_id}" "${requested_scope}" "${access_token}" <<'PY'
import base64
import json
import sys

name, path, username, client_id, requested_scope, token = sys.argv[1:]
parts = token.split(".")
if len(parts) != 3:
    raise SystemExit(f"Token for {name} is not a JWT")
payload = parts[1]
payload += "=" * (-len(payload) % 4)
claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
audience = claims.get("aud")
if isinstance(audience, str):
    audience = [audience]
ttl_seconds = None
if isinstance(claims.get("exp"), int) and isinstance(claims.get("iat"), int):
    ttl_seconds = claims["exp"] - claims["iat"]
print(json.dumps({
    "name": name,
    "path": path,
    "username": username,
    "clientId": client_id,
    "requestedScope": requested_scope,
    "claims": {
        "preferred_username": claims.get("preferred_username"),
        "sub": claims.get("sub"),
        "aud": audience,
        "azp": claims.get("azp"),
        "scope": claims.get("scope"),
        "doris_groups": claims.get("doris_groups"),
        "tenant": claims.get("tenant"),
        "department": claims.get("department"),
        "iat": claims.get("iat"),
        "exp": claims.get("exp"),
        "ttlSeconds": ttl_seconds
    }
}, sort_keys=True))
PY
    )")
    rm -f "${response_file}"
}

generate_case "oidc_alice_grafana_query" \
    "${OIDC_GRAFANA_CLIENT_ID}" \
    "${OIDC_GRAFANA_CLIENT_SECRET}" \
    "${OIDC_ALICE_USER}" \
    "${OIDC_ALICE_PASSWORD}" \
    "doris.query doris.profile"
generate_case "oidc_alice_grafana_missing_scope" \
    "${OIDC_GRAFANA_CLIENT_ID}" \
    "${OIDC_GRAFANA_CLIENT_SECRET}" \
    "${OIDC_ALICE_USER}" \
    "${OIDC_ALICE_PASSWORD}" \
    "doris.profile"
generate_case "oidc_alice_agent_query" \
    "${OIDC_AGENT_CLIENT_ID}" \
    "${OIDC_AGENT_CLIENT_SECRET}" \
    "${OIDC_ALICE_USER}" \
    "${OIDC_ALICE_PASSWORD}" \
    "doris.query"
generate_case "oidc_bob_grafana_query" \
    "${OIDC_GRAFANA_CLIENT_ID}" \
    "${OIDC_GRAFANA_CLIENT_SECRET}" \
    "${OIDC_BOB_USER}" \
    "${OIDC_BOB_PASSWORD}" \
    "doris.query"
generate_case "oidc_carol_grafana_query" \
    "${OIDC_GRAFANA_CLIENT_ID}" \
    "${OIDC_GRAFANA_CLIENT_SECRET}" \
    "${OIDC_CAROL_USER}" \
    "${OIDC_CAROL_PASSWORD}" \
    "doris.query"
generate_case "oidc_dave_grafana_query" \
    "${OIDC_GRAFANA_CLIENT_ID}" \
    "${OIDC_GRAFANA_CLIENT_SECRET}" \
    "${OIDC_DAVE_USER}" \
    "${OIDC_DAVE_PASSWORD}" \
    "doris.query"
generate_case "oidc_erin_grafana_query" \
    "${OIDC_GRAFANA_CLIENT_ID}" \
    "${OIDC_GRAFANA_CLIENT_SECRET}" \
    "${OIDC_ERIN_USER}" \
    "${OIDC_ERIN_PASSWORD}" \
    "doris.query"
generate_case "oidc_frank_legacy_query" \
    "${OIDC_LEGACY_CLIENT_ID}" \
    "${OIDC_LEGACY_CLIENT_SECRET}" \
    "${OIDC_FRANK_USER}" \
    "${OIDC_FRANK_PASSWORD}" \
    "doris.query"

printf '%s\n' "${manifest_entries[@]}" >"${entries_file}"

python3 - \
    "${entries_file}" \
    "${ISSUER}" \
    "${JWKS_URI}" \
    "${OIDC_DORIS_AUDIENCE}" \
    "${OIDC_GROUPS_CLAIM}" \
    "${OIDC_TOKEN_TTL_SECONDS}" \
    "${MANIFEST_FILE}" <<'PY'
import json
import sys

entries_file, issuer, jwks_uri, audience, groups_claim, token_ttl_seconds, manifest_file = sys.argv[1:]
expected_ttl = int(token_ttl_seconds)
with open(entries_file, "r", encoding="utf-8") as handle:
    tokens = [json.loads(line) for line in handle if line.strip()]

for token in tokens:
    ttl_seconds = token["claims"].get("ttlSeconds")
    if ttl_seconds is None or abs(ttl_seconds - expected_ttl) > 60:
        raise SystemExit(
            f"Generated token {token['name']} has unexpected ttlSeconds={ttl_seconds}, expected around {expected_ttl}"
        )

manifest = {
    "issuer": issuer,
    "jwksUri": jwks_uri,
    "audience": audience,
    "groupsClaim": groups_claim,
    "tokenTtlSeconds": expected_ttl,
    "tokens": tokens,
}

with open(manifest_file, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

echo "Generated OIDC token fixtures under ${TOKEN_DIR}"
