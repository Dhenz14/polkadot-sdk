#!/usr/bin/env sh

set -eu

readme="${1:-README.md}"
upstream_raw='https://raw.githubusercontent.com/paritytech/polkadot-sdk/master/scripts/getting-started.sh'

if grep -F "$upstream_raw" "$readme" >/dev/null; then
	printf '%s\n' "error: $readme points quickstart at upstream paritytech master."
	printf '%s\n' "Use scripts/getting-started.sh from this checkout or pin a Dhenz14 tag/commit."
	exit 1
fi

if grep -E 'raw\.githubusercontent\.com/paritytech/polkadot-sdk/master/scripts/getting-started\.sh.*\|[[:space:]]*(ba)?sh' "$readme" >/dev/null; then
	printf '%s\n' "error: $readme contains an upstream paritytech master curl-bash quickstart."
	printf '%s\n' "Use scripts/getting-started.sh from this checkout or pin a Dhenz14 tag/commit."
	exit 1
fi

printf '%s\n' "fork-boundary README check passed"
