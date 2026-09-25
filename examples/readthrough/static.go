package main

import "embed"

// staticFiles embeds the demo's single-page UI directly into the binary,
// so the reference app has no separate frontend build step or asset
// directory to deploy alongside it.
//
//go:embed static
var staticFiles embed.FS
