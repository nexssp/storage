package nexssflow

import (
	"context"
	"strings"

	"github.com/nexssp/flow/compiler"

	"github.com/nexssp/storage"
)

// Directive parses `@storage <kind>:<backend> [as <alias>] { options }`
// and appends a Declaration to the shared store.
//
// Examples:
//
//	@storage kv:memory { ttl: "5m" }
//	@storage kv:redis as cache { addr: "redis://localhost" }
//	@storage blob:s3 as files { bucket: "my-bucket" }
//
// Without `as`, the store name is `<kind>:<backend>` (e.g. "kv:memory").
// With `as`, the store name is the alias. Two declarations with the
// same store name produce a Conflict error at compile time.
//
// The actions `storage.kv.get`, `storage.kv.set`, etc. reference the
// store by name via the `store:` argument:
//
//	storage.kv.get @{ store: "kv:memory", key: "user:1" }
func storageDirective() compiler.Directive {
	return compiler.Directive{
		Name:    "storage",
		Example: `@storage kv:memory { ttl: "5m" }`,
		Handler: func(_ context.Context, req compiler.DirectiveReq) (compiler.DirectiveRes, error) {
			line := strings.TrimSpace(req.Lines[req.I])
			rest := strings.TrimSpace(strings.TrimPrefix(line, "@storage"))
			pos := compiler.Position{File: req.File, Line: req.I + 1}
			if rest == "" {
				return compiler.DirectiveRes{}, compiler.SourceError(pos,
					"@storage: expected `<kind>:<backend> [as <alias>] { options }`")
			}
			header, hasBlock := splitHeader(rest)
			namePart, alias, err := parseHeader(header)
			if err != nil {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@storage: %v", err)
			}
			kind, backend, err := parseName(namePart)
			if err != nil {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@storage: %v", err)
			}
			storeName := namePart
			if alias != "" {
				storeName = alias
			}
			var options map[string]string
			next := req.I + 1
			if hasBlock {
				parsed, after, err := compiler.ParseBlockOptions(req.Lines, req.I)
				if err != nil {
					return compiler.DirectiveRes{}, compiler.SourceError(pos,
						"@storage %s: %v", storeName, err)
				}
				options = parsed
				next = after
			}
			decl := storage.Declaration{
				Name:     storeName,
				Backend:  backend,
				Kind:     kind,
				Options:  options,
				Position: storage.Position{File: req.File, Line: req.I + 1},
			}
			existing, _ := req.Out["storage"].([]storage.Declaration)
			for i := range existing {
				if existing[i].Name == storeName {
					return compiler.DirectiveRes{}, compiler.SourceError(pos,
						"@storage %s: duplicate declaration in this file", storeName)
				}
			}
			req.Out["storage"] = append(existing, decl)
			return compiler.DirectiveRes{Next: next}, nil
		},
	}
}

// splitHeader splits rest into (header, hasBlock). If rest contains a
// `{` at depth 0, everything before it is the header and the rest
// (including the brace) is the options block. Otherwise, the whole
// rest is the header and hasBlock is false.
//
// We scan for the first `{` that is not inside a quoted string. This
// matches the simplicity of the @pool and @config directives, which
// also assume a single-line header followed by an optional block.
func splitHeader(rest string) (header string, hasBlock bool) {
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '{':
			return strings.TrimSpace(rest[:i]), true
		case '"':
			// Skip quoted strings — a `{` inside a string is not a
			// block delimiter.
			i++
			for i < len(rest) && rest[i] != '"' {
				if rest[i] == '\\' && i+1 < len(rest) {
					i++
				}
				i++
			}
		}
	}
	return strings.TrimSpace(rest), false
}

// parseHeader splits "kv:memory as cache" into ("kv:memory", "cache").
// Without `as`, returns (header, "", nil).
func parseHeader(header string) (name, alias string, err error) {
	name, alias, found := strings.Cut(header, " as ")
	if !found {
		return header, "", nil
	}
	name = strings.TrimSpace(name)
	alias = strings.TrimSpace(alias)
	if name == "" {
		return "", "", storage.BadRequestf("storage: name is required before `as`")
	}
	if alias == "" {
		return "", "", storage.BadRequestf("storage: `as` requires a non-empty alias")
	}
	return name, alias, nil
}

// parseName splits "kv:redis" into (KindKV, "redis"). The kind must
// be one of the known storage.Kind values; the backend must be a
// non-empty identifier.
func parseName(s string) (storage.Kind, string, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", storage.BadRequestf(
			"storage: name must be `<kind>:<backend>`, got %q", s)
	}
	kind := storage.Kind(parts[0])
	switch kind {
	case storage.KindKV, storage.KindBlob, storage.KindLock,
		storage.KindFencedLock, storage.KindIdem, storage.KindJournal,
		storage.KindTx, storage.KindRateLimit:
		// ok
	default:
		return "", "", storage.BadRequestf(
			"storage: unknown kind %q in %q (want one of %v)",
			parts[0], s, storage.AllKinds())
	}
	return kind, parts[1], nil
}
