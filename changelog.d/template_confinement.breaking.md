Sinks that accept `{{ field }}` references in routing templates now enforce a
confinement boundary: the rendered value must stay within the literal prefix
declared in the template. Templates with no literal prefix (e.g.
`key_prefix: "{{ host }}/"`) are rejected at startup. The `file` sink is the
only exception: its `base_dir` config field can provide an explicit
confinement root for `path` templates with no usable literal prefix.

Any sink that includes a templated config field can be affected.

The `file` sink gains a `base_dir` config field to set the confinement root
explicitly when the `path` template has no usable literal prefix.

**HTTP-family templates:** HTTP/HTTPS URI templates that use `{{ field }}`
references must not contain `?` or `#`. A field-rendered value could smuggle
additional query parameters or fragments into the rendered URI. Fully static URI
templates (no `{{ }}`) with a query string or fragment are still accepted.
Dynamic query or fragment segments (e.g.
`https://api.internal/ingest?tenant={{ tenant }}`) are rejected at startup.
Templated `request.headers` values are also confined for HTTP-family sinks.

**Opt-out:** set `dangerously_allow_unconfined_template_resolution: true` on
the affected sink to disable all confinement checks for that sink — both at
startup and at runtime. Vector logs a warning per template on startup and sets
`vector_security_confinement_disabled{component_type=...}` to `1`.

**Observability:**

- `component_errors_total{error_type="confinement_failed"}` — increments on
  each violation; events that trigger it are dropped.
- `vector_security_confinement_disabled` — set to `1` while a sink is running
  with confinement disabled.

authors: pront
