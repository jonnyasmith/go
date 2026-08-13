# go-systems

A collection of independent Go programs, each a standalone systems component. The solution has no shared runtime and no shared code — only shared vocabulary.

## Language

**Module**:
One of the five top-level components (`cache`, `proxy`, `resp`, `queue`, `sysmon`). Each is a self-contained Go module with its own README, glossary, and decisions.
_Avoid_: Project, app, service, package

**Independence**:
The rule that no module imports, links to, or assumes the presence of another. Two modules solving overlapping problems is intended, not duplication to be factored out.
_Avoid_: Decoupling, isolation

**Working target**:
The module a task is scoped to. Instructions, vocabulary, and decisions are resolved against the working target first, and only then against the solution root.
_Avoid_: Context, scope, workspace
