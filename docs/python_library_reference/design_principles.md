
## System decomposition and boundaries

1. **Information hiding (design around “secrets”)**
   Good looks like: each module/component has a small set of internal decisions it “owns” (data layout, algorithms, vendor quirks, wire formats), and other code *cannot* accidentally depend on those decisions. Callers interact through a stable surface that stays meaningful even if internals change radically. When information hiding is working, changes feel “local”: you update one module and the blast radius is small and predictable.

2. **Separation of concerns**
   Good looks like: policy and domain rules are readable without wading through IO, parsing, logging, retries, metrics, concurrency, or framework glue. Mechanisms exist, but they’re clearly “plumbing” and can be swapped or modified without rewriting the rules. When this is working, you can point to where “the decisions” live vs where “the wiring” lives.

3. **Single Responsibility as “one reason to change”**
   Good looks like: you can explain why a file/module/class changes in a single sentence, and that sentence doesn’t contain “and.” Components that change for different reasons live apart, even if today they happen to be edited together. When SRP is working, the codebase has fewer “mixed-purpose” objects that become dumping grounds.

4. **High cohesion, low coupling**
   Good looks like: related concepts live together, and unrelated concepts don’t share a home just because it was convenient. Low coupling means components communicate via narrow, intention-revealing interfaces rather than by poking into each other’s internals. When it’s working, you can understand or test one component without needing to load a big mental model of the whole system.

5. **Dependency direction (inward dependencies)**
   Good looks like: the most important logic (core rules, domain operations, semantics) depends on almost nothing, while the “details” (DB, filesystem, network, UI, frameworks) depend on the core. The core should not know what library or vendor you used; adapters should. When this is working, swapping a database/client/transport is mostly confined to the edge.

6. **Ports & Adapters (Hexagonal)**
   Good looks like: the core expresses what it *needs* from the world via ports (“I need to load/store X”, “I need to fetch Y”, “I need time/logging/metrics”), and adapters implement those ports for specific technologies. The boundary is explicit and test-friendly: you can replace an adapter with a fake in minutes. When it’s working, external systems stop infecting your internal types and logic.

## Shared types, schemas, and “knowledge”

7. **DRY is about knowledge, not lines**
   Good looks like: there is a single authoritative place for an invariant, schema, mapping rule, or policy — and everything else calls into it or references it. Duplication that matters most is “semantic duplication” (two places encode the same business truth), because it drifts over time. When DRY is working, changes to rules or schema don’t require hunting for “other copies.”

8. **Design by contract (explicit preconditions/postconditions/invariants)**
   Good looks like: functions and modules make clear what they require, what they guarantee, and what cannot happen (invariants). Contracts are enforced via types, assertions, validation at boundaries, and tests; the goal is to prevent subtle “garbage in, garbage out” propagation. When it’s working, failures happen early and are easy to attribute.

9. **Parse, don’t validate**
   Good looks like: you convert messy inputs into a structured representation once, at the boundary, and after that you operate on well-formed values. Validation becomes a side effect of parsing into types/constructors that can’t represent invalid states. When it’s working, the rest of your code contains fewer scattered “if missing field / if wrong type / if out of range” checks.

10. **Make illegal states unrepresentable**
    Good looks like: your data model prevents impossible combinations by construction (e.g., states encoded as enums/variants, required fields enforced, mutually exclusive options modeled explicitly). Instead of “a bunch of fields with rules in comments,” you have constructors/variants that embody the rules. When it’s working, bugs shift from runtime surprises to compile-time/type-time or construction-time errors.

11. **Command–Query Separation (CQS)**
    Good looks like: functions either return information (queries) *or* change the world/state (commands), but not both. This makes reasoning, caching, retries, and tests much safer because you don’t accidentally trigger side effects while “just reading.” When it’s working, you can call queries freely without fear, and commands are easy to audit.

## Dependencies, composition, and construction

12. **Dependency inversion + explicit composition**
    Good looks like: high-level logic depends on abstractions and stable concepts, not on low-level details. The system’s object graph is assembled intentionally (often in one place), and dependencies are passed in rather than silently created in many scattered spots. When it’s working, substituting implementations and writing isolated tests is straightforward.

13. **Prefer composition over inheritance**
    Good looks like: behavior is built by combining smaller components rather than by deep class hierarchies. Composition keeps relationships explicit (“this uses that”) and avoids brittle base-class coupling. When it’s working, extending behavior rarely requires modifying a base class and hoping nothing else breaks.

14. **Law of Demeter (least knowledge)**
    Good looks like: code talks to its direct collaborators, not to “a collaborator’s collaborator’s collaborator.” This limits how much structure leaks across boundaries and prevents cascading breakage when internal representations shift. When it’s working, refactors that restructure internals don’t force widespread call-site rewrites.

15. **Tell, don’t ask**
    Good looks like: objects/modules encapsulate their rules and behavior, rather than exposing raw data everywhere and having unrelated code re-implement logic on top of it. This reduces duplicated logic and keeps invariants close to the data they govern. When it’s working, you see fewer “anemic models” and fewer scattered conditional chains operating on the same shape.

## Effects, determinism, and pipeline correctness

16. **Functional core, imperative shell**
    Good looks like: the heart of the system is composed of deterministic transformations that are easy to test and reason about, while IO and orchestration live at the edges. Side effects are concentrated and visible rather than smeared throughout. When it’s working, most logic can be tested without mocks of the world.

17. **Idempotency as a design goal**
    Good looks like: re-running operations with the same inputs doesn’t corrupt state or produce inconsistent outcomes; it either does nothing or produces the same results. This enables retries, incremental rebuilds, and resilient orchestration without “special cleanup procedures.” When it’s working, failure recovery is simple: rerun rather than manually intervene.

18. **Determinism / reproducibility**
    Good looks like: given the same inputs and environment, you get the same outputs (artifacts, derived tables, indexes, plans), or you explicitly record and control sources of nondeterminism. Determinism dramatically reduces debugging cost because you can reproduce a problem reliably. When it’s working, caching becomes safe and “what changed?” questions are easy to answer.

## Simplicity, evolution, and predictability

19. **KISS (simplicity as a constraint)**
    Good looks like: solutions are as simple as possible *without* sacrificing essential boundaries and clarity. You add complexity only when it pays for itself in reduced coupling or clearer contracts. When it’s working, new contributors can read the system top-down without constantly needing “tribal knowledge.”

20. **YAGNI (avoid speculative generality)**
    Good looks like: you don’t introduce abstraction layers without a clear, near-term second use case or a clearly identified change vector that the abstraction isolates. You design with seams that *allow* extension later, but you don’t build the extension mechanism before it’s needed. When it’s working, the codebase feels lean, and abstractions are easy to justify.

21. **Principle of least astonishment**
    Good looks like: APIs behave how a competent reader expects; naming, return types, error behavior, and side effects match common conventions. Surprises are expensive because they force everyone to re-learn local rules. When it’s working, callers can use components correctly with minimal documentation and without defensive programming everywhere.

22. **Declare and version your public contracts**
    Good looks like: you are explicit about what is “stable surface” (schemas, CLI outputs, internal protocol formats, key interfaces) and what is internal detail. Versioning and compatibility rules clarify when breakage is acceptable and how evolution happens. When it’s working, changes are intentional and communicated through clear contract boundaries rather than accidental.

## Testability and observability

23. **Design for testability**
    Good looks like: units can be tested without heavyweight setup because dependencies are injectable, pure logic is separated from IO, and contracts are explicit. Tests become cheap and fast, which encourages more of them and enables confident refactoring. When it’s working, you rarely need elaborate integration scaffolding just to test core behavior.

24. **Observability as consistent, structured data**
    Good looks like: logging/metrics/tracing form a coherent narrative with stable fields and semantics; you can answer “what happened?” and “why?” without ad hoc print-debugging. Observability isn’t sprinkled randomly — it’s aligned to boundaries, key operations, and failure modes. When it’s working, you can correlate events across layers and reproduce operational issues from telemetry.


