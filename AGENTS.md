# AGENTS.md — Agent Operating Protocol (AOP)

> **Purpose**: This file is the agent-first playbook for building and maintaining this codebase at **best‑in‑class** quality. It specifies *exact commands*, *acceptance gates*, and *fallbacks* so both humans and AI agents can ship production‑grade Python safely and quickly.

---

## Table of contents
1. [Agent Operating Protocol (TL;DR)](#agent-operating-protocol-tldr)
2. [Environment Setup (Agent‑grade, deterministic)](#environment-setup-agentgrade-deterministic)
3. [Source‑of‑Truth Index](#source-of-truth-index)
4. [Code Formatting & Style (Ruff is canonical)](#code-formatting--style-ruff-is-canonical)
5. [Type Checking (pyright strict, pyrefly sharp)](#type-checking-pyright-strict-pyrefly-sharp)
6. [Docstrings (NumPy style; enforced; runnable)](#docstrings-numpy-style-enforced-runnable)
7. [Testing Standards (markers & coverage)](#testing-standards-markers--coverage)
8. [Data Contracts (JSON Schema 2020‑12 / OpenAPI 3.2)](#data-contracts-json-schema-2020-12--openapi-32)
9. [Link Policy for Remote Editors](#link-policy-for-remote-editors)
10. [Agent Catalog & STDIO API (session‑scoped, no daemon)](#agent-catalog--stdio-api-sessionscoped-no-daemon)
11. [Task Playbooks (feature / refactor / bugfix)](#task-playbooks-feature--refactor--bugfix)
12. [PR Template & Checklist](#pr-template--checklist)
13. [Quick Commands (copy/paste)](#quick-commands-copypaste)
14. [Troubleshooting (for Agents)](#troubleshooting-for-agents)
15. [Security & Compliance Hygiene](#security--compliance-hygiene)
16. [Repo Layout & Do‑not‑edit Zones](#repo-layout--do-not-edit-zones)
17. [CI / Pre‑commit parity](#ci--precommit-parity)
18. [Glossary](#glossary)

---



1) For all python code run via the terminal you must use "uv run". Failing to use "uv run" when running python commands will result in error messages about missing module imports due to failure to properly utilize the uv based environment



4) **Git protocol**
- Do not use git checkout or any other file manipulation via git under any circumstance. If you notice file changes unrelated to your changes, assume they are conscientious changes by the user that your are not involved with and that you do not have to address them in any way. 
- Do not worry about git cleanliness. The user has conscientiously configured their .gitignore file and you do not have to worry about build files, coverage data, etc. being committed


### Quality checks (zero-error mandate)

For all code blocks that you make edits to, please check for pyright, pyrefly, and Ruff errors or warnings and ensure that by the time you have concluded your edits in a code block there are zero errors in any of those categories. Your error checking and resolution should not solely focus on your scope, all errors in the files that you have made edits in are part of the scope of your review when implementing a change and should be fully resolved before you conclude your work. Under no circumstances should you ever suppress errors, even stylistic Ruff errors. Instead of suppressing errors, derive a structural solution that causally addresses the source of the error, even if it is intensive to implement. Below is the complete set of rules that you must adhere to in order to align with the project pyright, pyrefly, and ruff rule sets:

#### Comprehensive list of rules to comply with on code formatting (all rules are "MUST" unless an exception is explicitly given)

Strict Python Code Compliance Rules
This contract defines every enforceable coding constraint from our Ruff linter, Pyrefly type checker, and Pyright (strict mode) configurations. All rules are mandatory and phrased as must or must not directives, grouped by theme for clarity. AI code generation must adhere to all of these rules.
##### Part I: Imports and Module Structure
    • Absolute Imports Only: Code must not use relative imports; all imports must be absolute with full module paths[1]. No from .module import X or import ..package – always use the complete package name.
    • Type-Checking Imports: Any import used only for type annotations must be enclosed in an if TYPE_CHECKING: block (to avoid runtime overhead)[2]. This keeps type-only dependencies out of runtime execution.
    • No Duplicate Imports: Code must not import the same module more than once. If something is already imported, reuse it instead of importing again.
    • Standard Aliases: Certain libraries must be imported using their conventional aliases. For example, import NumPy as np, pandas as pd, and matplotlib.pyplot as plt[5]. Do not deviate from established aliases (e.g., do not import pandas as pandas or as a non-standard name).
    • No Star Imports: Wildcard imports (e.g. from module import *) are not allowed. Always import explicitly what is needed to avoid namespace pollution and ensure clarity.
    • No Useless Aliases: Avoid import aliases that do nothing. For example, must not do import numpy as numpy or import module as module (aliasing to the same name)[8].
    • Unused Imports: Every import in the code must be used. Remove any import that is not actually referenced in the module (the linter treats unused imports as errors[9]). No imported names should remain unused.
    • Executable Scripts: If a Python file is meant to be run as a script, it must include a proper shebang line (e.g., #!/usr/bin/env python3) and have executable permissions. Conversely, library modules must not contain shebang lines or executable file permissions unless they are truly intended as entry points (enforced by executable-check rules[10]).
##### Part II: Code Style and Naming Conventions
    • Indentation: Use 4 spaces per indentation level. Code must use spaces (not tabs) for indenting blocks[12]. Mixed tabs and spaces or incorrect indent multiples are disallowed[13].
    • Quotation Style: Use double quotes for all string literals, including docstrings[12]. Single quotes should be used only when a string contains double quotes and no easier escaping is possible. Consistent quoting is enforced – e.g., "example" rather than 'example' unless necessary.
    • Whitespace and Formatting: Follow PEP 8 spacing rules strictly. There must be no trailing whitespace at line ends. Files must end with a single newline (no extra blank lines at EOF)[13][14]. Do not include extraneous spaces around operators, commas, brackets, or before colons in slices (except as allowed by the formatter)[15]. No more than one blank line in a row in the middle of code (aside from separating definitions as noted below).
    • Blank Lines: Separate top-level definitions (functions, classes) with two blank lines for clarity[13]. Inside a class, separate methods with a single blank line. A class’s docstring, however, should appear immediately after the class definition with no blank line separating them (see Docstrings section)[16].
    • Single Statement per Line: Each line of code must contain at most one statement. Do not put multiple statements on one line (e.g., avoid using semicolons ; to cram multiple commands in one line)[13].
    • Naming – General: All names in code must follow standard Python naming conventions (PEP 8)[17][18]. This means CapWords (PascalCase) for class names, lower_case_with_underscores for functions and variables, UPPER_CASE for constants. Do not use CamelCase for variables or functions, and do not use lowercase names for classes.
    • Naming – Variables and Attributes: Module-level or global constants should be named in all caps with underscores (e.g. MAX_RETRIES)[19]. Ordinary variables (including those in functions) must be lowercase (and not mixedCase)[20]. In class scope, avoid mixedCase attributes; use snake_case unless they are meant to be constants[21].
    • Naming – Functions and Methods: Function and method names must be lowercase, with words separated by underscores[22]. Do not use CamelCase for function names. Also, names must not start or end with double underscores unless implementing a special dunder method (e.g., __str__ is allowed, but no custom name should both begin and end with __)[20][23].
    • First Argument Names: The first parameter of an instance method must be named self[18]. The first parameter of a class method must be named cls[18]. Do not use other names for these special parameters.
    • Naming – Exceptions: Custom exception class names must end with the word “Error”[24] (to clearly indicate an error/exception type).
    • No Builtin Shadowing: Never use a name that shadows a Python built-in or keyword. Variables, attributes, and function parameters must not use names like list, dict, id, file, etc., which would override Python’s built-in names[25]. This applies to class attributes as well[26]. Importing a module must not shadow a built-in name either[27].
    • ASCII-only Identifiers: Identifiers must use standard letters, numbers, and underscores only (ASCII characters). Avoid using Unicode characters in names. (Non-ASCII in identifiers is flagged as a naming convention violation[28].)
    • Duplicate Definitions: There must not be duplicate definitions of the same class attribute or class field in a class (no repeating an attribute name in the class body)[29]. Each name should be defined only once in its scope.
    • Enum Uniqueness: If using enum.Enum, all member values must be unique. Do not define two enum members with the same value[29].
    • No Unnecessary Pass: Avoid using pass where it’s not needed. For instance, an empty class or function body can use ... (ellipsis) instead of pass in stub files, and in regular code an empty function should at least have a docstring or a comment. Unneeded pass statements are discouraged[30].
    • No Print Debugging: The code must not contain print statements or pretty-print (pprint) calls for debugging purposes. Printing to stdout/stderr for debugging (e.g., print("Debug")) is forbidden[31][32]. Use proper logging instead (and even then, only as needed).
    • Pathlib for Paths: When handling filesystem paths, the code must prefer pathlib.Path objects over direct string manipulation or os.path functions (enforced by use-pathlib rules). Build paths using Path operations instead of concatenating strings[33].
##### Part III: Functions, Complexity, and Best Practices
    • Function Length and Complexity: Functions and methods must remain reasonably simple. The cyclomatic complexity of a function must not exceed 10 branches[34]. In practice, this means avoiding overly complicated logic flows – refactor if a single function has too many if/elif branches or loop nesting. Additionally, a single function must not have more than 12 distinct branches (decision points) as measured by Pylint[35], and must not contain more than 6 return statements in total[35]. Keep functions focused and straightforward.
    • Parameter Count: While not explicitly configured, follow best practice by keeping the number of parameters manageable (Pylint’s default threshold is 5 arguments). Functions should not have an excessive number of parameters; break them into smaller functions or use objects to encapsulate if needed.
    • No Unused Function Arguments: Every function parameter defined must be used in the function’s body. Do not include parameters that serve no purpose (lint will flag unused function arguments[33]). If a function signature is constrained by an interface (thus unused parameters), explicitly prefix the name with an underscore to indicate it’s unused.
    • Consistent Return Statements: Functions must have consistent return behavior. If a function is declared to return a non-None value, then all code paths must return an appropriate value of that type. Do not let a function implicitly return None when a value is expected[36][37]. Conversely, if a function is intended to return nothing (None), do not include stray return statements with values on some code paths.
    • No Implicit None Returns: If a function could logically fail to return a value, ensure an explicit return is at the end. Missing a return at the end of a function that should return a value is not allowed[38].
    • No Redundant return None: Do not explicitly return None at the end of a function if None is the only possible return value[39]. In such cases, omit the return or just use a bare return for clarity.
    • No Useless Assign-Then-Return: Avoid assigning to a local variable right before returning it. For example, x = compute(); return x is unnecessary – just return compute() directly. The linter will flag variables that are immediately returned without other uses[40].
    • No Redundant Else after Return/Break: Do not use else (or elif) blocks immediately following a return, break, continue, or raise statement. Once you’ve returned or broken out, the else is superfluous. Instead, structure the control flow without that redundant else[41][42].
    • Avoid Return in Finally: Code must not use a return (or continue/break) inside a finally block of a try statement[43]. Doing so would suppress exceptions possibly raised in the try/except sections. Let the finally block finish without non-local control flow.
    • Loops and Comprehensions: If a loop variable is not used inside the loop, that’s a problem – either use it or rewrite the loop (or use an underscore placeholder)[44][45]. Prefer comprehension syntax for simple list/set/dict building instead of accumulating in a loop (performance and clarity). For example, use a list comprehension instead of a for loop with .append() when appropriate[46][47]. However, avoid complex multi-line comprehensions that hurt readability.
    • Use enumerate and zip Wisely: When iterating with an index, use enumerate() instead of managing an index manually[48]. When iterating two sequences in parallel, use zip() instead of indexing both. Ensure you don’t reuse the iterator returned by itertools.groupby() or similar more than once (as it will be exhausted)[49].
    • No Yoda Conditions: Avoid “Yoda conditions” – writing literal comparisons in reverse. For instance, do not write if 5 == count:; write if count == 5: for clarity[50].
    • Boolean Expressions: Simplify boolean expressions. Do not compare boolean values to True/False using == or != – instead, use the boolean value directly or its negation[51]. E.g., replace if is_ready == False: with if not is_ready:. Similarly, avoid double negation (not (not x))[51] – just use x. Do not use patterns like True if cond else False; just use cond directly (and similarly, avoid False if cond else True; use not cond)[52][46].
    • Avoid Pointless Statements: Every statement should have an effect. Do not leave “useless” statements or expressions in the code (e.g., a standalone expression that is not assigned or used) – these do nothing and should be removed[53][54].
    • Mutable Default Arguments: Must not use lists, dictionaries, or other mutable objects as default values for function arguments[55]. Default parameters are evaluated at function definition time and shared across calls, leading to bugs. Use None (and inside the function, assign a new list if needed) or other immutable values as defaults.
    • No Function Call in Default Args: Similarly, must not call functions or perform complex expressions in default parameter values[56]. Defaults should be static expressions. If a default value needs a function call (like a timestamp or random number), compute it inside the function, not in the signature.
    • Avoid Repeated Code Patterns: Be mindful of duplicate patterns that could be refactored. For example, do not make multiple identical isinstance checks on the same variable – combine them into one check with a tuple of types[57]. Do not write consecutive if statements that can be merged or simplified (like nested ifs that could be a single if with and)[58].
    • Resource Management: Always use context managers to handle resources. Opening files must be done with a with open(...) as f: block rather than open() without closing[59]. Do not leave file handles or similar resources to be closed implicitly – handle them in a with or ensure explicit close in finally blocks.
    • Combine Context Managers: When using multiple context managers, prefer a single with statement with commas, instead of nested separate with statements for each[60].
    • Use Literal Collections: When a string is being split into characters or a similar static operation, prefer using a literal form or existing methods. (E.g., don’t do list("abc"), just use ["a","b","c"] or the string directly as needed. The linter flags unnecessary conversions like casting to list before iteration[61].)
    • Performance Considerations: Follow performance best practices flagged by the linters. For example, do not use len(x) in a boolean context without comparison if the intention is to check non-emptiness – either compare to 0 explicitly or simply use if x: (depending on internal guidelines)[62]. Avoid putting try/except inside a tight loop if it can be moved outside (catch exceptions outside loops to prevent slowdown)[63]. Use dictionary comprehensions and list comprehensions instead of manual loops to build data structures where it improves clarity and speed[47]. Don’t copy lists or other iterables in inefficient ways (e.g., avoid unnecessary intermediate list casts)[61].
    • NumPy and Pandas Best Practices: Use NumPy/Pandas idioms correctly and avoid deprecated or slow patterns:
    • In pandas, do not use the inplace=True argument; it is discouraged due to often unclear behavior[64]. Prefer non-inplace methods that return new DataFrames.
    • Avoid the deprecated .ix indexer in pandas; use .loc or .iloc explicitly[65]. Also use .loc instead of .at, and .iloc instead of .iat for clarity and consistency[66].
    • Use .isna() / .notna() instead of .isnull() / .notnull() for checking missing data[67] – they are equivalent, but the former are the preferred names.
    • Prefer DataFrame.merge(...) method over the pandas top-level pd.merge(...) function for merging dataframes[68], to keep code object-oriented.
    • Avoid using .values on DataFrames/Series; use .to_numpy() for clarity when you need the underlying array[69].
    • Use pivot_table instead of the older pivot or unstack when creating pivot tables[69].
    • Don’t use overly generic names like df for DataFrame variables in complex contexts[70]; while a short name is common for simple examples, in larger scopes use a descriptive name.
    • No Commented-Out Code: Do not leave commented-out code fragments in the codebase. Remove dead code entirely instead of commenting it out[71][72]. The presence of large commented code blocks is considered a violation (it’s flagged as “commented-out code” by the linter).
##### Part IV: Error Handling and Logging
    • No Bare Exceptions: Every except clause must specify an exception type. Never use a bare except: with no exception class[73]. If you intend to catch all exceptions, at least use except Exception: (and even that should be used sparingly).
    • Avoid Broad Exception Catches: Catch the most specific exception that makes sense. Do not catch high-level exceptions like Exception or BaseException unless you genuinely need to handle all errors. In particular, do not silence system-exiting exceptions (like KeyboardInterrupt or SystemExit) unintentionally by catching BaseException.
    • No Silent Pass in Except: An exception handler must not be completely empty or just pass. Swallowing exceptions without at least logging or handling is not allowed[73]. If you truly want to ignore an exception, explicitly state it or use contextlib.suppress() with the specific exception.
    • Use contextlib.suppress for Expected Exceptions: If the logic is intentionally ignoring certain benign exceptions, use a with contextlib.suppress(SomeError): block instead of a try/except/pass pattern[74]. This makes the intent clear and is a cleaner idiom than an empty except.
    • Reraise Exceptions Properly: When catching an exception with the intent to reraise it (possibly after some handling), use a bare raise to rethrow the original exception with its traceback intact, or raise new_exception from e to chain exceptions[75]. Must not catch an exception and then raise a different exception without using from – always preserve context by using raise X from e so that the original stack trace is not lost[76]. Also, do not explicitly name the exception in a raise if you just want to rethrow; raise by itself is correct.
    • Avoid Useless Try/Except: Do not write try/except blocks that immediately re-raise the exception without handling. For example, catching an exception only to log and rethrow is acceptable (logging is handling), but catching and then doing raise with nothing else is redundant – let it propagate instead[77]. Remove any try/except that doesn’t meaningfully handle the error.
    • else and finally in Try Blocks: Use else blocks on try/except when you have code that should run when no exception occurs, rather than putting all logic in the try. This clarifies error handling. Likewise, avoid putting code in finally that might raise exceptions or interfere with exceptions (especially avoid return in finally as noted). Only use finally for cleanup that must run regardless of errors.
    • Raise Specific Exceptions: When raising errors, raise specific exception classes. Do not use the base Exception class for errors that have a more appropriate type. For instance, if a function gets a bad argument, raise ValueError or TypeError as appropriate, not a generic Exception[78]. If an operation is not implemented, raise NotImplementedError (not bare NotImplemented which is a special constant).
    • Don’t Raise Literals: You must not use a literal or raw value with a raise statement (e.g. raise "Error" or raise 5). Only Exception objects (or subclasses) can be raised[79]. If you see raise False or similar, that’s incorrect – throw an appropriate Exception subclass instead.
    • Type Checking Exceptions: If manually validating types or conditions, use the correct exception. For example, raising TypeError for a wrong type, ValueError for an out-of-range or invalid value, etc. (There is a specific rule to prefer TypeError when a function gets an argument of the wrong type[80].)
    • No assert for Runtime Checks: Do not use assert statements for enforcing business logic or data validation in production code. Asserts are stripped out when Python is run with optimizations and are meant for internal consistency checks. If something is an actual runtime error condition, raise an exception instead of using assert. In particular, never use assert False to deliberately fail – raise an appropriate exception instead[81].
    • Logging Practices: Use the logging library instead of prints for non-trivial output. In exception handlers, prefer logging.exception("message") to log an error with traceback. Do not use logging.warn(), as it is deprecated – use logging.warning()[82]. Also ensure that your log messages’ format strings match the arguments: no missing or extra placeholders. The static analysis will error on mismatched logging format placeholders and arguments[83].
    • No Redundant Exception Info in Logs: When logging an exception via logging.exception() or logging.error(exc_info=True), do not include the exception message or traceback text in the log message manually. For example, logging.exception("Failed to do X: %s", err) is redundant because logging.exception will already log the exception info. Just provide context message; avoid duplicating exception text.
    • Don’t Overly Broadly Except: We already forbid bare except, but also be cautious not to catch exceptions too broadly only to ignore or log them. If you catch Exception, consider if it should be narrower. And never use except Exception as a way to control flow for non-error conditions.
    • No Blanket Pass on KeyboardInterrupt/SystemExit: Unless in a very specific top-level scenario, do not catch KeyboardInterrupt or SystemExit and suppress them. The program should be allowed to exit on those.
    • Avoid Try-Except in Loops for Flow: Don’t use exceptions for normal loop flow control (like using a try/except inside a loop to break out – restructure the logic or use other approaches). Also, be mindful that try/except inside a heavily repeated loop can degrade performance[63].
    • Graceful Resource Cleanup: If you catch exceptions to do cleanup, ensure you re-raise or handle appropriately after cleanup. Do not just suppress the exception unless it’s truly non-critical.
    • Testing Exceptions: In test code, prefer using testing frameworks’ facilities to check for exceptions rather than bare asserts of exception text or overly broad try/except.
Security and Dangerous Functions:
    • No eval or exec: The code must not use Python’s eval() or exec() builtins for executing dynamic code from strings[84][82]. Executing arbitrary strings is unsafe and against policy. Use safe parsing or explicit logic instead of eval/exec.
    • No assert for Security: As mentioned, assert statements should not be used for checks that ensure security or data integrity. They can be optimized out; use explicit exceptions.
    • Avoid execfile or input() misuse: Similarly, do not use deprecated or dangerous functions like execfile. When reading input, validate and never use input() to execute code.
    • Subprocess and Shell: If executing external commands, do not use shell=True with unsanitized input. Avoid using os.system for complex tasks – prefer the subprocess module with careful parameterization. This is more of a best practice (the linters may not catch it explicitly unless using Bandit rules).
    • No Hardcoded Credentials: Do not hardcode passwords, API keys, or other secrets in the code. Such patterns may be caught by security linters or secret scanners (even if not explicitly in Ruff rules, it’s an essential practice).
    • Cryptography: Use standard cryptographic libraries and avoid writing your own or using outdated algorithms. (E.g., do not use the random module for security-critical randomness – use secrets or os.urandom.)
    • Input Validation: Validate inputs from untrusted sources. This is a general guideline: ensure functions check and sanitize inputs if used in security contexts (though this may not be enforced by static analysis, it’s expected in secure code).
(The security rules above align with common Bandit checks included via Ruff’s S (flake8-bandit) rules[85].)
##### Part V: Typing and Static Type-Checking
    • All Functions Typed: Every function and method must have type annotations for all parameters and for the return type (except special cases like __init__ which implicitly returns None). The project enforces complete typing of function signatures[86]. No function should be left untyped in its definition.
    • Public Attributes Typed: All module-level variables, class attributes, and constants that are part of the public interface should be annotated with types, unless their value immediately makes the type obvious. In new code, aim to annotate any significant variable with an explicit type if the initializer expression is not clear or if it’s None initially.
    • No Any Types (Unless Explicitly Needed): Avoid the use of untyped Any types. All types should be as explicit and precise as possible. In strict mode, using Any (especially implicitly) can lead to type checker warnings. Thus, must not introduce Any types unless absolutely necessary (and if so, consider a comment or make it clear why).
    • Generics and Type Vars: When using generic classes or functions, provide type parameters. For example, do not use List or dict without type arguments – use List[int], dict[str, str], etc., so that types are explicit. If you define a TypeVar or ParamSpec, follow naming conventions (TypeVars meant to be covariant typically end in _co, etc.)[87]. Type variable names should reflect variance properly (e.g. T_co for covariant, T_contra for contravariant)[87].
    • Consistent Assignments: You must not assign a value of an incompatible type to a variable. Once a variable is established as a certain type, do not later assign a different type to it. If reusing a variable name, ensure all assignments are type-consistent or the variable is only used in a narrow scope.
    • Type Inference and Any: Do not rely on implicit Any types. If a variable cannot have its type inferred clearly by the checker (for instance, assigning from a dynamic expression), add an annotation. All variables should either have an obvious literal or constructor giving a clear type or have an explicit annotation.
    • No Unknown Attributes: Access only attributes that exist on an object’s type. Code must not use object.some_attr if some_attr is not known to exist according to the type. Pyright/Pyrefly will flag attribute accesses on unknown or Any types. Use hasattr checks or type guards (like isinstance) if needed, but avoid situations where an attribute is used without the type system’s knowledge of it.
    • Call Signature Matching: Ensure function calls match their definitions in both argument count and types. Do not call a function with missing required parameters, or with extra ones that aren’t accepted, or with types that do not match the declared parameter types. All function arguments must conform to the target function’s parameter type (and all return values must conform to the declared return type).
    • Iterable and Iterator Protocol: When a function expects an iterable of type X, do not pass a different type (e.g., don’t pass a list of int where a list of str is expected by the type). Respect container element types as well – e.g., if a function expects List[User], ensure you provide exactly that type.
    • Override Signatures: When overriding methods in subclasses, the signature (parameter types and return type) must be Liskov-substitution compatible with the base class. Do not change types in a way that violates substitutability. The type checker will treat incompatible overrides as errors. For example, you cannot narrow a parameter type in an override or change the return type to something incompatible.
    • No Misused Type[Any]: Don’t use the generic type incorrectly. If you need to annotate a variable that holds a class, use Type[BaseClass] for some known base.
    • Type Checking Imports Resolved: All imports needed for type annotations must be present. If you reference a class or type from another module in a type hint, ensure that import is included (either at runtime or inside if TYPE_CHECKING for heavy dependencies). The type checker should not encounter undefined names in type annotations.
    • Strict Optional Handling: Handle Optional types carefully. If a variable or expression can be None, you must check for None before using it in a way that requires a non-None type. Static analysis will enforce this (e.g., Pyright will error if you call a method on an Optional without a check). So, must not assume an optional is non-null without an explicit check or cast. Use assert var is not None or an if-check to narrow types when needed.
    • No type: ignore without Justification: Avoid using # type: ignore comments to silence type errors. If absolutely necessary, never use a blanket ignore with no code – always include a specific error code or a comment explaining why. Blanket ignores are flagged[88]. Ideally, fix the type issue rather than ignoring it.
    • No Unused Suppression Comments: Do not leave linter suppression comments that no longer apply. The configuration actively checks for unused suppression comments and will treat them as errors[89]. Every suppression or ignore comment must correspond to a real lint issue being suppressed, and should specify the code if possible (e.g., F401 for an unused import you intend to keep).
    • Pyright Strict Diagnostics: Under strict type checking, many optional checks are enabled. This means:
    • All variables should have defined types (no usage of variables before assignment, no dynamic attributes).
    • All code paths are checked: e.g., no falling off the end of a function that should return something[38], no unreachable code or unreachable conditions (the type checker may warn if you have contradictory conditions).
    • Conversions and casts should be explicit – if you intend to ignore a type issue, better to refactor or use a cast with typing.cast than to let implicit unsafe casts occur.
    • Any use of Any that flows through code could turn into errors. E.g., if you call an untyped function, the result is Any and using it might yield an error in strict mode. Therefore, prefer to add annotations to functions to avoid propagating Any.
    • Stubs and Types: If you maintain stub files (*.pyi in the stubs/ directory), ensure they follow stub rules:
    • In stubs, function bodies must contain only ... and no actual code or pass[90][91].
    • Do not include runtime logic or docstrings in stub files[92].
    • Keep stub signatures in sync with implementation. (These are mainly for contributors writing stubs; the AI should generate code, not stubs, so just be aware that if writing any type stub, it should obey stub conventions.)
    • Pylint Type Specific: Pylint rules in effect also forbid some typing misuse:
    • If a class defines __slots__, you cannot assign to attributes not in those slots[93].
    • Do not use nonlocal and global for the same variable[94] (a name can’t be both).
    • Don’t do tricky things like a global after using a name as local.
    • Special methods like __len__ must return the correct type (int for __len__, bool for __bool__, etc.)[95]. Similarly, __str__ must return str, __bytes__ bytes, __index__ int, etc.[95]. Ensure your implementations follow these contracts.
    • A bare raise outside of an exception handler context is not allowed[96]; it only makes sense inside except.
    • Do not reuse the same keyword argument twice in a function call (each parameter name should appear at most once)[97].
    • If you use await, it must be inside an async function[98]. No top-level await or inside sync code.
    • When using the logging format strings, ensure the number of %s placeholders matches the number of arguments (Pylint will error on too many or few args)[83].
    • Async/Await: All coroutine functions (marked with async def) must actually be awaited or used, not left un-awaited. If an AI writes async code, it must ensure not to ignore coroutine objects (document rare fire-and-forget behavior explicitly). Also, within an async function, use await for async calls; do not call asynchronous functions without awaiting them (flake8-async rules ensure proper await usage).
    • Self Use in Methods: Avoid using attributes named “self” outside of method definitions. Also, do not assign to self (the name) in a method – use it only as the instance reference. Flake8-self also flags instances of accessing private members of classes: do not access another class’s private variables (e.g., something that starts with _ClassName__), and do not use double prefix __var names in ways that rely on name-mangling inconsistently[99].
    • Typing Imports: Prefer modern typing syntax and imports. Use built-in generic types (like list[str] not typing.List[str]) on Python 3.13+ as appropriate (target version is py313[100]). Also prefer from collections.abc import Iterable instead of importing the old typing.Iterable if applicable. The code should be compatible with Python 3.13’s typing features.
(Pyrefly and Pyright will enforce that any type errors are eliminated. In summary: the code must type-check cleanly with no errors or unresolved types[101], and must conform to the strictest interpretation of the type hints provided.)
##### Part VI: Documentation and Comments
    • Docstrings Required: Every public module, class, function, and method must include a docstring explaining its purpose. This project treats missing docstrings as violations (via Pydocstyle’s rules under the NumPy convention). All API elements should be documented.
    • Module docstring at the top of each file (if the file has code aside from perhaps imports).
    • Class docstring for each class or interface explaining what it represents.
    • Function/method docstring explaining what it does, its parameters, return value, exceptions, and any additional notes.
    • Docstring Style: Docstrings must follow the NumPy docstring convention[102]. This means:
    • Begin with a one-line summary in imperative mood (describe what the function does, e.g. “Compute the average…” not “Computes” or “This function computes…”)[103]. The summary should start on the line right after the opening triple quotes and end with a period.
    • If more explanation is needed, after the one-line summary, include a longer description in the following paragraphs (separated by a blank line only if needed for clarity – in NumPy style, typically you do not insert an extra blank line between the summary and the next description or sections[104]).
    • Include sections for Parameters, Returns, Raises, Examples, etc., as appropriate. Use the exact section names and formatting expected by NumPy format (e.g., a “Parameters” section with a list of parameters, each followed by its type and description).
    • Every parameter in the function signature must be documented in the Parameters section, including the expected type and a brief description[105][106]. There must be no undocumented parameters and no spurious entries for parameters that don’t exist. The lint config enforces parameter documentation completeness[107][108].
    • If a function returns a value (and is not __init__), include a Returns section describing the return value and its type. If the function returns None (or doesn’t return anything meaningful), you can omit a Returns section or explicitly state it returns None.
    • Do not include a Returns section for a constructor (__init__) or any function that only raises an exception and never returns normally – having a Returns doc for those is incorrect (and in fact, an internal rule DOC202 is configured to avoid requiring a Returns section for functions that always raise)[109].
    • If the function or method can raise exceptions as part of normal operation (not just code errors), include a Raises section listing those exceptions and the conditions under which they occur.
    • For classes, you may include a Attributes section if the class has important public attributes to document.
    • For module-level docstring, describe the purpose of the module and maybe key points of usage.
    • Keep the docstring text wrapped at a reasonable width (typically 72 characters in documentation, although not strictly enforced by Ruff except for docstring code examples formatting). Use proper punctuation and grammar in docstrings.
    • Imperative Mood Summary: The first line of any docstring must be written in imperative mood (as a command)[110]. For example, “Generate a mapping from names to IDs.” instead of “Generates a mapping…” or “This function generates…”. The linter explicitly checks and enforces this style[110].
    • Docstring One-Liner vs Multi-line: If a docstring can fit on one line, you may use a single-line docstring (within triple quotes). If it’s multi-line, the summary line should be on the first line and the closing triple quote on its own line. In multi-line docstrings, do not insert a blank line between the summary and the following description text in NumPy style (this differs from some other conventions). Our configuration has disabled the rule that would require a blank line after the summary[16], so writing the summary followed immediately by detailed description is acceptable and expected in many cases.
    • Class Docstring Placement: A class’s docstring goes immediately after the class definition line, with no blank line in between[16]. (We ignore the PEP257 rule about needing a blank line before a class docstring to accommodate this.) So format as:
    • class MyClass:
    """One-line summary.

    Further details...
    """
    ... 
    • No blank line between class MyClass: and the """.
    • Internal Documentation: Use inline comments sparingly and only where necessary to explain non-obvious code logic. Inline comments must be preceded by two spaces and start with # and a space. Keep them short and relevant. Do not explain the obvious in comments – prefer clear code.
    • TODO/FIXME Comments: If using TODO comments, they must include an attribution (author or team name) and/or a tracking issue URL[111]. For example, use # TODO(john_doe): ... or # TODO: (ISSUE-123) .... A lone “# TODO:” with no context is not allowed[111]. The same applies to FIXME or other similar tags. Always provide enough context on who/what will address it.
    • No Obsolete Ignore Comments: Remove any  or # type: ignore comments that are no longer necessary. The linter will treat orphaned ignore comments (which don’t correspond to a currently active warning) as errors[89]. Every ignore must have a purpose.
    • No Trailing Debug Comments: Ensure no leftover debug comments or commented-out prints/logs. Commented-out code is forbidden as stated, but also avoid comments like “# debug” or “# remove later” – address them before finalizing the code.
    • Docstring Formatting for Examples/Code: When including example code in docstrings (in an Examples section), format it as doctest-style or appropriately indented code blocks. The Ruff formatter is configured to automatically format code in docstrings[112], so ensure example code is valid and properly indented, e.g. using four-space indents after the docstring text indent.
    • Spelling and Grammar: While not strictly a lint rule, docstrings and comments should be checked for clarity and typos. (We have spelling checks in docs build, so be mindful of common misspellings.)
    • Punctuation in Docstrings: The first line of a docstring should end with a period[113]. Likewise, ensure proper punctuation in multi-sentence descriptions. (Some Pydocstyle rules like D400 enforce a period at end of summary; our config likely includes that via the NumPy convention.)
    • Avoid Redundant Phrases: Don’t start docstrings with redundant phrases like “This function ...” or “Class to ...”. Just start with the action or description (imperative mood as noted).
    • Commented Code Blocks: As stated, commented-out code is disallowed (by the eradicate plugin)[71]. If you need to retain a snippet for reference, move it out or explain in prose rather than keeping actual code commented.
    • Documentation Coverage: The project may enforce docstring coverage separately, but as a rule, assume all public-facing code needs documentation. When generating code, always include meaningful docstrings for public functions/classes.

By following this contract, AI agents will ensure all generated Python code is fully compliant with the enforced linting and typing standards of the project’s Ruff, Pyrefly, and Pyright configurations. Each rule above is compulsory – code that violates any of these points will be rejected by the automated checks. Always write code that conforms to every single rule, ensuring clean, correct, and maintainable Python consistent with project guidelines.[114][101]

[1] [2] [4] [5] [6] [9] [11] [12] [16] [34] [35] [86] [100] [102] [104] [109] [114] pyproject.toml
file://file_00000000516471f5b988085aff6fd61d
[3] [7] [8] [10] [13] [14] [15] [17] [18] [19] [20] [21] [22] [23] [24] [25] [26] [27] [28] [29] [30] [31] [32] [33] [36] [37] [38] [39] [40] [41] [42] [43] [44] [45] [46] [47] [48] [49] [50] [51] [52] [53] [54] [55] [56] [57] [58] [59] [60] [61] [62] [63] [64] [65] [66] [67] [68] [69] [70] [71] [72] [73] [74] [75] [76] [77] [78] [79] [80] [81] [82] [83] [84] [85] [87] [88] [90] [91] [92] [93] [94] [95] [96] [97] [98] [99] [111] Rules | Ruff
https://docs.astral.sh/ruff/rules/
[89] The Ruff Linter - Astral Docs
https://docs.astral.sh/ruff/linter/
[101] Configuration | Pyrefly
https://pyrefly.org/en/docs/configuration/
[103] Why is imperative mood important for docstrings? - Stack Overflow
https://stackoverflow.com/questions/59902102/why-is-imperative-mood-important-for-docstrings
[105] Error Codes — pydocstyle 0.0.0.dev0 documentation
https://www.pydocstyle.org/en/stable/error_codes.html
[106] [pydocstyle]: d417 being ignored with convention=google #16477
https://github.com/astral-sh/ruff/issues/16477
[107] Error Codes — pydocstyle 1.0.0 documentation
http://www.pydocstyle.org/en/5.0.1/error_codes.html
[108] undocumented-param (D417) | Ruff - Astral Docs
https://docs.astral.sh/ruff/rules/undocumented-param/
[110] non-imperative-mood (D401) | Ruff - Astral Docs
https://docs.astral.sh/ruff/rules/non-imperative-mood/
[112] The Ruff Formatter - Astral Docs
https://docs.astral.sh/ruff/formatter/
[113] Error Codes — pydocstyle 1.0.0 documentation
http://www.pydocstyle.org/en/2.1.1/error_codes.html



Besides those formatting rules, all of your code designs and implementation work should be structural, holistic, and best-in-class, guided by the following principles:


#### Principles for your code design and implementation

##### 1) Clarity & API design

* Public API is explicit and named per PEP 8; internal helpers remain private.
* Every public module/class/function has a PEP 257 docstring whose first line is a one-sentence summary.
* Fully annotated signatures; prefer modern generics/type params (PEP 484 + PEP 695). **No untyped public APIs.**
* Exceptions are part of the contract. Define a small **exception taxonomy**, and for HTTP use **RFC 9457 Problem Details**; always `raise ... from e` to preserve cause chains.
* **Verify:** `ruff format && ruff check --fix` is clean; docstrings present; types on all public symbols; include a sample Problem Details JSON for at least one error path.

##### 2) Data contracts & schemas

* Cross-boundary data MUST have a schema: **JSON Schema 2020-12** for payloads; **OpenAPI 3.2** for HTTP.
* Code models may **emit** or **round-trip** the schema, but the schema file is the **source of truth**.
* Include examples and versioning notes (backward-/forward-compat, deprecations).
* **Verify:** schema validates against 2020-12 meta-schema; OpenAPI passes a linter; round-trip tests confirm model↔schema parity.

##### 3) Testing strategy

* Pytest; **table-driven** tests with `@pytest.mark.parametrize` covering happy path, edges, and failure modes.
* Integrate **doctest/xdoctest** so examples in docs truly run.
* Map tests to scenarios in the spec (when applicable).
* **Verify:** `pytest -q` green; param/edge/error cases present; doctests pass.

##### 4) Type safety

* Project is **type-clean** under **pyright** (strict mode), **pyrefly** (sharp checks).
* Prefer `Protocol`/`TypedDict`/PEP 695 generics over `Any`; minimize `cast` and justify any `# type: ignore[...]`.
* **Verify:** both checkers pass; no unexplained ignores.

##### 5) Logging & errors

* Use stdlib `logging`; libraries define a `NullHandler`; apps configure handlers.
* Prefer **structured logs** (extra fields); never log secrets/PII.
* For HTTP, return **Problem Details** consistently and log with correlation IDs (see Observability below).
* **Verify:** module loggers exist; no `print` in libraries; errors produce logs + exceptions/Problem Details.

##### 6) Configuration & 12-factor basics

* Config via **environment variables** (use `pydantic_settings` or equivalent for typed settings); no hard-coded secrets.
* Backing services are replaceable; logs to stdout/stderr.
* **Verify:** swapping URLs/credentials is config-only; startup fails fast if required env is missing.

##### 7) Modularity & structure

* Favor **single-responsibility** modules; separate pure logic from I/O; explicit dependency injection; **no global state**.
* Layering: **domain** (pure) → **adapters/ports** → **I/O/CLI/HTTP**; consider import-linter rules to prevent cross-layer leaks.
* **Verify:** imports are acyclic; small functions; side-effect boundaries explicit.

##### 8) Concurrency & context correctness

* For async, use **`contextvars`** for request/task context; document timeouts and cancellation.
* Avoid blocking calls in async code; use thread pools for legacy I/O.
* **Verify:** async APIs document await/timeout rules; context propagated via `ContextVar`.

##### 9) Observability (logs • metrics • traces)

* Emit **structured logs**, **Prometheus metrics**, and **OpenTelemetry traces** at boundaries.
* Minimum: request/operation name, duration, status, error type, correlation/trace ID.
* Semantic Pro telemetry now emits `rerank.*` spans and reports `method.rerank` metadata so XTR reordering decisions are debuggable.
* **Verify:** a failing path produces (1) an error log with context, (2) a counter increment, and (3) a trace span with error status.

##### 10) Security & supply-chain

* **Never** use `eval/exec` or untrusted `pickle`/`yaml.load` (use `safe_load`).
* Validate/sanitize all untrusted inputs; prevent path traversal with `pathlib` and whitelists.
* Run a vuln scan (e.g., `pip-audit`) on dependency changes; pin ranges sensibly.
* **Verify:** `pip-audit` clean; inputs validated in tests.

##### 11) Packaging & distribution

* `pyproject.toml` with **PEP 621** metadata; **PEP 440** versioning; build wheels.
* Keep dependencies minimal; use **extras** for optional features; add environment markers for platform-specific bits.
* **Verify:** `pip wheel .` succeeds; `pip install .` works in a clean venv; metadata is correct.

##### 12) Performance & scalability

* Set simple budgets where relevant (e.g., p95 latency, memory ceiling) and write micro-bench tests for hot paths.
* Avoid quadratic behavior; stream large I/O; prefer `pathlib`, `itertools`, vectorized ops where apt.
* **Verify:** a representative input meets the budget locally; add notes if a budget is intentionally exceeded.

##### 13) Documentation & discoverability

* Examples are **copy-ready** and runnable; public API shows minimal, idiomatic usage.
* Cross-link code to spec and schema files; keep the **Agent Portal** links working (editor/GitHub).


##### 14) Versioning & deprecation policy

* Use **SemVer language** for public API; mark deprecations with warnings and removal version; update CHANGELOG.
* **Verify:** deprecated calls warn once with a clear migration path.

##### 15) Idempotency & error-retries

* Any externally triggered operation (HTTP/CLI/queue) should be **idempotent** where possible; document retry semantics.
* **Verify:** repeated calls with same input either no-op or converge; tests prove it.

##### 16) File, time, and number hygiene

* **`pathlib`** for paths; **timezone-aware** datetimes; use **`time.monotonic()`** for durations; **`decimal.Decimal`** for money.
* **Verify:** no `os.path` in new code; no naive datetimes in boundaries.


> If any step fails, **stop and fix** before continuing.

## Testing Charter (for AI + Human Contributors)

> **Intent:** All tests should behave as close to production as possible, even when targeting very small units of behavior. This charter defines hard rules and strong defaults for how to design and implement tests in this repo.

---

### 1. North Star & Scope

* **North Star:**
  Tests validate reality, not a lab simulation. Anything that passes our test suite should be extremely likely to work in production under realistic conditions.

* **Scope:**
  Applies to **all** pytest-based tests in this repo:

  * Unit-ish tests
  * Integration / “thin slice” tests
  * End-to-end tests
  * CLI, services, background jobs, data pipelines, code-intel components, etc.

When in doubt, **choose the option that is closer to how the system runs in production.**

---

### 2. Global Rules

1. **No monkeypatching at all.**

   * You **MUST NOT** use `monkeypatch` fixture.
   * You **MUST NOT** use `unittest.mock.patch`, `pytest-mock`, or any runtime-patching utilities on production code.
   * All behavioral variation must be achieved via:

     * configuration,
     * dependency injection, or
     * explicitly selectable implementations at composition time.

2. **No test-only code paths.**

   * Production code **MUST NOT** contain `if TESTING: ...` or branches that check `PYTEST_` env vars or “is pytest running?”.
   * Configuration may differ (e.g. file paths, URLs, DB names), but logic may not.

3. **Same stack, different instances.**

   * Tests **MUST** use the same technologies as production:

     * Same DB engine (e.g. DuckDB), same indexer (e.g. FAISS), same queues, same HTTP stack, etc.
   * Tests may use **isolated instances** (temporary directories, in-memory DBs, local containers) but never replace a technology with a fake one (e.g. replacing DuckDB with a list).

---

### 3. Entry Points & Boundaries

**Goal:** Exercise the system through the same seams that real users and systems use.

* **Public entry points only**

  * Tests **SHOULD** invoke:

    * CLI commands via the real CLI app or `python -m` style entry points.
    * Web/API operations via the real app factory / ASGI app.
    * Library behavior via public functions/classes, not private helpers.
  * Avoid reaching into internal helpers that production never calls directly.

* **Compat shims**

  * Where we maintain legacy shims (e.g. old CLI entry functions), tests **MAY** target them if they are real, deployed entry points.
  * Test the **shim behavior + delegation**, not an alternative “test-only” path.

---

### 4. Imports & Dependency Wiring

**Goal:** The dependency graph under test matches the production graph.

* Tests **MUST** import modules using the same patterns as production (no special test-only import tricks).
* Tests **MUST NOT** rely on manipulating `PYTHONPATH` or sys.path in ways not used in production.
* If a dependency injection container or “composition root” exists:

  * Tests **SHOULD** construct the app via that same composition root.
  * Tests may override **configuration**, not the types bound to interfaces (except where explicitly supported by the production composition design).

---

### 5. Configuration & Environment

**Goal:** Tests load configuration the same way production does.

* Config **MUST** be loaded through the real config mechanism:

  * e.g. `$ENV` + config files + CLI flags, etc.
* Tests may:

  * use a dedicated test config file (e.g. `.env.test`, `config.test.yaml`),
  * or set environment variables that are **documented and meaningful** for real deployments.
* Env vars:

  * You **MAY** introduce test-specific values (e.g. `APP_ENV=test`, `TEST_DATA_DIR=...`),
  * But they **MUST** map to behavior that could realistically occur in a non-test environment (e.g. “pointing to a different directory or DB”), not fundamentally different logic.

---

### 6. Data & Fixtures

**Goal:** Test data looks and behaves like real data.

* **Structural realism**

  * Fixtures **SHOULD** use realistic shapes and values:

    * Nested structures, multi-field records, non-ASCII text, reasonably long strings, realistic numerics, optional fields, etc.
  * Avoid trivial “toy” fixtures like `{"id": 1, "name": "test"}` as the only coverage.

* **Golden files**

  * Golden reference files (JSON, Parquet, CSV, Markdown, HTML, etc.) **SHOULD** mirror real artifacts:

    * Production column names, realistic row counts (not just a single row), plausible content and edge cases (nulls, weird spacing, etc.).
  * Size may be small for performance, but complexity should reflect reality.

---

### 7. Dependencies & External Systems

**Goal:** Same technology, smaller blast radius.

* For each external system that production uses:

  * Tests **MUST** use the **same kind of system**:

    * DuckDB for DuckDB, FAISS for FAISS, Neo4j for Neo4j, etc.
  * Tests **MAY** use:

    * Temporary schemas / databases,
    * Local containers / in-process mocks that are **drop-in compatible** and used in dev/staging,
    * In-memory variants provided by the same engine (if realistic for production footprints).
* No replacing a complex system with a toy in-memory structure that bypasses serialization, indexing, error handling, or the real API.

---

### 8. Time, Randomness & IDs

**Goal:** Deterministic tests without cheating via monkeypatch.

* Nondeterministic behavior **MUST** go through explicit abstractions:

  * e.g. `Clock`, `NowProvider`, `RandomSource`, `IdGenerator`.
* Production implementations call the real `time`/`datetime`/`random`/`uuid` APIs.
* Test implementations:

  * **SHOULD** be deterministic but realistic (monotonic time, realistic UUID shapes),
  * **MUST NOT** be injected via monkeypatching;
  * they are selected through configuration or DI.

---

### 9. Concurrency & Processes

**Goal:** If production is concurrent, tests exercise that concurrency model.

* Async code:

  * Tests for async components **MUST** run them under the real event-loop / async stack used in production (e.g. `anyio`, `asyncio`) and not wrap them in artificial sync helpers that production never uses.
* Multiprocessing / threading:

  * Where production uses process pools or multiprocessing, tests **SHOULD** include at least some scenarios that use those same mechanisms.
* Parallel test execution:

  * All tests **MUST** be safe under parallel pytest execution (e.g. `pytest -n auto`):

    * No shared fixed filenames,
    * No shared mutable global state,
    * Use unique temp directories or per-test resources.

---

### 10. Observability & Error Behavior

**Goal:** Test not only outcomes, but also the signals we rely on in production.

* Logging:

  * For critical flows, tests **SHOULD** assert on log events and structure (log level, message, and key fields).
* Metrics:

  * When important behavior is tracked by metrics, tests **SHOULD** verify that the right counters/gauges/histograms are updated.
* Errors:

  * Prefer simulating real failure modes:

    * e.g. corrupting a file, using a missing index path, closing a connection, making a dependency return a realistic error.
  * Avoid artificial “magic” failures introduced via monkeypatch.

---

### 11. Use of Fakes & Test Doubles

**Goal:** When you must use a stand-in, it behaves like a real component.

* Test doubles **MUST**:

  * Implement the same interface/protocol as the real dependency.
  * Preserve key invariants (serialization, validation, error semantics).
  * Be something you could plausibly use in a dev/staging environment (e.g. Minio for S3, local SMTP debug server, file-based cache).

* Test doubles **MUST NOT**:

  * Skip core behaviors (e.g. skip indexing, skip parsing, skip network boundaries) in ways that make bugs untestable.
  * Be wired through runtime patching; they should be selected via configuration/DI.

---

### 12. Coverage & Test Types

**Goal:** Every critical path has at least one realistic “thin slice” test.

* For each critical user or system path:

  * There **MUST** be at least one test that:

    * Enters through the real entry point (CLI command, HTTP endpoint, worker main, etc.),
    * Uses the real configuration mechanism,
    * Hits real storage/indexing/cache layers (with isolated instances),
    * Exercises typical and edge-case data.

* Unit-level tests:

  * Are allowed and encouraged for complex logic,
  * **BUT** should target functions/classes that are **actually used** in production,
  * And ideally are paired with a higher-level slice test for the same path, so they don’t drift from reality.

---

### 13. Anti-Patterns (Hard “NO”s)

Agents and humans **MUST NOT**:

1. Use `monkeypatch`, `unittest.mock`, or any runtime patching/mocking of production modules.
2. Introduce `if TESTING` or equivalent branches in production code.
3. Create alternate “test-only” code paths, containers, or composition roots that are not valid in real deployments.
4. Rely on toy in-memory stand-ins for critical external systems (DBs, indexers, queues, etc.).
5. Write tests that only validate internal helpers that are not used by any real code path.

---

### 14. Quick Checklist for New Tests (Agent-Friendly)

Before finalizing a test, verify:

1. **Entry point**

   * Am I entering through the same public API/CLI/endpoint that production uses?

2. **Config & env**

   * Am I using the real configuration loading mechanism?
   * Are any env vars I set consistent with plausible real deployments?

3. **Dependencies**

   * Am I using the same tech stack (DuckDB, FAISS, etc.) as production, just with isolated instances?

4. **No patching**

   * Did I avoid `monkeypatch`, `unittest.mock`, and other runtime patching tools?

5. **Data realism**

   * Does my test data look like real data, including some complexity and edge cases?

6. **Nondeterminism**

   * If time/randomness/UUIDs matter, am I using injected abstractions rather than patching built-ins?

7. **Concurrency & isolation**

   * Will this test still pass when tests run in parallel?

8. **Observability (if applicable)**

   * For critical flows, am I asserting on logs/metrics/errors in the way we depend on in production?

If any answer is “no,” revise the test to move it closer to production reality.

---



---

## Environment Setup (Agent‑grade, deterministic)

- **Canonical manager:** `uv`
- **Python:** pinned to **3.13.11**
- **Virtual env:** project‑local `.venv/` only (never system Python)
- **One-shot bootstrap (REQUIRED):** use `scripts/bootstrap.sh`
  ```bash
  scripts/bootstrap.sh
  ```
  The script lives at `scripts/bootstrap.sh` and provisions uv, pins Python 3.13.11, syncs dependencies, activates the project `.venv/`, and sets project paths correctly. It is REQUIRED, the code will not function correctly if you do not run this script. Pass `--help` for options.

  If you are having difficulties with reaching the directory even after running `scripts/bootstrap.sh` please attempt to run the bash command "/bin/bash -lc 'pwd && ls -la'"

- **Remote container / devcontainer:** follow [Link Policy for Remote Editors](#link-policy-for-remote-editors) so deep links open correctly from generated artifacts.
- **Do not** duplicate tool configs across files. `pyrightconfig.jsonc` and `pyrefly.toml` are canonical; `pyproject.toml` is canonical for Ruff and packaging.
- **Admin surfaces:** set `CODEINTEL_ADMIN=1` locally to enable `/admin/index` endpoints for staging/publishing lifecycle changes. Leave unset in most environments.


---

## Source‑of‑Truth Index

Read these first when editing configs or debugging local vs CI drift:

- **Formatting & lint:** `pyproject.toml` → `[tool.ruff]`, `[tool.ruff.lint]`
- **Types:** `pyrefly.toml` (single source), `pyrightconfig.jsonc` (strict pyright)
- **Tests:** `pytest.ini` (markers, doctest/xdoctest config)


---

## Code Formatting & Style (Ruff is canonical)

- **Run order:** `uv run ruff format` → `uv run ruff check --fix` (imports auto‑sorted).
- **Imports:** stdlib → third‑party → first‑party; absolute imports only.
- **Style guardrails:** 100‑col width, 4‑space indent, double quotes, trailing commas on multiline.
- **Complexity:** cyclomatic ≤ 10, returns ≤ 6, branches ≤ 12; refactor if exceeded.
- **Rule families emphasized (non‑exhaustive):**
  - Baseline: `F,E4,E7,E9,I,N,UP,SIM,B,RUF,ANN,D,RET,RSE,TRY,EM,G,LOG,ISC,TID,TD,ERA,PGH,C90,PLR`
  - Extra quality & safety: `W,A,ARG,BLE,DTZ,PTH,PIE,S,PT,T20`
    (builtins shadowing, unused args, bare except, timezone‑aware datetimes, pathlib, security, pytest style, ban prints)

> We standardize on **Ruff** for formatter + linter. Do **not** run Black in parallel (conflicts / duplicate work).

---

## Type Checking (pyright strict, pyrefly sharp)

- **Static analysis (strict mode):**
  ```bash
  uv run pyright --warnings --pythonversion=3.13.11
  ```
- **First-line check (semantics):**
  ```bash
  uv run pyrefly check
  ```
- **Rules of engagement:**
  - Pyright runs in strict mode (`pyrightconfig.jsonc`); update execution environments when adding new roots.
  - No untyped **public** APIs.
  - Prefer **PEP 695** generics and `Protocol`/`TypedDict` over `Any`.
  - Every `# type: ignore[...]` requires a comment **why** + a ticket reference.
  - Narrow exceptions; HTTP surfaces return **RFC 9457 Problem Details**.

**“Type-clean” means pyright and pyrefly both pass.**

---

## Typing Gates (Postponed Annotations & TYPE_CHECKING Hygiene)

**Purpose**: Prevent runtime imports of heavy optional dependencies (numpy, fastapi, FAISS) when they're only used in type hints. This ensures tooling stays lightweight and import-clean.

> Heavy dependency metadata (module names, min versions, install extras) lives in `src/CodeIntel_common/typing/heavy_deps.py`. Update that file whenever a new gated dependency is introduced.

### 1. Postponed Annotations (PEP 563)

Every Python module MUST include:
```python
from __future__ import annotations
```

This directive must be **the first import statement** (after shebang and encoding declaration). Use the automated fixer:
```bash
python -m tools.lint.apply_postponed_annotations src/ tools/ docs/_scripts/
```

**Why**: Postponed annotations eliminate eager type hint evaluation, preventing `NameError` when optional dependencies are missing.

### 2. Typing Façade Modules

Use canonical typing imports instead of direct imports from heavy libraries:

**Canonical façades** (re-export safe type-only helpers):
- `CodeIntel_common.typing` — Core type aliases and runtime helpers
- `tools.typing` — Tooling scripts (re-export from CodeIntel_common.typing)
- `docs.typing` — Documentation scripts (re-export from CodeIntel_common.typing)

**Type-only imports MUST be guarded**:
```python
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np
    from fastapi import FastAPI

def process(vectors: np.ndarray, app: FastAPI | None = None) -> None:
    """Annotations use types safely; runtime code doesn't import them."""
    pass
```

**Runtime access to heavy types** (when genuinely required):
```python
from CodeIntel_common.typing import gate_import

# Inside a function that actually needs numpy at runtime:
np = gate_import("numpy", "array reshaping in process()")
result = np.reshape(data, (10, 10))
```

### 3. Ruff Rules (Automatic Enforcement)

**Enabled rules** (errors by default):
- `TC001–TC006` — Type-checking import violations (move to TYPE_CHECKING blocks)
- `PLC2701` — Type-only import used at runtime (special: allowed in façade modules only)
- `INP001` — Implicit namespace packages (require `__init__.py` for packages)
- `EXE002` — Missing shebang for executable files

Per-file ignores are defined in `pyproject.toml` for:
- Façade modules (`src/CodeIntel_common/typing`, `tools/typing`, `docs/typing`)
- Special internal packages (`docs/_types`, `docs/_scripts`)

### 5. Development Workflow

**When adding a new module that uses type hints**:
1. Add `from __future__ import annotations` at the top
2. Move heavy type imports into `if TYPE_CHECKING:` blocks
3. For runtime needs, use `gate_import()` from the façade
4. Run checks before committing:
   ```bash
   uv run pyrefly check
   uv run ruff check --fix  # Enforces TC/INP/EXE rules
   ```

**Deprecation path**: Old code using `docs._types` or private imports will emit `PLC2701` warnings and will be removed after Phase 1 migration (see openspec/changes/typing-gates-holistic-phase1/).

### Using heavy types safely (recipe)

1. Always add `from __future__ import annotations` and guard heavy imports with `if TYPE_CHECKING:`.
2. Reach for facade aliases from `CodeIntel.typing` (`NDArrayF32`, `NDArrayI64`, `gate_import`, `HEAVY_DEPS`) instead of raw types.
3. For runtime access, wrap modules with `LazyModule` so that imports happen only when a function actually executes.
4. Keep signatures and docstrings aligned with the façade aliases so lint/type gates remain clean.

**Before (violates typing gates)**:

```python
import numpy as np

def search(query: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    ...
```

**After (import-clean + typed)**:

```python
from typing import TYPE_CHECKING, cast
from CodeIntel._lazy_imports import LazyModule
from CodeIntel.typing import NDArrayF32

if TYPE_CHECKING:
    import numpy as np
else:
    np = cast("np", LazyModule("numpy", "faiss manager vector ops"))

def search(query: NDArrayF32) -> tuple[NDArrayF32, NDArrayF32]:
    scores = np.asarray(...)
    return scores, query
```

This pattern keeps `python -c "import CodeIntel"` working on minimal hosts while preserving precise types for Ruff/pyright/pyrefly.

---

## Docstrings (NumPy style; enforced; runnable)

- **Style:** NumPy docstrings; PEP 257 structure (module/class/function docstrings for all public symbols)
- **Enforcement:** `pydoclint` parity checks + `docstr-coverage` (≥90%)
- **Runnability:** Examples in `Examples` must execute (doctest/xdoctest); keep snippets short and copy‑ready
- **Required sections (public APIs):**
  - Summary (one line, imperative)
  - Parameters (name, type, meaning)
  - Returns / None
  - Raises (type + condition)
  - Examples
  - Notes (performance, side‑effects)


---

## Testing Standards (markers & coverage)

- **Markers:**
- `@pytest.mark.integration` — network/services/resources
- `@pytest.mark.benchmark` — performance, non-gating
- **Conventions:**
  - Parametrize edge cases with `@pytest.mark.parametrize`
  - No reliance on test order or realtime; use fixed seeds
- **Helper fixtures:** prefer the utilities in `tests/_helpers/` (e.g.,
  contexts) and the typed gate overrides (`CodeIntel_common.typing.override_gate_import`,
  `faiss_runtime.override_parameter_application`) instead of ad-hoc monkeypatching.
- Entire suite runs on CPU; GPU markers have been removed
- **Coverage (local whenever core paths change):**
  ```bash
  uv run pytest -q --cov=src --cov-report=xml:coverage.xml --cov-report=html:htmlcov
  ```

### Hexagonal Test Architecture

The test suite uses a hexagonal architecture for test setup. See `tests/_helpers/MIGRATION.md` for detailed migration recipes.

**Core Components:**
- `TestContext` (`tests/_helpers/context.py`) — Unified test environment with lazy resource access
- **Seed Packs** (`tests/_helpers/seeds/`) — Composable data seeding modules:
  - `CORE_PACK` — modules, goids, repo_map
  - `GRAPH_PACK` — call graph, import graph, CFG/DFG
  - `COVERAGE_PACK` — test catalog, coverage edges
  - `METRICS_PACK` — function metrics, risk factors
  - `DOCSTRING_PACK`, `SUBSYSTEM_PACK`, `SYMBOL_PACK`, `CONFIG_PACK`
- `TestScenario` (`tests/_helpers/scenarios.py`) — Fluent builder for complex test setups
- **Env Adapters** (`tests/_helpers/env_adapters.py`) — Backward-compatible adapters for legacy env classes

**Preferred Fixtures:**
```python
# Pre-seeded contexts (recommended for new tests)
def test_graph_feature(graph_ctx: TestContext) -> None:
    # GRAPH_PACK seeds already applied
    result = analyze(graph_ctx.gateway)

# Manual seeding when needed
def test_custom_scenario(test_ctx: TestContext) -> None:
    test_ctx.require(CORE_PACK, COVERAGE_PACK)
    # Custom setup on top of seeds
```

**Migration Priority:** New tests should use the hexagonal architecture. Existing tests can be migrated incrementally as they are touched.

---

## Data Contracts (JSON Schema 2020‑12 / OpenAPI 3.2)

- **Boundary rule:** whenever data crosses a boundary (API, file, queue), define a **JSON Schema 2020‑12**. For HTTP, use **OpenAPI 3.2** (embeds 2020‑12).
- **Source of truth:** the schema is canonical; models may be generated from it (or emit it) but do not replace it.
- **Validation:** validate inputs/outputs in tests; version schemas with SemVer and document breaking changes.
- **Data models:** consume the normalized tables/views (`analytics.data_model_fields`, `analytics.data_model_relationships`, `docs.v_data_models_normalized`) via `codeintel.storage.data_models`; do **not** parse `fields_json`/`relationships_json`. Legacy JSON fallbacks have been removed; always read from the normalized views.

---

## Link Policy for Remote Editors

- **Editor mode (preferred for local dev):**
  - `DOCS_LINK_MODE=editor`
  - `EDITOR_URI_TEMPLATE="vscode-remote://dev-container+{container_id}{path}:{line}"`
  - Optional `PATH_MAP` file, lines: `/container/prefix => /editor/workspace/prefix`
- **GitHub mode (fallback):**
  - `DOCS_LINK_MODE=github`
  - `DOCS_GITHUB_ORG`, `DOCS_GITHUB_REPO`, `DOCS_GITHUB_SHA`
  - Links like: `https://github.com/{org}/{repo}/blob/{sha}/{path}#L{line}`
- **Agent rule:** use **editor** mode in remote containers for deep linking; use **github** when editor URIs are unavailable.

Example `PATH_MAP`:
```
/workspace => /workspaces/CodeIntel
/app       => /workspaces/CodeIntel
```

---

## Quick Commands (copy/paste)

```bash
# Format & lint
uv run ruff format && uv run ruff check --fix

# Types (pyright strict + pyrefly sharp)
uv run pyright --warnings --pythonversion=3.13.11
uv run pyrefly check

# Tests (incl. doctests/xdoctest via pytest.ini)
uv run pytest -q


# All pre-commit hooks
uvx pre-commit run --all-files

# Index lifecycle helpers
codeintel indexctl status --root indexes
codeintel indexctl stage --version v1 --faiss path/to/faiss --duckdb path/to/catalog.duckdb --scip path/to/index.scip
codeintel indexctl publish --version v1
```

## Advanced Query Engine - Usage Guide

This guide explains how to run the advanced query engine CLI, what each query returns,
how to control scope and budgets, and how to validate outputs. A copy lives at
`tools/advanced_query_engine/README.md`.

### What it is

The advanced query engine runs a small set of search handlers that map to eight search
intents:

1) symbol.resolve
2) refs.find
3) callgraph.slice
4) pattern.scan
5) contract.lookup
6) wiring.map
7) precedent.search
8) impact.slice

The CLI prints JSON to stdout. You can view it directly in the terminal or pipe it to
`jq` or a file.

### Pack roots

- Query packs (rpygrep, ast-grep, tree-sitter):
  `docs/advanced_query_engine/query_packs`
- Wiring packs (framework wiring maps):
  `docs/advanced_query_engine/wiring_packs/packs`

### CLI quickstart

```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . \
  --type refs.find \
  --text compose_runtime
```

The JSON is printed to stdout. To save:

```bash
uv run python -m tools.advanced_query_engine.cli --repo . --type refs.find --text X \
  > /tmp/aqe.json
```

### Common request fields

All query types accept:

- `--repo` repository root.
- `--type` one of the eight query types.
- `--text` query text (symbol name, pattern, or target).
- `--scope` repeatable repo-relative paths to constrain scanning.
- `--budget-json` JSON string matching `QueryBudget`:
  - `max_files` max candidate files (0 means no cap).
  - `max_matches` max matches (0 means no cap).
  - `max_depth` max directory depth (0 means unlimited).
  - `max_seconds` optional time budget (seconds).
  - `context_lines` context lines for snippets.
- `--options-json` or `--options-file` for query-specific options.

Default scope is the repo root. For performance, prefer `--scope src --scope tests`.

### Output shape

Every response includes:

- `summary`: human-readable summary.
- `primary`: main results list.
- `related`: auxiliary groups or secondary outputs.
- `debug`: engine metadata and partial/budget hints.

If any backend hits limits, `debug` includes flags such as `rg_partial`,
`ast_partial`, `ts_partial`, or `budget_exhausted`.

### Query-specific guidance

#### 1) symbol.resolve

Finds definition records for a symbol name.

Example:
```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . --type symbol.resolve --text SearchService
```

Output highlights:
- `primary`: list of symbol records with `symbol_id`, `def_span`, `signature`, `docstring`.

#### 2) refs.find

Enumerates usage sites and classifies them as call/import/read/write/inherit.

Example:
```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . --type refs.find --text compose_runtime --scope src
```

Output highlights:
- `primary`: records with `role`, `confidence`, `rank`, `snippet`, `enclosing`.
- `related.groups`: counts by module and by kind (prod/test/doc/example).
- `related.overrides`: method overrides with signature/docstring.

#### 3) callgraph.slice

Returns incoming and outgoing call edges with callsite spans and arg maps.

Example:
```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . --type callgraph.slice --text compose_runtime --scope src
```

Output highlights:
- `related.calls_in` and `related.calls_out`: each edge includes `call_span`,
  `callsite` snippet, `arguments`, and `arg_map` (when signature is available).

#### 4) pattern.scan

Runs a lexical or structured scan. Options choose backends.

Common options:
- `pattern_group_id`: rpygrep group id.
- `ast_grep_pack_id`, `rule_ids`: ast-grep pack + rule ids.
- `tree_sitter_pack_id`: tree-sitter pack id.

Example (rpygrep):
```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . --type pattern.scan --text "TODO" \
  --options-json '{"pattern_group_id":"rg.precedent.search"}'
```

Example (ast-grep):
```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . --type pattern.scan --text "" \
  --options-json '{"ast_grep_pack_id":"ag.python.policy_security", "rule_ids":["py.security.eval"]}'
```

#### 5) contract.lookup

Finds tests/docs/examples that reference a symbol, and extracts test assertions.

Example:
```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . --type contract.lookup --text compose_runtime --scope tests --scope docs
```

Output highlights:
- `primary`: test records with `assertions`, `references`, and a test snippet.
- `related.docs` and `related.examples`: documentation/example hits.

#### 6) wiring.map

Runs wiring packs (FastAPI, Flask, Click, Typer, argparse, env, entrypoints).

Options:
- `pack_ids`: list of wiring pack ids to run.
- `allow_cross_file_resolution`: resolve handler symbols across files.

Example:
```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . --type wiring.map --text "" \
  --options-json '{"pack_ids":["wire.python.fastapi.routes"]}'
```

Output highlights:
- `primary`: wiring edges with `entry_key`, `hook_span`, `target`, and `config`.
- `related.by_pack`: per-pack results and validation findings.

#### 7) precedent.search

Finds similar definitions based on signature/decorators/docstring overlap.

Example:
```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . --type precedent.search --text "compose_runtime" --scope src
```

Options:
- `k`: top-K results (default 5).
- `pattern_group_id`: optional rpygrep pattern group for candidate pool.

Output highlights:
- `primary`: scored exemplars with `why` explanations.

#### 8) impact.slice

Builds a bounded transitive slice of callers/callees plus boundary crossings.

Options:
- `slice`: object with `caller_depth`, `callee_depth`.
- `package_depth`: depth for boundary crossings.

Example:
```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . --type impact.slice --text compose_runtime \
  --options-json '{"slice":{"caller_depth":2, "callee_depth":1}, "package_depth":2}'
```

Output highlights:
- `primary`: node list in the slice.
- `related.edges`: transitive call edges.
- `related.calls_in/out`: 1-hop edges from callgraph.slice.
- `related.boundary_crossings` and `related.risk`.

### Persistence and analytics

The CLI can persist results and optionally run analytics:

```bash
uv run python -m tools.advanced_query_engine.cli \
  --repo . --type pattern.scan --text TODO \
  --persist --analytics --validate-persisted
```

Relevant flags:
- `--persist`, `--persist-path`, `--persist-partition-by`
- `--analytics`, `--analytics-profile`, `--analytics-chunk-size`, `--analytics-max-rows`
- `--validate-persisted`

### Validation harness

Run independent checks across the eight query types:

```bash
uv run python -m tools.advanced_query_engine.validation_harness \
  --repo-root . --scope src --scope tests
```

The default report is:
`build/advanced_query_engine/validation_report.json`

You can override cases with `--case` or provide a JSON config via `--config`.

### Troubleshooting

- If a query is slow, add `--scope` (prefer `src` and `tests`) and reduce `max_files`.
- If you see `rg_partial` or `budget_exhausted`, raise budgets or tighten scope.
- If wiring packs emit validation errors, fix the pack captures or templates.

**Problem Details Reference:**
- Example: `schema/examples/problem_details/search-missing-index.json`
- Schema: RFC 9457 Problem Details format
- Implementation: `src/CodeIntel_common/errors/`

---

## Troubleshooting (for Agents)

- **Ruff vs Black conflicts**: We use **Ruff only**; remove Black changes, re-run Ruff.
- **Third‑party typing gaps**: prefer small typed facades (`Protocol`, `TypedDict`) or `stubs/` alongside the stub configuration in `pyrightconfig.jsonc`, not `Any`.
- **Editor links open wrong path**: verify `PATH_MAP` and `EDITOR_URI_TEMPLATE` configuration.
- **Slow CI**: check cache restore logs for `uv`, `ruff`, `pyright`. If keys miss, verify `uv.lock` & Python version detection step.

---

## Security & Compliance Hygiene

- **Secrets**: never commit `.env` or tokens; redact secrets in logs.
- **Validation**: sanitize all untrusted inputs at boundaries; validate against schemas.
- **Licenses**: prefer MIT/Apache‑2.0; consult SBOM when adding third‑party libs.

---

## Repo Layout & Do‑not‑edit Zones

- **Source**: `src/**`
- **Tests**: `tests/**` (mirrors `src`)
- **Docs & site**: `docs/**`, `site/**`
- **Generated artifacts**: `docs/_build/**`, `site/_build/**` → **do not hand‑edit**
- **Stubs**: `stubs/**` for local type shims (referenced by `stubPath`)

---

## CI / Pre‑commit parity

- **Job order**: precommit → lint → types → tests → docs
- **OS matrix**: lint/types on linux + macOS; tests on linux (expand later if needed)
- **Caches**: `~/.cache/uv`, `~/.cache/ruff` keyed on OS + Python + lock/config
- **Artifacts**: docs site, Agent Portal, coverage, JUnit are uploaded for each run
- **Branch protection (recommended)**: require `precommit`, `lint`, `types`, `tests` to merge

---



---

## Schema-First Development (Pandera Unified Schemas)

This project uses a unified schema approach where Pandera `DataFrameSchema` is the single source of truth for dataset structure. The schema unification architecture provides:

### Core Components

- **DatasetSchema** (`build/hamilton/contracts/schemas/schema.py`): Unified wrapper for Pandera schemas with metadata
- **DatasetSchemaRegistry** (`build/hamilton/contracts/schemas/registry.py`): Global registry for all dataset schemas
- **Constraint Layer** (`build/hamilton/contracts/schemas/constraints.py`): Extract and aggregate constraints from schemas
- **Introspection Service** (`build/hamilton/contracts/schemas/introspection.py`): Complete dataset introspection for LLM/tooling

### Schema Registration

All datasets should be registered in `SCHEMA_REGISTRY`:

```python
from codeintel.build.hamilton.contracts.schemas import SCHEMA_REGISTRY

# Get a schema
schema = SCHEMA_REGISTRY.require("analytics.function_metrics")

# Check if registered
if SCHEMA_REGISTRY.get("analytics.my_table") is not None:
    ...

# List all registered schemas
all_keys = SCHEMA_REGISTRY.all()
```

### Schema-Aware Adapters

Adapters can inherit `SchemaValidationMixin` for automatic Pandera validation:

```python
from codeintel.build.analytics.adapters.schema_adapter import SchemaValidationMixin

class MyAdapter(BatchAdapter[MyRow], SchemaValidationMixin):
    table_key: ClassVar[str] = "analytics.my_table"
    
    def persist_with_validation(self, rows: Sequence[MyRow], *, strict: bool = True) -> int:
        df = pd.DataFrame(rows)
        validated_df = self.validate_dataframe(df) if strict else self.try_validate_dataframe(df)
        # ... persist validated_df
```

### Schema-Aware Writing in Build Context

Use `write_validated_table()` for DataFrame validation before persistence:

```python
async def execute(self, ctx: TargetExecutionContext) -> TargetResult:
    df = compute_metrics(...)
    ctx.write_validated_table("analytics.function_metrics", df, strict=True)
    return TargetResult.success()
```

### Dataset Introspection

Introspect datasets for LLM/agent consumption:

```python
from codeintel.build.hamilton.contracts.schemas.introspection import introspect_dataset

intro = introspect_dataset("analytics.function_metrics")
print(intro.summary_for_llm())
```

### CLI Commands

- `codeintel dataset info <table_key>` - Show schema information
- `codeintel dataset flow <table_key>` - Show producer/consumer relationships
- `codeintel dataset constraints <table_key>` - Show extracted constraints

### Adding New Datasets

1. Define the Pandera schema in `build/hamilton/contracts/schemas/pandera_schemas.py`
2. Register via `build_dataset_schema()` or directly in `SCHEMA_REGISTRY`
3. Use `typed_dict_from_pandera()` to generate row models
4. Inherit `SchemaValidationMixin` in adapters for validation

### Row Model Access

Manual row models have been removed; schema-generated row models are canonical.
Use the schema registry to obtain a TypedDict row model when needed:

```python
from codeintel.build.hamilton.contracts.schemas import SCHEMA_REGISTRY

schema = SCHEMA_REGISTRY.require("analytics.function_metrics")
row_model = schema.get_row_model()
```

---

## NetworkX GPU backend

- Use `GraphBackendConfig` plus `codeintel.cli.nx_backend.maybe_enable_nx_gpu`; do not import
  `nx_cugraph` outside that module.
- New graph analytics should accept `graph_backend: GraphBackendConfig | None`, compute
  `use_gpu`, and pass it into NetworkX loaders and `build_graph_context`.
- Adjust backend wiring in `nx_backend.py` or `_maybe_to_gpu_graph` rather than scattering
  backend logic across analytics modules.

---

## Ibis 11 Patterns (DuckDB Backend)

This project uses **Ibis 11.0.0+** with DuckDB. Key API patterns:

### Connection

```python
# ✅ Correct - Ibis 11
ibis_con = ibis.duckdb.from_connection(duckdb_connection)

# ❌ Deprecated - pre-Ibis 11
ibis_con = ibis.duckdb.connect(con=duckdb_connection)
```

### Table Access with Qualified Names

```python
# ✅ Correct - use database parameter
table = con.table("function_metrics", database="analytics")

# ❌ Will fail in Ibis 11
table = con.table("analytics.function_metrics")

# ✅ Or use IbisGateway.table() which handles splitting automatically
table = gateway.ibis.table("analytics.function_metrics")
```

### View Creation with Qualified Names

```python
# ✅ Correct - use database parameter
con.create_view("v_function_summary", expr, database="analytics", overwrite=True)

# ❌ Will create view in wrong schema
con.create_view("analytics.v_function_summary", expr, overwrite=True)

# ✅ Or use _create_view() helper in ibis_views.py
_create_view(con, "analytics.v_function_summary", expr)
```

### Case Expressions

```python
# ✅ Correct - Ibis 11 ibis.cases() builder
bucket = ibis.cases(
    (loc <= 50, "small"),
    (loc <= 200, "medium"),
    else_="large",
)

# ❌ Removed in Ibis 11
bucket = ibis.case().when(loc <= 50, "small").when(loc <= 200, "medium").else_("large").end()
```

### Helper Functions

Two helper functions in `src/codeintel/storage/views/ibis_views.py` handle qualified names:

- `_table(con, "schema.table")` - Returns Ibis table with correct database parameter
- `_create_view(con, "schema.view", expr)` - Creates view with correct database parameter

The `IbisGateway.table()` method in `src/codeintel/storage/ibis_adapter.py` also handles
qualified names automatically.

### Deprecated Methods to Avoid

| Deprecated | Replacement |
|------------|-------------|
| `String.to_date` | `String.as_date` |
| `String.to_timestamp` | `String.as_timestamp` |
| `IntegerValue.to_interval` | `IntegerValue.as_interval` |
| `IntegerValue.to_timestamp` | `IntegerValue.as_timestamp` |
| `Struct.destructure` | `Table.unpack` |

---

## Bulk Operations (DuckDBPolicyBackend)

All bulk insert and upsert operations should use `DuckDBPolicyBackend` from
`codeintel.storage.duckdb_policy_backend`. This centralizes SQL generation and
ensures type-safe operations via SQLGlot.

### Bulk Insert

```python
from codeintel.storage.duckdb_policy_backend import DuckDBPolicyBackend

backend = DuckDBPolicyBackend(gateway)

# Bulk insert with explicit columns
rows = [
    ("goid1", "repo", "commit", 100),
    ("goid2", "repo", "commit", 200),
]
count = backend.bulk_insert(
    "analytics.function_metrics",
    rows,
    columns=["function_goid_h128", "repo", "commit", "loc"],
)

# Bulk insert with columns from TableSchema (if registered)
count = backend.bulk_insert("analytics.function_metrics", rows)
```

### UPSERT (Insert or Update)

For tables needing insert-or-update semantics (e.g., aggregations, caches):

```python
# UPSERT with ON CONFLICT DO UPDATE
count = backend.upsert(
    "analytics.function_metrics",
    rows,
    columns=["function_goid_h128", "repo", "commit", "loc"],
    conflict_columns=["function_goid_h128", "repo", "commit"],
    update_columns=["loc"],  # Only update these on conflict
)

# UPSERT with ON CONFLICT DO NOTHING (skip duplicates)
count = backend.upsert(
    "analytics.function_metrics",
    rows,
    columns=["function_goid_h128", "repo", "commit", "loc"],
    conflict_columns=["function_goid_h128", "repo", "commit"],
    update_columns=[],  # Empty = DO NOTHING
)
```

### Snapshot-Scoped Deletion

Before bulk inserts, clear existing data for the snapshot:

```python
backend.delete_for_snapshot(
    "analytics.function_metrics",
    repo="org/repo",
    commit="abc123",
)
```

### Deprecated Patterns

The following patterns are deprecated:

| Deprecated | Replacement |
|------------|-------------|
| `con.executemany(sql, rows)` | `backend.bulk_insert(table_key, rows)` |
| `QueryBuilder.insert()` | `backend.bulk_insert()` |
| `QueryBuilder.delete()` | `backend.delete_for_snapshot()` |
| `SafeTable`, `SafeColumn` | Use policy backend methods |

### When Direct executemany is Acceptable

Direct `executemany` usage is only acceptable in:

1. `DuckDBPolicyBackend` itself (the centralized location)
2. Storage layer helpers (`storage/helpers/db.py`)
3. Schema bootstrap (`storage/metadata/bootstrap.py`)
4. Test fixtures

All other code should use `DuckDBPolicyBackend.bulk_insert()` or `upsert()`.

---

## Glossary

- **Agent Catalog** — machine‑readable index of packages/modules/symbols with stable anchors and links
- **Agent Portal** — static HTML UI over the catalog with search and deep links
- **Anchor** — a deep link to source (editor/GitHub), optionally including a line number
- **PATH_MAP** — rules for translating container paths to editor workspace paths
- **DocFacts/NavMap** — generated indices that power docs and linking
- **RFC 9457 Problem Details** — standard JSON error envelope for HTTP APIs

---

**This document is the authoritative operating protocol for agents.** If a task conflicts with these rules, prefer the rules — or open a proposal under `openspec/changes/**` to evolve them.
