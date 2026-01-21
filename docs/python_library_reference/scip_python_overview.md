
Comprehensive Guide to Using scip-python for Python Code Indexing
# Introduction to scip-python and SCIP

scip-python is a precise Python code indexer built by Sourcegraph on top of the Pyright type checker
sourcegraph.com
github.com
. It generates a SCIP index for a Python project, which is a serialized database of symbols (functions, classes, variables, etc.) and their relationships, enabling code intelligence features like “Go to definition” and “Find references” across and within repositories
sourcegraph.com
sourcegraph.com
. The term SCIP stands for “SCIP Code Intelligence Protocol”, a language-agnostic format (encoded via Protocol Buffers) designed as a successor to LSIF for scaling precise code navigation
sourcegraph.com
sourcegraph.com
. In practical terms, scip-python analyzes your Python code (and its dependencies) and produces an index.scip file containing a complete index of symbols, definitions, and references in your codebase.

Key benefits of scip-python:

Compiler-accurate indexing: scip-python reuses Pyright’s deep understanding of Python (types, imports, etc.), so the index is precise and handles things like type hints and infers references accurately
sourcegraph.com
sourcegraph.com
.

Cross-repository linking: If your project’s dependencies are available (e.g. in your virtual environment), scip-python will embed stable references to external package symbols (including package name and version) in the index. This enables cross-repo navigation – for example, if your code calls yaml.dump() from the PyYAML library, the index will tag that call with a symbol like scip-python python PyYAML 6.0 yaml/dump() including the package name/version
sourcegraph.com
. If the PyYAML library itself is indexed, a “Go to definition” can take you directly to PyYAML’s implementation of dump
sourcegraph.com
.

Find implementations: The index includes information to support “find implementations” (for classes and methods) by linking subclass relationships and overridden methods
sourcegraph.com
sourcegraph.com
.

Language-agnostic format: The output index.scip uses a standardized Protobuf schema for code indexing. This means tools can consume the index in a uniform way for multiple languages (Sourcegraph provides indexers for Java, TypeScript, C++, etc. in addition to Python
github.com
).

In summary, scip-python provides an offline index of your Python code’s symbol graph, which can be used with Sourcegraph or other developer tools to enable advanced code navigation and analysis.

Repository note: In this codebase, scip-python is the required indexer. The optional
`scip` CLI binary (from Sourcegraph) is only used for `scip print/snapshot/test`
hooks; when it is not installed those hooks are disabled.

Installation and Prerequisites

Using scip-python requires a few specific tools and environment setup. We assume a Linux environment for the examples below (the commands are similarly applicable to macOS). The prerequisites are:

Python 3.10+ – scip-python only supports indexing projects using Python 3.10 or newer
github.com
. Ensure your project is running on a supported Python version.

Node.js 16+ and npm – scip-python is distributed as an NPM package (it’s implemented as a Node CLI around Pyright). You should have Node v16.0 or above installed to run it
github.com
.

scip-python package – Install the scip-python tool via NPM. It is recommended to install it globally for convenient CLI use. For example:

$ npm install -g @sourcegraph/scip-python


This will provide the scip-python command. (You may also want to install the Sourcegraph CLI @sourcegraph/src globally if you plan to upload indexes to a Sourcegraph instance, as shown later.)

A Python virtual environment with project dependencies installed – scip-python needs to analyze your code in the context of its dependencies. Activate the virtualenv for your project and ensure all dependencies (from requirements.txt or pyproject) are installed before running the indexer
sourcegraph.com
.

pip or an environment file – By default, scip-python will invoke pip to detect the installed packages and their versions/files in your environment
github.com
. If your environment is not pip-based (e.g., using Conda or other means), you can supply an explicit environment description file (JSON) with the --environment option (detailed below).

Protocol Buffers tools (for programmatic use of the index): If you plan to programmatically read the generated index.scip file in Python, ensure you have the Protocol Buffers compiler (protoc) installed and the Python Protobuf library (protobuf package) available. This will allow you to generate and use Python classes from the SCIP .proto schema. (We will cover this in a later section.)

Summary of assumptions: We assume a Linux environment with Node.js and npm available, Python 3.10+ interpreter, and the Protocol Buffers toolkit installed. These tools provide the foundation for running the scip-python CLI and working with the output. With these in place, you’re ready to generate and use SCIP indexes for Python code.

Using the scip-python CLI to Index a Python Project

The primary way to use scip-python is via its command-line interface. In this mode, you run scip-python as a tool that analyzes your code and emits an index.scip file. This section covers the significant use cases and options of the CLI, including indexing the whole project or parts of it, customizing how dependencies are handled, and preparing the index for use with Sourcegraph or other tools.

Basic Indexing Command

To index a Python codebase, navigate to the root of your project (after activating the relevant virtualenv), and run the scip-python index command. At minimum you should provide the directory to index (usually . for the current directory) and a project name identifier. For example:

$ scip-python index . --project-name my_project


This will scan all Python files in the current directory and produce an index.scip file in the working directory. The --project-name is an arbitrary identifier (usually the repository or package name) that will be recorded in the index metadata. It helps identify your project’s symbols, especially in a cross-repository context
github.com
. After a successful run, you should see an index.scip binary file generated at the project root.

By default, scip-python will attempt to use your environment’s installed packages (via pip) to resolve external symbols. Ensure that all required dependencies are installed; if they are not, scip-python might not index references to those libraries fully.

Example usage:

$ cd /path/to/my-python-project
$ pip install -r requirements.txt         # install deps into venv
$ scip-python index . --project-name my_project


If the command completes successfully, you’ll have an index.scip file containing the index. The CLI may output progress or any errors encountered (for example, if Pyright finds import errors or type errors, you might see those logged as diagnostics in the index).

CodeIntel integration defaults

CodeIntel standardizes several scip-python behaviors to keep SCIP ingestion deterministic and easy to maintain:

- Project identity: project_name is always set; project_version/project_namespace are optional
  overrides via ScipIndexSettings.
- Protobuf parsing: index.scip is parsed via generated scip_pb2 bindings; JSON artifacts are not used.
- DAG codegen: the scip_proto target runs scripts/scip_proto_codegen.py and publishes scip_pb2.py.
- Environment manifests: optional env JSON generation via scripts/gen_scip_env.py (or
  ScipIndexSettings.generate_env_json/env_json_path).
- Incremental indexing: optional shard reuse via use_incremental_shards/shards_dir/shards_manifest_path.
- CLI hooks: optional scip print/snapshot/test steps for diagnostics.
- Extracted tables: scip_metadata includes tool_arguments + project identity; scip_document_symbols
  preserves per-document symbol attribution; scip_signature_occurrences captures signature docs.

CodeAnatomy extraction outputs (flat tables)

- scip_metadata_v1 (tool + project identity, text_document_encoding)
- scip_index_stats_v1 (document/occurrence/diagnostic counts + text telemetry)
- scip_documents_v1 (document_id, path, language, position_encoding)
- scip_document_texts_v1 (optional document text)
- scip_occurrences_v1 (ranges + role flags + syntax_kind_name)
- scip_symbol_information_v1 (symbol metadata + signature_text/signature_language)
- scip_document_symbols_v1 (per-document symbol attribution)
- scip_external_symbol_information_v1 (external symbol metadata)
- scip_symbol_relationships_v1 (reference/implementation/type_definition/definition edges)
- scip_signature_occurrences_v1 (signature occurrences)
- scip_diagnostics_v1 (diagnostics attached to occurrences)

Using the index with Sourcegraph: If your goal is to use Sourcegraph’s code intelligence, the next step would be to upload this index to a Sourcegraph instance. Using Sourcegraph’s CLI (src), you can do:

$ src code-intel upload --file index.scip --upload-type=scip


This assumes you have src configured to point to your Sourcegraph and authentication set up. (The older command was src lsif upload, but Sourcegraph now accepts SCIP uploads similarly
sourcegraph.com
github.com
.) Once uploaded, Sourcegraph will process the index (converting it to its internal format) and your repository will have precise code navigation enabled. However, even without Sourcegraph, the index.scip file can be consumed by other tools or inspected as we will see later.

Indexing a Subdirectory or Single Module (Using --target-only)

In some cases, you may not want to index the entire repository. For example, in a monorepo or a project with multiple components, you might generate separate indexes for different subpackages. The --target-only option allows you to restrict indexing to a specific subdirectory of the project
github.com
. This is useful to reduce indexing time or focus on a particular module.

For example, to index only the src/lib subdirectory of the project:

$ scip-python index . --project-name my_project --target-only=src/lib


This will index files under src/lib (and still include references to external packages as needed), producing an index.scip for just that part of the codebase
github.com
. The project-name in the index metadata remains whatever you specify (e.g., "my_project"), but only symbols under the target path are indexed. Use this flag if you have a large repository and need fine-grained control over indexing scope.

Adding a Namespace to Project Symbols (Using --project-namespace)

By default, the symbols emitted in the index will include your project name, but not necessarily a higher-level namespace. Sometimes, especially for cross-repository navigation, it’s useful to prefix all symbols with a custom namespace. The --project-namespace flag accomplishes this by prepending a string to all symbol identifiers generated for your project
github.com
.

When to use this: If your project is typically referenced with a certain import prefix or lives inside an implicit namespace (e.g., internally your code is accessed as company.my_project.module), you might want the symbols to reflect that to avoid clashing with similarly named symbols from other projects.

Example:

$ scip-python index . --project-name my_project --project-namespace=companyName.my_project


This would cause a symbol that might normally be indexed as, say, my_project/foo.py:function:bar to appear as companyName.my_project/foo.py:function:bar (the exact format is simplified for illustration). In practice, the namespace is inserted into the symbol strings. According to the documentation, if your project is loaded with a prefix and you provide --project-namespace, then “all symbols will have implicit.namespace (your prefix) prepended to their symbol”, enabling cross-repo navigation even if the directory structure doesn’t reflect that namespace
github.com
. This is an advanced option primarily for ensuring unique symbol identities across different projects.

If unsure, you can usually skip --project-namespace – it’s optional and not needed for most single-repo use cases, but it’s available for disambiguation when necessary.

Managing the Python Environment and Dependencies

One of scip-python’s strengths is indexing not just your code but also references to symbols in external libraries you import. To do this, scip-python needs to know what packages (and which versions) are present, and which files belong to those packages. There are two ways scip-python gathers this information:

By default, scip-python uses pip: It will invoke pip (specifically pip show or similar commands) to list installed packages and determine their names, versions, and file paths
github.com
. This means if you have installed your dependencies via pip (or pipenv/Poetry which ultimately uses pip under the hood), scip-python should automatically detect the packages and include them in the index’s metadata.

Alternatively, you can provide an environment file with the --environment flag. This is useful if you don’t use pip or want to avoid any pip calls (for example, in an environment managed by Conda or if pip is not available). The environment file is a JSON file containing a list of packages, with each package’s name, version, and list of files.

Using pip (default behavior)

If you installed packages via pip into your environment, you usually do not need to do anything special. Activate the environment and run scip-python index ... normally. The tool will call pip to retrieve information about each installed package (name and version), and it will attempt to enumerate the files for each package. The collected package info is embedded in the index’s metadata. For example, if you have PyYAML 6.0 installed, the index metadata will include an entry for PyYAML with version 6.0 and the list of files (like yaml/__init__.py, yaml/composer.py, etc.) that belong to that package
github.com
.

This information is used to create stable symbol identifiers for library symbols. Notably, the name and version in each package entry are combined in symbol strings so that references to external code are unambiguous. According to the scip-python docs, “the version of the package is used to generate stable references to external packages”
github.com
. This is why, for example, the symbol for yaml.dump() included PyYAML and 6.0 in the earlier example – it ensures that if different versions of PyYAML exist, their symbols are distinct
sourcegraph.com
.

In short, if everything is installed via pip, scip-python should “calculate [the environment] from the pip environment” automatically
github.com
. If you encounter any issues with this detection or if your environment is unusual, consider using the explicit environment file approach.

Using an environment JSON file (–environment flag)

If pip integration isn’t viable (or you prefer to predefine the environment), you can create a JSON file describing the environment’s packages. scip-python expects this file to be a list of package objects, where each object has:

name: The package/distribution name (e.g. "Django" or "PyYAML"). Note this might differ from the import module name in some cases (the docs give the example: package name PyYAML vs import name yaml
github.com
).

version: The package version as a string (e.g. "6.0").

files: An array of file paths (relative to the site-packages directory) that belong to this package. Essentially, all module files and metadata files for the package.

The scip-python README provides an example snippet for a package entry:

{
    "name": "PyYAML",
    "version": "6.0",
    "files": [
        "PyYAML-6.0.dist-info/INSTALLER",
        ...,
        "yaml/__init__.py",
        "yaml/composer.py",
        "yaml/tokens.py",
        ...
    ]
}


…and so on for each package
github.com
. Usually you would list all your top-level dependencies (and possibly their dependencies, unless scip-python only needs top-level ones; it might resolve recursively via imports). In practice, you might generate this list via a script or tool. Once you have the JSON, save it (e.g. as env.json).

Then run scip-python with the --environment option pointing to this file:

$ scip-python index . --project-name my_project --environment=path/to/env.json


This will disable any calls to pip and use your provided package list instead
github.com
. The output index will include those packages in its metadata. Make sure the file paths listed are accurate; if some package files are missing, references to them might not be indexed correctly.

Tip: If you are using this in a CI or hermetic build environment where pip isn’t available, you can create the environment file during the build. For example, if you have a lockfile or a list of installed files, transform it into the JSON format expected. The structure is straightforward – a list of {name, version, files} objects
github.com
github.com
.

Most users won’t need to manually create this file, but it’s good to know it exists. Using pip is generally easier. The scip-python documentation notes: “If you’re just using pip, this [environment file] should not be required… The goal is to support standard pip installations without additional configuration.”
github.com
. So, consider --environment an advanced option for special situations.

Handling Large Projects and Memory

Indexing a large codebase or one with many dependencies can be memory-intensive (since Pyright needs to analyze many files). scip-python runs on Node.js, which by default has a memory limit. If you encounter out-of-memory errors during indexing, you can increase Node’s heap size. One simple way is to set the NODE_OPTIONS environment variable before running the indexer, for example:

$ export NODE_OPTIONS="--max-old-space-size=8192"
$ scip-python index . --project-name my_project


The above would allow up to ~8 GB of heap memory for scip-python (adjust the number as needed)
github.com
. This is just a troubleshooting tip – use it if you see Node heap allocation failures.

Verifying and Uploading the Index

After running the indexer, you might want to verify that the index.scip is valid. While it’s a binary file (Protocol Buffers encoded), you can do a basic sanity check on its content using the Protocol Buffers tools. One quick check is using protoc (the protobuf compiler) to decode it into a human-readable text format. For example:

$ protoc --decode=scip.Index scip.proto < index.scip > index.txt


In this command, scip.proto is the SCIP schema file (available from the Sourcegraph/scip repository), and we use --decode=scip.Index to decode the top-level Index message
help.sourcegraph.com
help.sourcegraph.com
. The result index.txt will contain a text representation of the index, including metadata, documents, symbols, etc. This can be very long for large projects, but it’s useful for debugging (e.g., ensuring that certain symbols are present in the index). The Sourcegraph help center specifically suggests this method to inspect or troubleshoot an index.scip file
help.sourcegraph.com
.

Once satisfied, if your goal is Sourcegraph, use the src CLI to upload (as shown earlier). If using a CI pipeline for Sourcegraph’s auto-indexing, you might integrate scip-python accordingly (there’s also a Docker image sourcegraph/scip-python:autoindex that can be used in CI with Sourcegraph’s auto-indexing configuration
github.com
github.com
).

In summary, the CLI usage of scip-python covers installing the tool, running scip-python index with appropriate flags, and optionally converting or uploading the index for consumption. Next, we will look at how to use the index programmatically and other downstream uses.

Programmatic Usage and Integration of SCIP in Python

While the scip-python CLI is the primary interface, you may want to integrate the indexing process or the index data into your own Python tools or pipelines. In this section, we cover two main aspects of programmatic use:

Automating scip-python invocation (e.g., from a Python script or CI job using subprocess calls).

Reading and using the index.scip data within a Python program (e.g. to build a custom analysis tool or to feed an LLM with code context, etc.).

We also discuss converting the SCIP index to other formats (like LSIF) using the SCIP CLI, which can be part of integration scenarios.

Calling the scip-python Indexer from Python (Subprocess Usage)

If you have a Python application or script and you want to trigger the indexing (for example, as part of a build process or a larger pipeline), you can invoke the scip-python CLI via Python’s subprocess module. This allows you to script the indexing and handle errors programmatically.

Here’s a simple example of using Python to run scip-python on a project:

import subprocess
import sys

project_path = "/path/to/my-python-project"
project_name = "my_project"
env_file = None  # or "/tmp/env.json" if you have a custom environment file

# Build the base command
cmd = ["scip-python", "index", project_path, "--project-name", project_name]
# If using an environment JSON, add the flag
if env_file:
    cmd += ["--environment", env_file]

# Run the command
result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if result.returncode != 0:
    print("scip-python indexing failed.", file=sys.stderr)
    print(result.stderr.decode(), file=sys.stderr)
else:
    print("Indexing completed successfully.")


In this snippet, we compose the command as a list (including the path and project name). We optionally add the --environment flag if we have a custom environment file. We then execute it. We capture stdout and stderr; if needed, you can log or inspect these to get details from the scip-python run. On success, an index.scip should have been produced in project_path (the working directory can be set via cwd in subprocess.run if you want the index file in a specific place).

This approach is straightforward: scip-python is treated as an external tool. There is no direct Python library to import for scip-python’s functionality (since it’s essentially a Node.js program). Therefore, subprocess execution is the way to “embed” the indexing in a Python-driven workflow. You might use this method in a larger Python script that, say, checks if an index exists or is up-to-date, triggers re-indexing, then perhaps processes the output.

Note on performance: Indexing can be time-consuming for large projects, so consider running this asynchronously or in a separate process if integrating into a server or long-running application. For CI pipelines, you can run the above and then use the output file as needed.

Generating Python Classes from the SCIP Protobuf Schema

To work with the contents of index.scip in a Python program (beyond just treating it as an opaque file), you will need to parse the binary file. The index.scip file is a serialized Protobuf message of type scip.Index. Sourcegraph provides the SCIP protobuf schema (scip.proto) in the [sourcegraph/scip GitHub repository][98]. To use it in Python, you have a couple of options:

Use the Google Protocol Buffers compiler (protoc) to generate Python classes from scip.proto.

Use an existing binding if available (at the time of writing, official pre-built bindings exist for Go, Rust, TypeScript, etc., but not for Python, so you’ll likely generate your own)
github.com
.

(Alternatively, use the protoc --decode approach in a subprocess as shown above to get text, but that is more for debugging than programmatic access.)

Generating the Python module: Assuming you have scip.proto (download it from the GitHub repo, or use a specific release version), run:

$ protoc --python_out=. scip.proto


This will produce a file scip_pb2.py in the current directory (you can specify a package or directory as needed). The scip_pb2.py file contains Python classes corresponding to all the messages defined in scip.proto (Index, Document, Occurrence, SymbolInformation, etc.), as well as enums (like SymbolRole, Language, etc.).

You should ensure that the version of scip.proto you use matches the version of the index. If you installed scip-python via NPM, check its version and use the corresponding SCIP schema version (scip-python likely pins a SCIP version). For example, scip-python v0.4 might produce an index.scip following SCIP v0.4. The schema is generally backward compatible, but it’s best to match versions.

Now, with scip_pb2.py available (and protobuf installed in Python), you can parse the index file.

Parsing and Inspecting the index.scip in Python

Once you have the generated classes, you can read the binary file and parse it into an in-memory object. For example:

from scip_pb2 import Index

# Read the binary index.scip file
with open("index.scip", "rb") as f:
    data = f.read()

index = Index()
index.ParseFromString(data)

# Now 'index' is a populated Index message object.
print("Index metadata tool name:", index.metadata.tool_info.name)
print("Project root URI:", index.metadata.project_root)
print("Number of documents indexed:", len(index.documents))


In the above snippet, index (of type Index) will contain all the indexing information. We can examine some metadata (for instance, index.metadata.tool_info.name should be "scip-python" and the version of the tool
help.sourcegraph.com
, and index.metadata.project_root is typically a file URI of your project directory). The number of documents gives how many source files were indexed.

The Index message contains several fields, but the most important for usage are typically:

documents: This is a list of Document messages. Each Document represents a source file in your project. A Document will have at least a relative_path (path of the file relative to the project root) and a list of occurrences (and possibly diagnostics).

occurrences: Each Occurrence represents a occurrence of a symbol or a semantic token in the code
docs.rs
. An occurrence ties a span in the source (line and character range) to a symbol (a string that uniquely identifies a semantic symbol) and possibly additional metadata like symbol role or syntax highlighting info.

symbols (often via symbol_information list): For many symbols that are defined in your project (and external symbols as well), there will be a SymbolInformation entry giving details about the symbol
docs.rs
. This can include documentation strings, and relationships (e.g., this class extends another, or this method overrides a parent method, etc.).

external_symbols: scip-python might separate out symbols defined outside the project. (Depending on SCIP version, external symbols’ info might appear in the same symbol_information list with a flag, or a separate field. Check the schema for fields like external_symbols or flags on SymbolInformation.)

Let’s illustrate how to navigate this structure with code snippets.

Example: Listing Definitions and Finding References

One common use of a code index is to find all references to a particular symbol, or to list all definitions in a project. We can do this by iterating over the documents and occurrences.

Each Occurrence has:

a symbol field (string) which is the identifier of the symbol at that location,

a symbol_roles field (integer) which is a bitmask indicating the role of the occurrence (definition, reference, import, write, read, etc.),

a range which gives the location within the document
github.com
.

Identifying definitions vs references: The symbol_roles bitmask uses the SCIP convention where different bits denote different roles. For example, the bit value 1 (binary 0001) denotes a Definition occurrence
docs.rs
. Other bits include 2 for Import, 4 for Write access, 8 for Read access, etc.
docs.rs
. If an occurrence’s symbol_roles has the definition bit set, it means that occurrence is where the symbol is defined. If not, then the occurrence is a usage/reference (or import) of that symbol. In code, you can test this by checking (occ.symbol_roles & SymbolRole.Definition) != 0 (assuming you have an enum or constants for SymbolRole). Without the enum, simply use & 1 to check the lowest-order bit.

Let’s print out all symbol definitions in the project:

from scip_pb2 import SymbolRole  # this enum was generated from the proto
for doc in index.documents:
    for occ in doc.occurrences:
        # Check if this occurrence is a definition of the symbol
        if occ.symbol_roles & SymbolRole.Definition:  # or & 1 if enum not available
            symbol = occ.symbol
            # occ.range is a list: [start_line, start_char, end_line, end_char] or [line, start_char, end_char]
            start_line = occ.range[0]  # starting line (0-based indexing likely)
            print(f"Definition of {symbol} in {doc.relative_path} at line {start_line}")


This will iterate through each document and occurrence, and print out those marked as definitions
docs.rs
. The output might look like:

Definition of scip-python python my_project 1.0 module/foo.class:Foo in src/foo.py at line 10
Definition of scip-python python my_project 1.0 module/foo.function:do_thing() in src/foo.py at line 42
...


(The symbol strings are verbose by design – they encode the language, package, etc. Don't be alarmed by their length; they ensure uniqueness.)

Now, suppose we want to find all references to a particular symbol, say the function do_thing defined above. We need the symbol string for it. We could get it from the definitions we printed (copy it), or we could derive it (if we know how the scheme works). For this example, let’s assume we captured the symbol from the definition occurrence. Then:

target_symbol = "scip-python python my_project 1.0 module/foo.function:do_thing()"  # symbol of interest
for doc in index.documents:
    for occ in doc.occurrences:
        if occ.symbol == target_symbol:
            if occ.symbol_roles & SymbolRole.Definition:
                print(f"Found definition in {doc.relative_path} at line {occ.range[0]}")
            else:
                print(f"Found reference in {doc.relative_path} at line {occ.range[0]}")


This will print one line for the definition (with the Definition bit) and all other lines where that symbol is referenced (without that bit). If the symbol is imported elsewhere, those occurrences might have the Import bit (value 2) set in addition to possibly being counted as references.

The key point is that by using the index’s occurrence list, we can perform custom analyses like: find all references to function X, find unused code (symbols defined but never referenced would show up with a definition but no other occurrences), build a call graph, etc. The data is all in the index; you just have to traverse it.

Using SymbolInformation: The index’s symbol_information entries (if present) can provide more context. For example, SymbolInformation for a function symbol might contain its documentation string (docstring) and a list of Relationship messages indicating, say, that it overrides another symbol or is overridden by others (in OOP scenarios). It might also indicate if a class implements an interface, etc. If you need such data, you can map SymbolInformation by symbol name and look them up. For instance:

# Build a lookup for symbol metadata
symbol_info_map = { info.symbol: info for info in index.symbols }  # or index.symbols or index.symbol_information depending on schema
sym = target_symbol
if sym in symbol_info_map:
    info = symbol_info_map[sym]
    doc = info.documentation  # maybe a string or list of strings
    relationships = info.relationships  # e.g., info.relationships[i].symbol gives related symbol
    print(f"Documentation for {sym}: {doc}")
    for rel in relationships:
        if rel.is_implementation:  # hypothetical field indicating "implements interface" or "overrides"
            print(f"  Implements/Overrides: {rel.symbol}")


The exact fields may vary; check the generated scip_pb2.py or the SCIP schema for details. But generally, this is how you could get additional context on symbols.

Languages and encoding: Each Document has a language field (an enum value) which for Python should be Language.Python. The index’s metadata also contains a field text_document_encoding which indicates whether character offsets are UTF-8 or UTF-16 (for Python it’s usually UTF8)
help.sourcegraph.com
. This is important if you correlate character offsets to file content (e.g., if you open the file to fetch the exact identifier name at that range).

In summary, by parsing the index.scip file in Python, you unlock the ability to perform custom code analysis using the rich symbol graph that scip-python produced. This can power tools like documentation generators, refactoring tools, or as one user scenario suggested, providing context to language model assistants by pulling not just a function’s code but also all of its references and dependents from the index.

Converting the SCIP Index to Other Formats (LSIF)

If you need to use the index with systems that don’t directly support SCIP, you can convert it to the more widely supported LSIF format. For instance, GitLab’s code intelligence (as of GitLab 17.x) expects an LSIF file. Sourcegraph provides a utility for this conversion via the SCIP CLI (a separate tool from scip-python). The SCIP CLI can translate .scip to an LSIF dump.

Obtaining the SCIP CLI: You can download a prebuilt binary from the SCIP releases (on the Sourcegraph/scip GitHub) or build it from source (it’s a Go program). The GitLab docs show an example of fetching a specific version of the SCIP CLI in a CI job
docs.gitlab.com
. In short, you can download a tarball for your OS/ARCH or use go install github.com/sourcegraph/scip/cmd/scip@latest if you have Go.

Using SCIP CLI to convert: Once you have the scip tool, run:

$ scip convert --from index.scip --to dump.lsif


This reads index.scip and outputs dump.lsif (in LSIF JSON Lines format, or sometimes a SQLite if that extension is used). The GitLab documentation confirms this usage: “use the SCIP CLI to convert indexes generated with SCIP tooling into an LSIF-compatible file.”
docs.gitlab.com
. In the context of a CI example, after indexing with scip-python, they do:

./scip convert --from index.scip --to dump.lsif


and then upload dump.lsif as an artifact
docs.gitlab.com
. The resulting LSIF can be consumed by GitLab or any other tool expecting LSIF v0.4.3 format
sourcegraph.com
. This compatibility layer is how Sourcegraph itself initially handled SCIP uploads (converting server-side), but now the tools are available for users to do it themselves if needed
sourcegraph.com
.

Other conversions and tools: The SCIP CLI has some other handy features. For example, an experimental SQLite conversion: scip expt-convert --to sqlite ... will output the index into a SQLite database, which you can then query with SQL (this can be useful for debugging or exploring the index data without writing a parser)
github.com
. This isn’t typically needed in production pipelines, but it’s a powerful way to inspect data. There are also language-specific indexers (scip-java, scip-typescript, etc.) and a Sourcegraph CLI that supports SCIP. If you are integrating with various systems, be aware of those tools; however, for Python, scip-python plus the generic SCIP CLI should cover most needs.

Downstream Uses of the Index

To recap some ways the index.scip can be used downstream:

Sourcegraph or code hosts: Upload to enable precise navigation on Sourcegraph, or convert to LSIF for platforms like GitLab which currently require LSIF
docs.gitlab.com
.

Custom analysis tools: In your own Python tooling, use the index to answer questions about the code (who calls this function, where is this variable defined, etc.) without having to re-parse code – leverage the compiler-accurate index. This can significantly speed up code analysis tasks since the heavy lifting was done by scip-python.

IDE/Editor integration: While not directly consumer-facing, one could imagine using the SCIP index to power editor features. (Though normally an LSP would handle that; still, SCIP indexes could be used for offline analysis or in editors that can ingest such data.)

Documentation generators or code review tools: Extract docstrings and symbol relationships from the index to generate documentation or to assist in code reviews (e.g., showing how a change to a function might impact references across the project).

LLM context providers: As hinted in some community discussions, one could use SCIP data to gather all related code symbols and their content to provide to an AI coding assistant for better context (for example, fetching all references and definitions related to a symbol when a question is asked)
reddit.com
. The index gives a quick way to navigate the code graph without manually grep-ing through files.

In essence, the SCIP index produced by scip-python is a rich snapshot of your code’s structure. By using the techniques above, you can integrate that data into various tools and workflows, far beyond the basic “upload to Sourcegraph” scenario.

Conclusion

In this guide, we covered a full spectrum of scip-python usage: from installing and running the CLI to index a project, through advanced options like targeting subdirectories and providing custom environment info, to programmatic parsing of the index and converting it for use in other systems. To summarize:

CLI usage of scip-python involves running scip-python index with appropriate flags. Important flags include --project-name (required), --target-only (to limit scope), --project-namespace (to namespace symbols), and --environment (to specify dependencies manually). Ensure Node.js and Python environments are set up as prerequisites.

The output is an index.scip file, a Protocol Buffer encoded index. This file contains comprehensive data about all symbols in your code and their relations, powered by the Pyright analyzer.

Using the index can be as simple as uploading to Sourcegraph for navigation, or as involved as writing your own Python code to parse it. We demonstrated loading the index with Python protobuf classes, and iterating through documents and occurrences to find definitions and references. The schema (Index/Document/Occurrence/SymbolInformation/etc.) provides a powerful representation that you can leverage in custom ways.

Symbol roles and relationships are encoded in the index, allowing one to distinguish definitions from references, detect read/write accesses, and follow implementation chains. We cited how the bitmask roles work (e.g., Definition = 1, ReadAccess = 8, etc. in the enum)
docs.rs
 and how to use them.

Downstream integration can involve converting SCIP to LSIF (using the scip CLI’s convert command) for compatibility with other tools. We gave an example of doing so for GitLab’s code intelligence, which does not yet natively support SCIP
docs.gitlab.com
docs.gitlab.com
.

Assumptions and environment: We assumed a Linux environment with necessary tools (Node, protoc). We listed these assumptions and indeed used protoc in examples for decoding and generating classes. If you replicate this in your environment, ensure those tools are present. The protobuf schema is available openly, and generating bindings is encouraged if you want to consume the data in a type-safe way
github.com
.

With this knowledge, you should be able to set up scip-python for your Python projects, generate indexes, and integrate the index data into your own applications or workflows. The combination of scip-python’s precise indexing and the flexibility of consuming the data opens up many possibilities for understanding and navigating complex codebases.

References:

Sourcegraph scip-python README (installation, usage, environment flags)
github.com
github.com
github.com
github.com
github.com
github.com
github.com

Sourcegraph blog on scip-python (context on precise indexing and cross-repo example)
sourcegraph.com

Sourcegraph SCIP protocol repository (schema and docs for consuming data)
github.com

Rust docs for SCIP (SymbolRole bits definition)
docs.rs

Sourcegraph help center (decoding SCIP with protoc)
help.sourcegraph.com

GitLab documentation (using SCIP CLI to convert to LSIF in CI)




## scip-python “full CLI + config surface” (focused on **`--project-version`** + **Pyright config pass-through**)

### Mental model: scip-python = “Pyright analysis + SCIP emission”

scip-python is a Sourcegraph fork of **Pyright** whose focus is generating **SCIP** indexes; the repo explicitly says it’s “primarily an addition to Pyright” with “no substantial changes” to the Pyright library, and calls out only a few targeted behavioral changes (e.g., don’t bail out on long runs, don’t drop files under memory pressure, parse some additional files). ([GitHub][1])
**Practical implication:** almost everything that controls “what gets analyzed” and “how imports/types resolve” is governed by **Pyright configuration** (and the Python environment), while scip-python CLI flags primarily control **SCIP packaging/identity** and a few scip-python-specific behaviors.

---

# 1) The scip-python CLI surface you must treat as “contract”

The README documents a limited set of flags, but they’re the core knobs you’ll use in automation:

### 1.1 `scip-python index <root>`

Canonical invocation (after activating your venv) is:

```bash
scip-python index . --project-name="$MY_PROJECT"
```

([GitHub][1])

### 1.2 Required identity: `--project-name`

This is *the* stable identifier for your indexed project. It becomes the “package” name for symbols emitted for this project (analogous to how external packages are named). It’s required in all documented examples. ([GitHub][1])

### 1.3 Recommended identity: `--project-version`

The Sourcegraph auto-indexing skeleton in the official README includes:

```json
"indexer_args": [
  "scip-python", "index", ".",
  "--project-name", "<your name here>",
  "--project-version", "_"
]
```

([GitHub][1])

**What it *means* operationally (important):**

* SCIP symbol identity encodes a `(package name, package version)` pair for **external** packages, and scip-python explicitly says the dependency `version` is used “to generate stable references to external packages.” ([GitHub][1])
* By direct analogy, `--project-version` is the version component for *your project’s* symbols. Sourcegraph’s skeleton uses `_` as a conventional “unknown / not versioned” placeholder. ([GitHub][1])

**High-signal takeaway:** if you want symbol identity that is stable across indexing runs *of the same build*, you must keep `(--project-name, --project-version, --project-namespace)` stable.

### 1.4 Output path: `--output` (observed, not in README)

The `--output` flag is used in the wild (and thus exists in the CLI) to set the destination `.scip` file:

```bash
scip-python index --project-name=... --output="output.scip"
```

([GitHub][2])

If you’re building a service, you should **always** set `--output` so you can write into a job artifact directory instead of relying on default `index.scip` placement.

### 1.5 Scope limiting: `--target-only`

Indexes only a subdirectory:

```bash
scip-python index . --project-name="$MY_PROJECT" --target-only=src/subdir
```

([GitHub][1])

### 1.6 Symbol prefixing: `--project-namespace`

Prepends a namespace segment onto *all* generated symbols for the project:

```bash
scip-python index . --project-name="$MY_PROJECT" --project-namespace=implicit.namespace
```

([GitHub][1])

This is the “collision-avoidance” and “multi-root monorepo” lever when different roots might otherwise emit overlapping symbol spaces.

### 1.7 Dependency modeling: `--environment`

scip-python uses `pip` to infer installed packages, but can be given an explicit JSON environment file (list of `{name, version, files}`) to bypass pip entirely. ([GitHub][1])

### 1.8 Memory knob: `NODE_OPTIONS`

If you hit Node OOM, the README recommends increasing heap:

```bash
NODE_OPTIONS="--max-old-space-size=8192" scip-python index ...
```

([GitHub][1])

---

## 1.9 “Full CLI” reality check (and what to do in your pipeline)

The README does **not** enumerate all flags. In a production indexing service, treat the CLI itself as the source of truth:

* **Always snapshot** `scip-python index --help` output during build image creation and commit it (or store as a build artifact).
* Build a strict allowlist of flags you rely on (`--project-name`, `--project-version`, `--output`, `--target-only`, `--project-namespace`, `--environment`) and fail CI if the help output changes incompatibly.

(You can automate this, see §4.3.)

---

# 2) Project identity strategy (name/version/namespace) for stable symbol identity

### 2.1 `--project-name` — what to choose

You want a value that is:

* globally unique across all repos you’ll index,
* stable across time,
* stable across directory relocations.

Common choices (pick one and standardize):

* **VCS canonical name** (e.g., `github.com/org/repo`)
* **Python distribution name** (from packaging metadata)
* **Internal service key** (e.g., `codeintel::<tenant>::<repo_id>`)

Note: scip-python has an open issue explicitly about improving inference of project name/version from packaging metadata (pyproject/setup.py), which implies you should not assume it’s auto-derived today. ([GitHub][3])

### 2.2 `--project-version` — what to choose

Use cases:

* **Release artifact indexing:** set to the package version (semver) you ship.
* **Repo commit indexing:** set to the git SHA (or an abbreviated SHA) for deterministic point-in-time identity.
* **“Always latest” indexing:** use `_` (the Sourcegraph skeleton) if you want one stable “rolling” identity. ([GitHub][1])

### 2.3 `--project-namespace` — when it matters

Use it when:

* you index multiple roots that would otherwise share symbol paths,
* you want to “mount” code under a logical prefix not present on disk,
* you want hard isolation between tenants/environments.

---

# 3) Pyright configuration pass-through (this is where most indexing behavior lives)

Pyright’s configuration is the primary way to control:

* which files are analyzed,
* how imports resolve,
* which interpreter / stdlib / platform rules apply,
* how multi-root repos are handled.

### 3.1 Where config comes from (and precedence)

Pyright supports:

* `pyrightconfig.json` at project root (default),
* `[tool.pyright]` inside `pyproject.toml`.

If both exist, **pyrightconfig.json takes precedence**. ([GitHub][4])
Relative paths are resolved relative to the config file, and shell expansions like `~` are not supported. ([GitHub][4])

### 3.2 The “must know” environment keys for indexing determinism

Below are Pyright keys that most directly affect scip-python’s indexing output (coverage, symbol resolution, diagnostics).

#### File selection

* `include`: project file roots (supports glob wildcards). Default is directory containing the config. ([GitHub][4])
* `exclude`: paths to exclude from analysis. **Important nuance:** excluded files *may still be analyzed if imported* by included files. ([GitHub][4])
* `ignore`: suppress diagnostics for files even if included or imported. ([GitHub][4])
* `strict`: paths that should be analyzed in strict mode (like `# pyright: strict`). ([GitHub][4])
* `extends`: inherit from another config (JSON or TOML), with overrides. ([GitHub][4])

#### Import resolution / interpreter selection

* `venvPath` + `venv`: point Pyright at a specific virtual environment’s site-packages. ([GitHub][4])
* `extraPaths`: additional import search roots (critical for `src/` layouts, generated code, monorepo libs). ([GitHub][4])
* `pythonVersion`: language version rules; affects parsing rules + which typeshed definitions apply. ([GitHub][4])
* `pythonPlatform`: platform-specific typing. ([GitHub][4])
* `executionEnvironments`: multi-root environment mapping; Pyright selects the first whose root matches a file path. ([GitHub][4])
* `typeshedPath`: override typeshed location. ([GitHub][4])
* `stubPath`: custom stub directory (default `./typings`). ([GitHub][4])

#### Analysis depth / library typing

* `useLibraryCodeForTypes`: if true, Pyright will read/analyze library code when stubs are missing (types will be incomplete but better than “unknown”). ([GitHub][4])

#### Debugging determinism

* `verboseOutput`: enables verbose logs (useful for diagnosing import resolution mismatches). ([GitHub][4])

### 3.3 “Why this matters for scip-python”

Because scip-python’s core is Pyright, these settings directly control:

* which symbols exist in the index,
* whether references resolve to external packages vs unresolved imports,
* whether certain files are even parsed (pythonVersion gating),
* whether monorepo packages are treated as separate environments (executionEnvironments),
* whether library symbols are discoverable without stubs (useLibraryCodeForTypes).

Also note scip-python intentionally differs from upstream Pyright in a few stability behaviors (don’t bail out on long indexing, don’t drop files under memory pressure, parse extra files). ([GitHub][1])

---

# 4) Practical recipes (dense, production-oriented)

## 4.1 “Single repo, src/ layout, .venv in repo”

`pyrightconfig.json`:

```json
{
  "include": ["src"],
  "exclude": ["**/__pycache__", "build", "dist", ".venv"],
  "extraPaths": ["src"],
  "venvPath": ".",
  "venv": ".venv",
  "pythonVersion": "3.11",
  "pythonPlatform": "Linux",
  "useLibraryCodeForTypes": true,
  "verboseOutput": false
}
```

Why:

* `extraPaths` makes `import mypkg` work even if runtime uses `PYTHONPATH=src`. ([GitHub][4])
* `venvPath`/`venv` forces Pyright (and therefore scip-python) to look in that venv for dependencies. ([GitHub][4])

Index command:

```bash
scip-python index . \
  --project-name "github.com/acme/myrepo" \
  --project-version "$(git rev-parse HEAD)" \
  --output "/tmp/artifacts/index.scip"
```

(You control identity explicitly; `--output` is observed in use.) ([GitHub][2])

## 4.2 Monorepo with multiple packages and incompatible deps (executionEnvironments)

`pyrightconfig.json` sketch:

```json
{
  "include": ["packages"],
  "executionEnvironments": [
    {
      "root": "packages/pkg_a",
      "venvPath": "packages/pkg_a",
      "venv": ".venv",
      "extraPaths": ["packages/pkg_a/src"]
    },
    {
      "root": "packages/pkg_b",
      "venvPath": "packages/pkg_b",
      "venv": ".venv",
      "extraPaths": ["packages/pkg_b/src"]
    }
  ],
  "pythonVersion": "3.11",
  "useLibraryCodeForTypes": true,
  "verboseOutput": true
}
```

Key: Pyright will choose the first execution environment whose `root` matches each file’s path. ([GitHub][4])
Then either:

* run one index per package with `--target-only`, or
* index the whole repo if deps don’t conflict.

Example per package:

```bash
scip-python index . \
  --project-name "github.com/acme/monorepo" \
  --project-version "$(git rev-parse HEAD)" \
  --project-namespace "pkg_a" \
  --target-only "packages/pkg_a" \
  --output "/tmp/artifacts/pkg_a.scip"
```

`--target-only` and `--project-namespace` are documented. ([GitHub][1])

## 4.3 Indexing-service pattern: generate config + run scip-python (Python code)

This is the “index untrusted repo in a sandbox” style pattern: write an ephemeral pyrightconfig.json tailored to your job, run scip-python, then delete.

```python
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

def write_pyrightconfig(
    repo_root: Path,
    *,
    include: list[str],
    exclude: list[str],
    python_version: str,
    venv_path: str | None = None,
    venv_name: str | None = None,
    extra_paths: list[str] | None = None,
    verbose: bool = False,
) -> Path:
    cfg: dict[str, object] = {
        "include": include,
        "exclude": exclude,
        "pythonVersion": python_version,
        "verboseOutput": verbose,
        "useLibraryCodeForTypes": True,
    }
    if extra_paths:
        cfg["extraPaths"] = extra_paths
    if venv_path and venv_name:
        cfg["venvPath"] = venv_path
        cfg["venv"] = venv_name

    path = repo_root / "pyrightconfig.json"
    path.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")
    return path

def run_scip_python(
    repo_root: Path,
    *,
    project_name: str,
    project_version: str,
    output_file: Path,
    node_heap_mb: int = 8192,
) -> None:
    env = os.environ.copy()
    env["NODE_OPTIONS"] = f"--max-old-space-size={node_heap_mb}"  # recommended knob
    cmd = [
        "scip-python", "index", str(repo_root),
        "--project-name", project_name,
        "--project-version", project_version,
        "--output", str(output_file),
    ]
    subprocess.run(cmd, cwd=repo_root, env=env, check=True)

# Example orchestration
repo = Path("/work/repo")
write_pyrightconfig(
    repo,
    include=["src"],
    exclude=["**/__pycache__", "dist", "build"],
    python_version="3.11",
    venv_path=".",
    venv_name=".venv",
    extra_paths=["src"],
    verbose=True,
)
run_scip_python(
    repo,
    project_name="github.com/acme/myrepo",
    project_version=os.popen("git -C /work/repo rev-parse HEAD").read().strip(),
    output_file=Path("/work/out/index.scip"),
)
```

Notes:

* `verboseOutput`, `include`, `exclude`, `venvPath`, `venv`, `extraPaths`, `pythonVersion`, `useLibraryCodeForTypes` are all Pyright config keys with documented semantics. ([GitHub][4])
* `NODE_OPTIONS` heap bump is explicitly recommended by scip-python. ([GitHub][1])
* `--project-version` is shown in Sourcegraph’s auto-indexing skeleton. ([GitHub][1])
* `--output` is observed in real usage. ([GitHub][2])

---

# 5) Debugging “index differs from runtime” issues (the fast checklist)

1. **Confirm which config Pyright is using**

* Is there both `pyrightconfig.json` and `[tool.pyright]`? `pyrightconfig.json` wins. ([GitHub][4])

2. **Import resolution mismatches**

* Set `verboseOutput: true` to see where imports are searched/resolved. ([GitHub][4])
* Check `extraPaths` for `src/` layout or generated code. ([GitHub][4])
* Check `venvPath`/`venv` if you rely on a non-default venv. ([GitHub][4])

3. **“Why is this excluded file still analyzed?”**

* Pyright’s `exclude` doesn’t prevent analysis if the file is imported by included files. ([GitHub][4])
  If you need “don’t analyze even if imported,” use `ignore` for diagnostics suppression or restructure include roots.

4. **Language-version parse errors**

* Set `pythonVersion` explicitly so Pyright/scip-python parse with the correct syntax rules. ([GitHub][4])

5. **Stubs vs library code**

* If stubs are missing and you want *some* types, ensure `useLibraryCodeForTypes` is enabled. ([GitHub][4])

---

If you want next: I can take this and produce a **flag-by-flag “CLI contract table”** by deriving the current `scip-python index --help` output from an authoritative build (and then map each CLI flag to the corresponding Pyright config key or scip-python behavior).

[1]: https://github.com/sourcegraph/scip-python "GitHub - sourcegraph/scip-python: SCIP indexer for Python"
[2]: https://github.com/sourcegraph/scip-python/issues/76 "Fatal Error While Indexing: RangeError · Issue #76 · sourcegraph/scip-python · GitHub"
[3]: https://github.com/sourcegraph/scip-python/issues/109?utm_source=chatgpt.com "Improve project name and version detection · Issue #109"
[4]: https://raw.githubusercontent.com/microsoft/pyright/main/docs/configuration.md "raw.githubusercontent.com"




## SCIP schema deep dive (symbol format + occurrences: `syntax_kind`, ranges, diagnostics)

This is a **protocol-level** guide: how to *interpret* and *consume* the **SCIP** (`index.scip`) data model—especially the parts you flagged: **symbol string grammar**, **occurrence fields** (range + `syntax_kind` + `symbol_roles` + `override_documentation` + `enclosing_range`), and **diagnostics**.

---

# 0) Top-level schema map (what’s in an `index.scip`)

### `Index`

A SCIP index is rooted at one directory, and the payload can be large; the schema explicitly recommends emitting/consuming it “one field value at a time,” and requires `metadata` to appear first for streaming consumption. ([Docs.rs][1])
Key fields:

* `metadata`: protocol version + tool info + project root + file encoding. ([Docs.rs][2])
* `documents[]`: per-file payload, each with occurrences and symbols. ([Docs.rs][3])
* `external_symbols[]` (optional): symbol info for referenced symbols defined externally; keep empty if you assume externals are indexed elsewhere; populate if you want hover docs even when externals won’t be indexed. ([Docs.rs][1])

### `Metadata`

Fields you *must* respect:

* `project_root`: **URI-encoded absolute path** to the repo root; all `Document.relative_path` are under this directory. ([Docs.rs][2])
* `text_document_encoding`: encoding of **source files on disk** referenced by `Document.relative_path`. This is explicitly **unrelated** to `Document.text` (protobuf strings are UTF-8). ([Docs.rs][2])

### `Document`

Fields:

* `relative_path`: file path relative to `project_root`. ([Docs.rs][3])
* `language`: string; often matches a standard enum but is string-typed to allow unknown languages. ([Docs.rs][3])
* `occurrences[]`: the core signal—ranges ↔ symbols ↔ highlighting ↔ diagnostics. ([Docs.rs][4])
* `symbols[]`: symbols “defined” in this document; includes symbols that have no direct definition but are defined via `Relationship.is_definition`. ([Docs.rs][3])
* `text` (optional): usually omitted; preferred that clients read file content via `project_root + relative_path`. Introduced to support signature documentation, and useful for in-memory documents. ([Docs.rs][3])
* `position_encoding`: the encoding of the **character offsets** in occurrence ranges for this document; recommended mapping depends on the indexer implementation language (Python → UTF32 offsets). ([Docs.rs][3])

---

# 1) SCIP **symbol** string format (authoritative grammar + why parsing is nontrivial)

SCIP symbols are comparable to URIs: they identify classes/methods/terms/local vars, and are designed to be **stable identifiers** across indexes. ([Docs.rs][5])

### 1.1 Grammar (what you must implement)

The schema defines the standardized string representation for `Symbol`:

* `<symbol> ::= <scheme> ' ' <package> ' ' (<descriptor>)+ | 'local ' <local-id>`
* `<package> ::= <manager> ' ' <package-name> ' ' <version>`
* `<scheme>` is UTF-8; spaces are escaped as **double space**; must not be empty or start with `'local'`
* `<manager>` / `<package-name>` / `<version>`: same escaping; `.` placeholder indicates empty value
* `<descriptor>` kinds are distinguished by suffix punctuation (`/`, `#`, `.`, `:`, `!`, `(...).`, `[T]`, `(x)`, etc.)
* backtick-escaped identifiers allow arbitrary UTF-8; backticks are escaped via **double backtick**. ([Docs.rs][5])

The descriptors together should form a fully qualified name (typically one per AST ancestry node), and **local symbols** must only be used for entities not accessible outside a document. ([Docs.rs][5])

### 1.2 Package triple: `(manager, name, version)`

`Package` is explicitly a unit of packaging/distribution with fields `manager`, `name`, `version`. ([Docs.rs][6])
In practice:

* For scip-python dependency symbols: `manager="python"`, `name=<distribution>`, `version=<dist version>` (e.g. `PyYAML 6.0`).
* For your project: you control the package name/version at indexing time (via scip-python flags), and those become the package triple for project-owned symbols (see earlier topic).

### 1.3 Why naïve parsing fails

You cannot safely do `symbol.split(" ")` because:

* spaces inside tokens are encoded as **double spaces**, meaning the delimiter is *single space not part of a double-space run*. ([Docs.rs][5])
  Also, you can’t treat descriptors as dot-separated—descriptor boundaries are semantic and depend on suffix punctuation.

---

## 1.4 Python code: robust parsing of a SCIP symbol string

This parser is designed for **consumption** (analysis tooling). It:

* tokenizes `<scheme> <manager> <name> <version> <descriptor-stream>` using “single-space delimiter / double-space escape”
* parses descriptor stream into typed descriptors

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional
import re

@dataclass(frozen=True)
class PackageTriple:
    manager: str
    name: str
    version: str

DescriptorKind = Literal[
    "namespace", "type", "term", "meta", "macro", "method", "type_parameter", "parameter"
]

@dataclass(frozen=True)
class Descriptor:
    kind: DescriptorKind
    name: str
    disambiguator: Optional[str] = None  # method-only

@dataclass(frozen=True)
class ParsedSymbol:
    is_local: bool
    scheme: Optional[str]          # None for local
    package: Optional[PackageTriple]
    descriptors: list[Descriptor]
    local_id: Optional[str]        # set for local

_IDENTIFIER_CHARS = re.compile(r"[A-Za-z0-9_\+\-\$]+")

def _split_tokens_with_double_space_escape(s: str) -> list[str]:
    """
    Split into tokens using ' ' (single space) delimiter, but treat '  ' as an escaped space.
    """
    out: list[str] = []
    cur: list[str] = []
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if ch == " ":
            # double-space => literal space
            if i + 1 < n and s[i + 1] == " ":
                cur.append(" ")
                i += 2
                continue
            # single-space => token boundary
            out.append("".join(cur))
            cur = []
            i += 1
            continue
        cur.append(ch)
        i += 1
    out.append("".join(cur))
    return out

def _parse_identifier(s: str, i: int) -> tuple[str, int]:
    """
    <identifier> ::= <simple-identifier> | <escaped-identifier>
    <escaped-identifier> uses backticks and `` escape.
    """
    if i >= len(s):
        raise ValueError("unexpected end while parsing identifier")

    if s[i] == "`":
        i += 1
        buf: list[str] = []
        while i < len(s):
            if s[i] == "`":
                # `` => literal `
                if i + 1 < len(s) and s[i + 1] == "`":
                    buf.append("`")
                    i += 2
                    continue
                # closing `
                i += 1
                return "".join(buf), i
            buf.append(s[i])
            i += 1
        raise ValueError("unterminated backtick identifier")

    m = _IDENTIFIER_CHARS.match(s, i)
    if not m:
        raise ValueError(f"invalid identifier at {i}: {s[i:i+20]!r}")
    return m.group(0), m.end()

def parse_scip_symbol(symbol: str) -> ParsedSymbol:
    # local symbol form: "local <local-id>"
    if symbol.startswith("local "):
        local_id = symbol[len("local "):]
        # spec says local-id is <simple-identifier>; we treat as a sanity check
        if not _IDENTIFIER_CHARS.fullmatch(local_id):
            raise ValueError(f"invalid local-id: {local_id!r}")
        return ParsedSymbol(True, None, None, [], local_id)

    toks = _split_tokens_with_double_space_escape(symbol)
    if len(toks) < 5:
        raise ValueError(f"expected at least 5 tokens, got {len(toks)}: {toks}")

    scheme, manager, pkg_name, version = toks[0], toks[1], toks[2], toks[3]
    desc_stream = " ".join(toks[4:])  # remaining spaces here are literal in the descriptor stream

    pkg = PackageTriple(manager=manager, name=pkg_name, version=version)
    descs: list[Descriptor] = []

    i = 0
    while i < len(desc_stream):
        # type-parameter: [name]
        if desc_stream[i] == "[":
            i += 1
            name, i = _parse_identifier(desc_stream, i)
            if i >= len(desc_stream) or desc_stream[i] != "]":
                raise ValueError("unterminated type-parameter")
            i += 1
            descs.append(Descriptor("type_parameter", name))
            continue

        # parameter: (name)
        if desc_stream[i] == "(":
            i += 1
            name, i = _parse_identifier(desc_stream, i)
            if i >= len(desc_stream) or desc_stream[i] != ")":
                raise ValueError("unterminated parameter")
            i += 1
            descs.append(Descriptor("parameter", name))
            continue

        # name + suffix-driven descriptor
        name, i = _parse_identifier(desc_stream, i)
        if i >= len(desc_stream):
            raise ValueError("unexpected end after identifier")

        suf = desc_stream[i]

        # namespace/type/term/meta/macro
        if suf in ["/", "#", ".", ":", "!"]:
            i += 1
            kind_map = {
                "/": "namespace",
                "#": "type",
                ".": "term",
                ":": "meta",
                "!": "macro",
            }
            descs.append(Descriptor(kind_map[suf], name))
            continue

        # method: name '(' (<simple-identifier>)? ').'
        if suf == "(":
            i += 1
            disamb: Optional[str] = None
            # optional disambiguator until ')'
            if i < len(desc_stream) and desc_stream[i] != ")":
                m = _IDENTIFIER_CHARS.match(desc_stream, i)
                if not m:
                    raise ValueError("invalid method disambiguator")
                disamb = m.group(0)
                i = m.end()
            if i >= len(desc_stream) or desc_stream[i] != ")":
                raise ValueError("unterminated method ()")
            i += 1
            if i >= len(desc_stream) or desc_stream[i] != ".":
                raise ValueError("method must end with ').'")
            i += 1
            descs.append(Descriptor("method", name, disambiguator=disamb))
            continue

        raise ValueError(f"unexpected descriptor suffix {suf!r} at {i} in {desc_stream!r}")

    return ParsedSymbol(False, scheme, pkg, descs, None)
```

**Try it on a real-world external symbol string** (as shown in Sourcegraph’s examples elsewhere):
`scip-python python PyYAML 6.0 yaml/dump().` parses as:

* scheme = `scip-python`
* package triple = `(python, PyYAML, 6.0)`
* descriptors = `namespace yaml/`, `method dump().`

(That method ends in `).` per spec.) ([Docs.rs][5])

---

# 2) Occurrences: the “where” + “what” + “why” in a file

An **Occurrence** associates a source range with:

* an optional **symbol** (`symbol`)
* optional **roles** (`symbol_roles`) (bitset)
* optional **highlighting class** (`syntax_kind`)
* optional **diagnostics** attached to that range
* optional **override documentation** (hover/docs at that range)
* optional **enclosing_range** (important for definitions)

The struct fields are explicitly: `range`, `symbol`, `symbol_roles`, `override_documentation`, `syntax_kind`, `diagnostics`, `enclosing_range`. ([Docs.rs][4])

## 2.1 Range encoding: `[start, end)` with 3-or-4 ints

`Occurrence.range` is a half-open range `[start, end)` encoded as a `repeated int32`, and must be **exactly 3 or 4 elements**:

* 4: `[startLine, startCharacter, endLine, endCharacter]`
* 3: `[startLine, startCharacter, endCharacter]` (endLine inferred = startLine)

Lines/chars are **0-based**, and **character interpretation depends on `Document.position_encoding`**. ([Docs.rs][4])

The schema notes this `repeated int32` was adopted for payload size reductions vs LSP-like nested messages. ([Docs.rs][4])

## 2.2 PositionEncoding (critical for consumers)

The document specifies `position_encoding` and gives guidance:

* JVM/.NET/JS/TS indexers: UTF16 code unit offsets
* Python indexers: **UTF32 code unit offsets**
* Go/Rust/C++ indexers: UTF8 byte offsets ([Docs.rs][3])

**Consumer implication:** if you want to extract text substrings by ranges, you must interpret “character” in the correct unit.

## 2.3 Symbol roles: bitset (definition/import/read/write/etc.)

`symbol_roles` is a bitset of SymbolRole values:
`Definition=1`, `Import=2`, `WriteAccess=4`, `ReadAccess=8`, `Generated=16`, `Test=32`, `ForwardDefinition=64`. ([Docs.rs][7])

## 2.4 Syntax highlighting: `syntax_kind`

`syntax_kind` is an enum with ~39 variants (Keyword, IdentifierFunction, IdentifierType, StringLiteral, NumericLiteral, etc.) intended purely for classification in syntax highlighting. ([Docs.rs][8])

## 2.5 `override_documentation`

`override_documentation` is per-occurrence CommonMark docs; if empty, consumers should fall back to symbol-level docs. It’s intended for cases where the same symbol has richer info at a specific site (e.g., generics with instantiated type args, gradual typing). ([Docs.rs][4])

## 2.6 Diagnostics on occurrences

`Occurrence.diagnostics[]` attaches diagnostics to that specific range. ([Docs.rs][4])

## 2.7 `enclosing_range` (why it exists, and how to use it)

SCIP distinguishes “the token range” (`range`) from a larger “AST node body range” (`enclosing_range`) in some ecosystems. A concrete motivation: scip-python users noted it’s hard to “grab the entire class or method definition because the range doesn’t include the body” and proposed using `enclosing_range` to store full boundaries for definition occurrences. ([GitHub][9])

**Best practice for consumers:**

* Treat `Occurrence.range` as the *selection span* (identifier token / signature token).
* Treat `Occurrence.enclosing_range` (if non-empty) as the *full syntactic span* (e.g., entire def/class body) when extracting or displaying “definition blocks”.

---

# 3) Diagnostics schema (Severity/Tag/code/message/source)

A `Diagnostic` has:

* `severity` (Error/Warning/Information/Hint)
* `code` (optional)
* `message`
* `source` (optional, human-readable—e.g., “typescript”, “super lint”)
* `tags[]` (e.g., Unnecessary, Deprecated) ([Docs.rs][10])

Severity numeric values are standardized (`Error=1`, `Warning=2`, `Information=3`, `Hint=4`). ([Docs.rs][11])
DiagnosticTag currently includes `Unnecessary=1`, `Deprecated=2`. ([Docs.rs][12])

---

# 4) SymbolInformation: docs + signature docs + kinds + relationships

While your focus is symbols/occurrences/diagnostics, **SymbolInformation** is where you get durable metadata for hover/UI/documentation:

* `symbol`: must match the Symbol grammar
* `documentation`: markdown docs (CodeAnatomy keeps this as-is)
* `signature_documentation`: a `Document` containing signature text + optional occurrences; CodeAnatomy
  flattens this into `signature_text`/`signature_language` plus `scip_signature_occurrences`
* `display_name`: UI name; symbol string is not a reliable display name (especially local symbols)
* `kind`: symbol kind (class/method/etc.)—preferred vs inferring from descriptor suffix
* `relationships`: edges to other symbols (implements/type def/definition override)
* `enclosing_symbol`: allows giving an owner for local symbols ([Docs.rs][13])

---

# 5) Python consumer “toolkit” code: ranges, roles, highlighting, diagnostics

Below is a minimal-but-correct pattern for consuming an `index.scip` in Python once you’ve generated `scip_pb2.py` from the SCIP proto (as you’re already doing).

## 5.1 Convert `project_root` URI + read file with correct encoding

`project_root` is URI-encoded absolute path. ([Docs.rs][2])
`text_document_encoding` indicates UTF-8 vs UTF-16 on disk. ([Docs.rs][2])

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse, unquote

# You'll import enums from scip_pb2: TextEncoding, PositionEncoding, etc.

def project_root_to_path(project_root_uri: str) -> Path:
    # Often "file:///..." but spec just says URI-encoded absolute path.
    parsed = urlparse(project_root_uri)
    if parsed.scheme in ("file", ""):
        # file URI: path is percent-encoded
        return Path(unquote(parsed.path))
    # fallback: treat as percent-encoded path-like
    return Path(unquote(project_root_uri))

def decode_file_bytes(raw: bytes, text_encoding) -> str:
    # text_encoding is a TextEncoding enum value
    if text_encoding == 1:  # UTF8
        return raw.decode("utf-8")
    if text_encoding == 2:  # UTF16
        # ambiguous endian; many tools use UTF-16LE/BE with BOM; try BOM-aware first
        return raw.decode("utf-16")
    return raw.decode("utf-8", errors="replace")  # unspecified -> tolerate
```

## 5.2 Normalize SCIP ranges (3-int vs 4-int) and extract substrings respecting PositionEncoding

```python
from typing import NamedTuple

class Range4(NamedTuple):
    start_line: int
    start_char: int
    end_line: int
    end_char: int

def normalize_range(r: list[int]) -> Range4:
    if len(r) == 3:
        sl, sc, ec = r
        return Range4(sl, sc, sl, ec)
    if len(r) == 4:
        sl, sc, el, ec = r
        return Range4(sl, sc, el, ec)
    raise ValueError(f"invalid range length {len(r)}: {r}")

def code_unit_offset_to_py_index(line: str, offset: int, position_encoding: int) -> int:
    """
    Convert a SCIP 'character' offset into a Python string index for a single line.
    position_encoding:
      1 UTF8 byte offsets
      2 UTF16 code unit offsets
      3 UTF32 code unit offsets (Pythonic codepoints)
    """
    if position_encoding == 3:  # UTF32 code units ~= Unicode scalar count; Python str indexing matches
        return offset

    if position_encoding == 1:  # UTF8 byte offset
        b = line.encode("utf-8")
        # bytes[:offset] decode back and take len in codepoints
        return len(b[:offset].decode("utf-8", errors="strict"))

    if position_encoding == 2:  # UTF16 code units
        b = line.encode("utf-16-le")
        # UTF16 code units are 2 bytes each in LE representation
        byte_off = offset * 2
        return len(b[:byte_off].decode("utf-16-le", errors="strict"))

    # unknown -> best effort
    return offset

def slice_document_text(doc_text: str, r: Range4, position_encoding: int) -> str:
    lines = doc_text.splitlines(keepends=True)
    if r.start_line == r.end_line:
        line = lines[r.start_line]
        a = code_unit_offset_to_py_index(line, r.start_char, position_encoding)
        b = code_unit_offset_to_py_index(line, r.end_char, position_encoding)
        return line[a:b]

    # Multi-line slice (rare for identifier tokens, common for enclosing_range)
    out: list[str] = []
    for li in range(r.start_line, r.end_line + 1):
        line = lines[li]
        if li == r.start_line:
            a = code_unit_offset_to_py_index(line, r.start_char, position_encoding)
            out.append(line[a:])
        elif li == r.end_line:
            b = code_unit_offset_to_py_index(line, r.end_char, position_encoding)
            out.append(line[:b])
        else:
            out.append(line)
    return "".join(out)
```

This implements:

* range normalization rules (3 vs 4 ints) ([Docs.rs][4])
* character-unit semantics driven by `Document.position_encoding` / PositionEncoding enum ([Docs.rs][3])

## 5.3 Role bitset helpers (Definition/import/read/write)

```python
def has_role(symbol_roles: int, role_bit: int) -> bool:
    return (symbol_roles & role_bit) != 0

# SymbolRole values:
# Definition=1, Import=2, WriteAccess=4, ReadAccess=8, Generated=16, Test=32, ForwardDefinition=64 :contentReference[oaicite:35]{index=35}
```

## 5.4 Aggregating per-range diagnostics (with tags)

```python
def format_diag(diag) -> str:
    # diag: Diagnostic
    sev = int(diag.severity)  # 1..4
    code = getattr(diag, "code", "")
    src = getattr(diag, "source", "")
    tags = [int(t) for t in getattr(diag, "tags", [])]  # Unnecessary=1, Deprecated=2
    return f"sev={sev} code={code!r} src={src!r} tags={tags} msg={diag.message!r}"
```

Diagnostics fields and enums are defined by the schema. ([Docs.rs][10])

---

# 6) How to interpret `syntax_kind` in downstream tooling

`syntax_kind` is designed for consistent syntax highlighting across indexers. The enum includes:

* `Keyword`
* identifier classes (`Identifier`, `IdentifierFunction`, `IdentifierType`, …)
* literal classes (`StringLiteral`, `NumericLiteral`, `BooleanLiteral`, …)
* punctuation, regex token parts, tag parts, etc. ([Docs.rs][8])

**Practical mapping idea (consumer-side):**

* Treat `SyntaxKind` as an approximate “semantic token type” + “modifier”.
* If you want LSP-style tokens, map:

  * `IdentifierFunctionDefinition` → tokenType=function, modifier=definition
  * `IdentifierFunction` → tokenType=function
  * `IdentifierType` → tokenType=class/type
  * `Keyword` → tokenType=keyword
  * `StringLiteral*` → tokenType=string
  * etc.

(Exact mapping is consumer-specific; SCIP just standardizes the classification labels.)

---

# 7) Enclosing range for “extract the whole definition” (the practical recipe)

When you need the “full definition body” snippet:

1. Use the **definition occurrence** for a symbol (`SymbolRole.Definition`). ([Docs.rs][7])
2. Prefer `occ.enclosing_range` if present; otherwise fall back to `occ.range`. The motivation is explicitly discussed in scip-python context. ([GitHub][9])
3. Slice the document text using the position encoding conversion (§5.2). ([Docs.rs][3])

---

[1]: https://docs.rs/scip/latest/scip/types/struct.Index.html "Index in scip::types - Rust"
[2]: https://docs.rs/scip/latest/scip/types/struct.Metadata.html "Metadata in scip::types - Rust"
[3]: https://docs.rs/scip/latest/scip/types/struct.Document.html "Document in scip::types - Rust"
[4]: https://docs.rs/scip/latest/scip/types/struct.Occurrence.html "Occurrence in scip::types - Rust"
[5]: https://docs.rs/scip/latest/scip/types/struct.Symbol.html "Symbol in scip::types - Rust"
[6]: https://docs.rs/scip/latest/scip/types/struct.Package.html "Package in scip::types - Rust"
[7]: https://docs.rs/scip/latest/scip/types/enum.SymbolRole.html "SymbolRole in scip::types - Rust"
[8]: https://docs.rs/scip/latest/scip/types/enum.SyntaxKind.html "SyntaxKind in scip::types - Rust"
[9]: https://github.com/sourcegraph/scip-python/issues/86?utm_source=chatgpt.com "Definitions should have an enclosing range #86"
[10]: https://docs.rs/scip/latest/scip/types/struct.Diagnostic.html "Diagnostic in scip::types - Rust"
[11]: https://docs.rs/scip/latest/scip/types/enum.Severity.html "Severity in scip::types - Rust"
[12]: https://docs.rs/scip/latest/scip/types/enum.DiagnosticTag.html "DiagnosticTag in scip::types - Rust"
[13]: https://docs.rs/scip/latest/scip/types/struct.SymbolInformation.html "SymbolInformation in scip::types - Rust"

## SCIP CLI workflows: `print`, `print --json`, `snapshot` (inspection + downstream pipelines)

This is the **operational/tooling** layer: how you treat a `.scip` index as a **debuggable artifact** and a **pipeline input**, without writing custom protobuf parsers first.

---

# 1) Installing the `scip` CLI (and pinning it)

The SCIP repo’s README explicitly calls out two installation paths: download a **release binary** or build locally with Go. ([GitHub][1])

### 1.1 Download a pinned release binary (recommended for CI/services)

The releases page includes a portable snippet that chooses OS/ARCH and extracts the `scip` binary from a release tarball. ([GitHub][2])

```bash
env \
  TAG="v0.6.0" \
  OS="$(uname -s | tr '[:upper:]' '[:lower:]')" \
  ARCH="$(uname -m | sed -e 's/x86_64/amd64/')" \
  bash -c 'curl -L "https://github.com/sourcegraph/scip/releases/download/$TAG/scip-$OS-$ARCH.tar.gz"' \
| tar xzf - scip
./scip --help
```

Pin **TAG** in build images; treat `scip` as a compiler toolchain component (version drift changes output shape/fields).

### 1.2 Build locally (useful for hacking / debugging)

```bash
git clone https://github.com/sourcegraph/scip.git --depth=1
cd scip
go build ./cmd/scip
./scip --help
```

([GitHub][1])

---

# 2) `scip print`: the fast path to “what’s in this index?”

The CLI reference describes `scip print` as a debugging command, and it supports:

* **colored textual output** (default),
* a `--json` mode, and
* a `--color` flag (with no effect on JSON).

### 2.1 Human-debug view (text)

```bash
scip print index.scip
```

Use this when you’re answering questions like:

* “Does this index contain *any* occurrences?”
* “Are my symbols namespaced as expected?”
* “Are diagnostics present / exploding?”

### 2.2 Control color deterministically (important for logs)

The CLI exposes `--color` and indicates it affects TTY output (and not JSON).
In CI, you generally want stable output:

```bash
scip print --color=false index.scip
```

### 2.3 `scip print --json`: bridge to “consume without protobuf”

The scip CLI added a `--json` flag to emit JSON instead of colored text. ([GitHub][3])
This is the key workflow when your downstream is Python tooling (or jq) and you want to avoid protobuf codegen.

```bash
scip print --json index.scip > index.json
```

**Important warning:** Sourcegraph maintainers have explicitly noted that converting an index to JSON can be **high overhead** depending on codebase size. ([GitHub][4])
So treat JSON output as:

* great for **debugging**, **small-to-medium repos**, **unit snapshots**, and **selective extraction**,
* potentially expensive for giant monorepos unless you stream-parse (below).

---

## 2.4 Python: stream `scip print --json` without loading it all

If `scip print --json` produces a single huge JSON document (common), `json.load()` can OOM. Prefer a streaming parser (e.g. `ijson`).

```python
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Iterator, Any

def scip_print_json_stream(index_path: Path) -> Iterator[Any]:
    """
    Streams JSON tokens from `scip print --json` without buffering full output.
    Requires: pip install ijson
    """
    import ijson  # type: ignore

    proc = subprocess.Popen(
        ["scip", "print", "--json", str(index_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    try:
        # Example: iterate top-level documents as they stream
        # Adjust the prefix to match the JSON shape emitted by your scip version.
        for doc in ijson.items(proc.stdout, "documents.item"):
            yield doc
    finally:
        proc.stdout.close()
        rc = proc.wait()
        if rc != 0:
            err = (proc.stderr.read() if proc.stderr else "")
            raise RuntimeError(f"scip print failed rc={rc}\n{err}")
```

**Why this matters:** you can build “index sanity” checks (counts, missing fields, symbol format) in seconds without building protobuf bindings.

---

# 3) `scip snapshot`: the “golden test / visual inspection” workflow

## 3.1 What snapshot produces

Snapshotting creates an **annotated copy of your project** (or an annotated view) that is meant to be greppable and diffable. It’s used heavily for regression testing indexers and visually inspecting index quality.

A concrete invocation pattern appears across the ecosystem:

```bash
scip snapshot --from=index.scip --to scip-snapshot
```

([GitHub][5])

After this, developers commonly `rg` for caret annotations like `^ reference` inside the generated snapshot tree. In the scip-ruby issue below, you can see the snapshot output lines contain comment-prefixed caret markers plus “reference … <symbol>” text. ([GitHub][5])

Example snapshot lines (from that issue):

* `//^^^^^ reference ...`
* `//               ^^^^^^^ reference ...`
* etc. ([GitHub][5])

This illustrates the “visual diff” model: you diff text files with inline markers, instead of diffing protobuf blobs.

## 3.2 Control comment syntax for snapshots

The scip CLI supports a flag to customize the **comment syntax** used for snapshot annotations, with default `"//"`. ([GitHub][6])

This matters when you snapshot non-C-like languages:

* Python: `#`
* SQL: `--`
* Lua: `--`
* Haskell: `--`
* HTML/XML: `<!-- -->` (may not be supported; check `--help`)

Pattern:

```bash
scip snapshot --from=index.scip --to out --comment-syntax="#"
```

(Flag name is explicitly mentioned as `--comment-syntax` in the changelog snippet. ([GitHub][3]))

## 3.3 Diagnostics in snapshot output

The scip changelog notes that **snapshot output includes the diagnostics field**. ([GitHub][3])
So snapshots are not just references/definitions—they can also embed diagnostics signals, which is extremely useful when validating index quality (e.g., import resolution errors from an indexer).

---

# 4) “Downstream pipelines” that use `print` + `snapshot` as primitives

## 4.1 Minimal “index quality gate” in CI (no protobuf)

**Goal:** fail fast if an index is obviously broken (empty occurrences, no definitions, etc.)

**Approach:**

1. `scip print --json index.scip`
2. run a few structural assertions on the JSON

Example (streaming, so it scales better):

```python
from __future__ import annotations

from collections import Counter
from pathlib import Path

def assert_index_has_occurrences(index_path: Path) -> None:
    counts = Counter()
    for doc in scip_print_json_stream(index_path):
        # doc is a dict (JSON object) for a document
        counts["docs"] += 1
        occs = doc.get("occurrences", [])
        counts["occurrences"] += len(occs)
        # optionally, count diagnostics embedded in occurrences
        for occ in occs:
            diags = occ.get("diagnostics", [])
            counts["diagnostics"] += len(diags)

    if counts["docs"] == 0:
        raise AssertionError("No documents in SCIP index")
    if counts["occurrences"] == 0:
        raise AssertionError("No occurrences in SCIP index (index likely unusable)")
    # choose your own threshold: some indexers intentionally omit diags
    print("SCIP stats:", dict(counts))
```

This avoids protobuf, avoids local file reads, and can be run in seconds.

## 4.2 Golden snapshots as regression tests (the “snapshot-first” discipline)

**Goal:** treat the snapshot directory as the stable “expected output” in VCS (or in test fixtures), so indexer changes are reviewed via diffs.

Workflow:

```bash
# produce snapshot
scip snapshot --from=index.scip --to out --comment-syntax="#"

# compare against golden
diff -ru golden_snapshot/ out/
```

**When it’s powerful:**

* You’re building/maintaining an indexer (or tuning scip-python config) and want to ensure behavior doesn’t drift.
* You want “reviewable” changes in symbol resolution, references, and diagnostics without decoding protobuf.

## 4.3 “Find the interesting bits” with ripgrep over snapshots

Because snapshot output is text, you can do fast, composable queries:

```bash
rg " definition " -n out/
rg " reference " -n out/
rg " diagnostic " -n out/
```

The scip-ruby issue shows exactly this kind of usage—`rg "\^ reference"` to detect meaningful reference markers. ([GitHub][5])

## 4.4 When to prefer `snapshot` vs `print --json`

* Prefer **snapshot** when:

  * you want *human diff review*,
  * you want to inspect *a particular file’s* annotations directly,
  * you’re writing golden tests.
* Prefer **print --json** when:

  * you want *programmatic inspection* (counts, filtering, mapping into DuckDB/NetworkX),
  * you’re building a downstream ingestion pipeline.
* Prefer **plain `print`** when:

  * you’re in “is this index obviously sane?” mode.

---

# 5) Operational guardrails (what tends to bite teams)

### 5.1 Version pinning is not optional

CLI output shape/fields evolve. Treat `scip` version as part of your “index format contract.” Install pinned tags in CI (see §1.1). ([GitHub][2])

### 5.2 JSON output cost is real

The ecosystem explicitly warns JSON conversion can have heavy overhead on large codebases. ([GitHub][4])
If you need JSON at scale, stream it.

### 5.3 Snapshot comment syntax must match your corpus

If you snapshot Python, set `--comment-syntax="#"` or you’ll generate invalid-looking lines in `.py` files. The CLI supports customizing comment syntax (default `//`). ([GitHub][6])

---

[1]: https://github.com/sourcegraph/scip "GitHub - sourcegraph/scip: SCIP Code Intelligence Protocol"
[2]: https://github.com/sourcegraph/scip/releases "Releases · sourcegraph/scip · GitHub"
[3]: https://github.com/sourcegraph/scip/blob/main/CHANGELOG.md?utm_source=chatgpt.com "scip/CHANGELOG.md at main · sourcegraph/scip"
[4]: https://github.com/sourcegraph/scip/issues/178?utm_source=chatgpt.com "Using index.scip locally? · Issue #178 · sourcegraph/scip"
[5]: https://github.com/sourcegraph/scip-ruby/issues/87 "No meaningful occurrences for rubocop repository · Issue #87 · sourcegraph/scip-ruby · GitHub"
[6]: https://github.com/sourcegraph/scip/blob/main/docs/CLI.md?utm_source=chatgpt.com "scip/docs/CLI.md at main · sourcegraph/scip"

Below is a **gap-fill addendum** that you can append to your existing `scip_python_overview.md` (), written in the same “dense technical guide” style as the attached template ().

---

# Addendum: contract-complete scip-python + SCIP CLI (upload contract, CLI surfaces, spec appendices, env.json generation)

### Mental model: treat scip-python + scip + src as a **3-part toolchain**

You don’t have “a CLI” — you have a versioned **compiler toolchain**:

1. **scip-python**: builds `index.scip` from Python source using Pyright-class semantic analysis (precise nav depends on dependency + typing resolution). ([Sourcegraph][1])
2. **scip (SCIP CLI)**: validates/prints/snapshots/converts indexes; it is your “object inspection + golden testing” layer. ([GitHub][2])
3. **src code-intel upload**: binds an `index.scip` to a *repo@commit + root* in Sourcegraph; most brittleness is here (auth + root inference). ([GitHub][3])

The stable contract surface is: **(a) pinned versions, (b) `--help` outputs, (c) the protobuf schema / bindings**, and (d) golden snapshot outputs.

---

## A) Version pinning + “CLI contract snapshots” (mandatory for drift resistance)

### A1) Pin `scip` (SCIP CLI) by release tag (don’t “latest”)

The GitHub releases page provides prebuilt binaries for multiple platforms (including **darwin-arm64** for Apple Silicon). ([GitHub][4])

**Operational contract**:

* Record the exact `TAG` you deploy (e.g. `v0.6.1`). ([GitHub][4])
* Check in a copy of `scip --help` and `scip <subcmd> --help` output in-repo.

**Why**: `scip` is actively evolving (e.g., new flags like `print --json`, snapshot options, test command additions). ([GitHub][2])

### A2) Pin `scip-python` (indexer) as a toolchain component

scip-python is a fork/addition on top of Pyright (not a “small wrapper”), but the repo itself notes there are **no substantial changes to the `pyright` library** “at this time” — meaning behavior is largely Pyright-driven and config-driven. ([GitHub][5])

**Contract pattern**:

* Always capture:

  * `scip-python --version`
  * `scip-python index --help`
  * The exact Pyright config used for the run (resolved config path + contents)
* Treat these as *golden artifacts*.

### A3) Suggested repo artifacts (minimal but decisive)

Store these under something like `tooling/scip/contracts/`:

* `scip.version.txt`
* `scip.help.txt`
* `scip.help.print.txt`
* `scip.help.snapshot.txt`
* `scip.help.convert.txt`
* `scip-python.version.txt`
* `scip-python.help.index.txt`
* `src.help.code-intel-upload.txt`

Then add a CI check: **diff must be empty unless intentionally bumped**.

---

## B) Sourcegraph upload contract (src-cli): auth, root, commit identity

### B1) Authentication + endpoint selection (don’t handwave this)

At minimum, `src` expects:

* `SRC_ACCESS_TOKEN`
* `SRC_ENDPOINT`
  Example instructions appear in multiple indexer docs (e.g., scip-dotnet). ([NuGet][6])

**Contract rule**: your CI must set these explicitly; do not depend on interactive login.

### B2) The `-root` inference footgun (must be explicitly handled)

`src code-intel upload` currently infers the index root as the directory where the SCIP file lives, and there is an explicit feature request to infer root from SCIP metadata instead. ([GitHub][3])

**Practical rule**:

* Always pass `-root <repo-root>` (or the equivalent root flag in your `src` version) when:

  * `index.scip` is emitted outside repo root (common in CI workspaces),
  * you stage artifacts into a separate directory,
  * you build indexes in a “dist/” folder.

This avoids silent “indexed paths don’t match repo paths” failures (the most common “upload succeeded but navigation is broken” symptom).

### B3) Commit identity constraints in CI

The upload path often expects a Git commit hash; `src-cli` issues show backend validation behavior around commits (e.g., requiring a 40-char revhash). ([GitHub][7])

**Rule**: in CI, always bind the upload to:

* repo identifier
* commit SHA
* root path
  …and do *not* rely on inference if reproducibility matters.

---

## C) SCIP CLI: the “inspection + golden testing” layer (what to use it for)

### C1) `scip print`: deterministic introspection, now also machine-readable

The SCIP changelog notes `scip print` supports `--json` output (vs colored text), and also supports disabling colors via `--color=false` or env config. ([GitHub][2])

**Practical usage**:

* Use `--json` for pipeline tooling (e.g., extract counts, verify invariants).
* Use colorless output for golden snapshots in CI.

### C2) `scip snapshot`: visual + diffable “semantic goldens”

The CLI reference explicitly positions `snapshot` as generating snapshot files to inspect an index visually. ([GitHub][8])
The changelog also indicates snapshot output has evolved (e.g., fields like diagnostics appearing in snapshot output). ([GitHub][2])

**Contract pattern**:

* snapshot outputs belong in your test corpus (golden diff).
* use snapshot to validate **relationships**, **token kinds**, **document coverage**, and **diagnostics** stability across toolchain bumps.

### C3) `scip convert` (SCIP→LSIF): compatibility bridge

GitLab docs explicitly state they don’t natively support SCIP and recommend converting to LSIF via the SCIP CLI. ([GitLab Docs][9])
Your base doc already references SCIP→LSIF conversion and experimental conversions. 

**Important constraint**: conversion can impose semantic limitations; treat “SCIP as ground truth”, LSIF as a derived artifact.

### C4) `scip test`: targeted conformance harness (emerging, high leverage)

The scip changelog indicates a `test` command exists and has gained flags like `--check-documents`. ([GitHub][2])
There’s also active discussion about extending this into a tree-sitter-style test workflow. ([GitHub][10])

**Design implication**: `scip test` is converging toward being the canonical “spec conformance + regression” runner for SCIP producers (indexers) and consumers.

### C5) “Last resort” binary decode for corruption triage

Sourcegraph support docs provide the direct protobuf decode approach:

```bash
cat index.scip | protoc --decode=scip.Index scip.proto
```

Useful when the index is corrupted or upload fails and you need raw confirmation of metadata/documents. ([Sourcegraph Help Center][11])

---

## D) SCIP schema appendices (consumer-grade completeness)

### D1) `SymbolRole` bitset: full known role surface + how to read it

`SymbolRole` is explicitly a **bitset**; roles include (with documented semantics in schema/bindings):

* `Definition = 0x01`
* `Import = 0x02`
* `WriteAccess = 0x04`
* `ReadAccess = 0x08`
* `Generated = 0x10`
* `Test = 0x20`
* `ForwardDefinition = 0x40` ([GitHub][12])

Consumer logic must treat this as a bitmask, not an enum:

```python
is_import = (roles & IMPORT) != 0
```

Docs.rs also frames it exactly this way: check bits via `role.value & SymbolRole.Import.value`. ([Docs.rs][13])

### D2) `SyntaxKind`: token classification (current canonical list + extensibility)

A canonical list of `SyntaxKind` constants (from generated TS bindings discussion) includes:

* Comment
* PunctuationDelimiter, PunctuationBracket
* Identifier* family: Keyword/Operator/Builtin/Null/Constant/MutableGlobal/Parameter/Local/Shadowed/Module/Function/FunctionDefinition/Macro/MacroDefinition/Type/BuiltinType/Attribute
* Regex* family: Escape/Repeated/Wildcard/Delimiter/Join
* Literals: StringLiteral/StringLiteralEscape/StringLiteralSpecial/StringLiteralKey/CharacterLiteral/NumericLiteral/BooleanLiteral
* Tags: Tag/TagAttribute/TagDelimiter ([GitHub][14])

**Forward-compat rule**: treat `SyntaxKind` as open-ended; changelog entries mention adding new “Kind enum constants” over time. ([GitHub][2])

### D3) `Diagnostic`: exact fields + semantics (what to trust, what’s optional)

The Diagnostic record is defined with the following primary fields:

* `severity` (error/warning/info/hint semantics)
* `code` (optional UI-facing code)
* `message` (required human text)
* `source` (optional producer label, e.g. “typescript”, “super lint”)
* `tags` (enum-valued tags list) ([Docs.rs][15])

**Consumer guidance**:

* Always display `message`.
* Treat `code`/`source` as optional (empty string is common).
* Treat `tags` as hints, not invariants (producers differ).
* Treat unknown enum values as “unknown” (don’t crash).

### D4) `Relationship`: the real basis of “find implementations” and override semantics

`Relationship` is not a “kind enum”; it’s a tuple of a **target symbol** plus boolean relationship flags:

* `symbol` (target)
* `is_reference` (Find references)
* `is_implementation` (Find implementations) — explicitly *not always coupled* with references
* `is_type_definition` (Go to type definition)
* `is_definition` (override go-to-definition/reference behavior; special constraints exist) ([Docs.rs][16])

Critical nuance (from bindings docs):

* It’s common for `is_implementation` and `is_reference` to both be true, but not required; implementors may be excluded from references on purpose. ([Docs.rs][16])
* `is_definition` can override def/ref behavior for symbols without a single canonical definition or with multi-definition semantics, but **SCIP→LSIF conversion currently only records `is_definition` reliably for global symbols** (locals may be dropped). ([Docs.rs][16])

### D5) Consumer algorithm sketches (deterministic, index-local)

**Find implementations for symbol S**:

* iterate all `SymbolInformation` records
* for each symbol T, check `relationships` for entries where `rel.symbol == S` and `rel.is_implementation == true`
* return T as implementor set

**Go to type definition for symbol S**:

* locate `SymbolInformation(S)`
* follow `relationships` where `is_type_definition == true`
* resolve those symbols to definition locations (via their `occurrences`/ranges)

**Find references for symbol S**:

* canonical: occurrences where `occurrence.symbol == S` and role has `ReadAccess`/`WriteAccess` (bitmask checks) ([GitHub][12])
* augmented: follow `relationships` targeting S with `is_reference == true` to include synthetically linked symbols (language-specific semantics) ([Docs.rs][16])

---

## E) Environment JSON (`--environment`) generation playbooks (pip/poetry/conda/editable)

### E1) Mental model: env.json is a **site-packages relative distribution manifest**

The overview doc’s format is:

```json
[
  {
    "name": "PyYAML",
    "version": "6.0",
    "files": [
      "PyYAML-6.0.dist-info/INSTALLER",
      ...
    ]
  }
]
```

`files` are **paths relative to the environment’s site-packages directory**, including dist-info metadata. 

scip-python uses this to **avoid invoking pip** and to stabilize dependency resolution for indexing. 

### E2) Canonical generator strategy (works for pip + poetry venvs; often works for conda)

Use `importlib.metadata` as the ground truth of installed distributions:

* enumerate distributions (`importlib.metadata.distributions()`)
* for each distribution:

  * `name`, `version`
  * file list from `dist.files` (these are already relative-ish paths)
* normalize each file entry to a POSIX-ish relative path under site-packages
* emit JSON list sorted by (name, version) for determinism

**Why this is the right primitive**:

* It reflects what Python import resolution sees.
* It captures editable installs *when they expose dist-info*; when they don’t, you detect and handle separately.

### E3) Conda edge cases (what breaks, how to detect)

Conda environments may include packages that don’t present standard `dist-info` layouts (or present them differently). Your generator should:

* log distributions where `dist.files` is `None` or empty
* optionally fallback to:

  * locate module root via `import <module>; module.__file__`
  * include that package directory + any `.dist-info` that exists
* treat these as “indexing degraded” if file lists are incomplete (expect missing symbol resolution for those deps).

### E4) Editable installs (PEP 660 / legacy `.pth`)

Editable installs often manifest as:

* `direct_url.json` inside dist-info
* `.pth` files that inject paths at runtime

**Policy**:

* If `dist.files` includes only metadata but not the source tree, add the editable source root explicitly (otherwise scip-python may not index the dependency sources even though imports resolve at runtime).
* Detect this by comparing:

  * importable module file path
  * presence/absence of package files in `dist.files`

### E5) Determinism requirements (for CI + golden tests)

* normalize path separators to `/`
* sort `files` lexicographically
* sort package records by `(name.lower(), version)`
* record the site-packages base path used during generation (so consumers can reproduce)

---

## F) Auto-indexing semantics: project identity is a cross-repo linking contract

### F1) `--project-name` + `--project-version` are not optional “labels”

Precise navigation “works” locally without strict identity, but cross-repo linking depends on stable project identity. scip-python’s issue tracker explicitly discusses improving **project name and version detection** for auto-indexing and cross-repo navigation. ([GitHub][17])

**Policy**:

* `project-name`: stable repo identifier (often matches Sourcegraph repo name)
* `project-version`: immutable revision identifier (commit SHA or release tag)
* `project-namespace`: stable org/host grouping (when needed)

### F2) Practical “best defaults” for CodeIntel-like systems

If you want determinism across:

* local runs
* CI runs
* multi-repo graphs

then default to:

* `project-version = git rev-parse HEAD` (full SHA, not short)
* `project-name = canonical repo slug`
* `root = repo root` (and pass that root explicitly to uploader, per §B)

---

That covers the missing surfaces called out in the gap analysis, with the schema/relationship/diagnostic sections now “consumer complete,” and with upload + env.json treated as first-class contracts rather than incidental CLI usage.

[1]: https://sourcegraph.com/blog/scip-python?utm_source=chatgpt.com "scip-python: a precise Python indexer"
[2]: https://github.com/sourcegraph/scip/blob/main/CHANGELOG.md?utm_source=chatgpt.com "scip/CHANGELOG.md at main · sourcegraph/scip"
[3]: https://github.com/sourcegraph/src-cli/issues/809?utm_source=chatgpt.com "[feature request] infer index root by reading scip metadata"
[4]: https://github.com/sourcegraph/scip/releases "Releases · sourcegraph/scip · GitHub"
[5]: https://github.com/sourcegraph/scip-python?utm_source=chatgpt.com "sourcegraph/scip-python: SCIP indexer for Python"
[6]: https://www.nuget.org/packages/scip-dotnet/0.2.10?utm_source=chatgpt.com "scip-dotnet 0.2.10"
[7]: https://github.com/sourcegraph/src-cli/issues/1035?utm_source=chatgpt.com "Support Perforce changelists in src code-intel upload #1035"
[8]: https://github.com/sourcegraph/scip/blob/main/docs/CLI.md?utm_source=chatgpt.com "scip/docs/CLI.md at main · sourcegraph/scip"
[9]: https://docs.gitlab.com/user/project/code_intelligence/?utm_source=chatgpt.com "Code intelligence"
[10]: https://github.com/sourcegraph/scip/issues/235?utm_source=chatgpt.com "`scip test` command · Issue #235 · sourcegraph/scip"
[11]: https://help.sourcegraph.com/hc/en-us/articles/15045932124941-Decoding-SCIP-index-file?utm_source=chatgpt.com "Decoding SCIP index file"
[12]: https://github.com/sourcegraph/scip/blob/main/scip.proto?utm_source=chatgpt.com "scip/scip.proto at main · sourcegraph/scip"
[13]: https://docs.rs/scip/latest/scip/types/index.html?utm_source=chatgpt.com "scip::types - Rust"
[14]: https://github.com/sourcegraph/scip/issues/39?utm_source=chatgpt.com "Doc comments for generated TypeScript bindings · Issue #39"
[15]: https://docs.rs/scip/latest/scip/types/struct.Diagnostic.html?search= "Diagnostic in scip::types - Rust"
[16]: https://docs.rs/scip/latest/scip/types/struct.Relationship.html "Relationship in scip::types - Rust"
[17]: https://github.com/sourcegraph/scip-python/issues/109?utm_source=chatgpt.com "Improve project name and version detection · Issue #109"
