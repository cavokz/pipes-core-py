## v0.7.0 - Oct 1, 2025

* Allow the import pipe to lazy import data from ndjson files so to allow
  incremental documents loading.
* Allow referring to environment variables from a pipes script.
* Improve the formatting of the `elastic-pipe new-pipe` generated pipe.
* Add switch to make stand-alone pipes describe themselves.
* Add switch to make pipes scripts describe themselves.
* Accept null `api_key` and `username`/`password` in the stack configuration.
* Make import/export pipes expand the user directory.
* Make import/export pipes base relative file paths onto the caller current
  directory, not the script's one.
* Detect unknown config nodes.
* Establish a context common to all the pipes (ex. for setting logging level).
* Prevent state nodes in the `runtime.` tree from being indirectable.
* Add BuildKite pipelines for testing also with internal resources.
* HCP Vault: add `token-file` configuration to the read and write pipes.

## v0.6.0 - May 1, 2025

* Make of Pipe.Context a real Python context manager, it can be used to
  acquire/release resources before/after the pipe function is executed.
* Unify standalone and runner logging setup.
* Make config bindings always mutable, rebind those pointing to some
  state node so to keep any change local to the own pipe configuration.
* Optionally provide an ExitStack to the pipe function so to allow
  clean up activities when the whole pipes script completes.
* At context binding time, check immediately the parameters. This
  is to early detect nodes presence and type.
* Rename what was called "interactive mode" into "UNIX pipe mode"
  and use `-p` instead of `-i` to activate it (activated by default
  when the pipe is invoked as part of a UNIX pipe chain).
* Provide facilities to add help and notes to the pipes, this allow
  pipes to self-document when invoked from terminal.
* Add the above annotations to the `core.import`, `core.export`,
  `hcp.vault.read` and `hcp.vault.write` pipes.

## v0.5.0 - Apr 17, 2025

* Allow appending `@` to config nodes names to indicate that the value
  shall be read from another state node.
  For example, configuration `name: user` would result in `user` to be
  used as value whereas `name@: user` would use whatever value is stored
  at state node `user`.
* Add `Pipe.Context` to struct with more freedom the bindings with state and
  config nodes.
* Drop `Pipe.State`'s `setdefault` option, now you have to put empty
  entries in the state before populating them.
* Disallow mutable values (ex. lists, dictionaries) as state/config
  default values, use `None` for these and update the pipe's logic
  accordingly.
* Drop `Pipe.get_es` and `Pipe.get_kb`, use `util.get_es_client` and
  `util.get_kb_client`; pass a stack value from the config/state.
* Add HCP Vault read & write pipes.

## v0.4.0 - Mar 31, 2025

* Add helpers `util.get_es_client` and `util.get_kb_client`,
  they get the stack configuration from arbitray nodes.
* Rework the node helpers and add unit tests.
* Allow the pipe function to omit the Pipe parameter.
* Allow the pipe function to have a Logger parameter.
* Add publishing to PyPI.

## v0.3.0 - Mar 06, 2025

* Use type annotations to define pipes' input/output.
* Add accessing command line arguments from the pipes.
* Allow configuring the pipes search path.
* Small reorganization of the package.
* Add unit tests for all the new code and part of the old one.
* New principle: spitting exceptions to the user is always a
  bug. If the error is due to the user, a more friendly and
  informative message shall be given instead.

## v0.2.0 - Feb 20, 2025

* Make import/export pipes use stdin/stdout by default.
* Add json and ndjson as import/export formats.
* Allow `run` sub-command read the configuration from stdin.
* Add `version` sub-command to print the version.
* The `new` sub-command is renamed to `new-pipe`.
* Allow setting the logging level from command line.
* Make stand alone pipes invocations use Typer.
* Improve user friendliness when reading from the console,
  the user is warned when they are being waited for input.
* Add a minimum required version check on the configuration so to
  reject configurations too new for the installed version.
* Run CI tests also on macOS and Windows.

## v0.1.0 - Jan 30, 2025

First release. What you can do with it:

* implement pipes
* run them as UNIX pipes on the command line
* run them as a sequence configured in a yaml file
