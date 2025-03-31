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
