# Documentation

This directory contains the Sphinx documentation for Elastic Pipes Core.

## Building Locally

1. Install documentation dependencies:

```bash
pip install -r requirements.txt
```

2. Build the HTML documentation:

```bash
make html
```

3. Open the built documentation:

```bash
open _build/html/index.html
```

## Building on ReadTheDocs

The documentation is automatically built on ReadTheDocs when commits are pushed.
Configuration is in `.readthedocs.yaml` at the repository root.

## Structure

- `conf.py` - Sphinx configuration
- `index.rst` - Documentation home page
- `getting-started.rst` - Getting started guide
- `user-guide.rst` - User guide with examples
- `api-reference.rst` - API reference (auto-generated from docstrings)
- `requirements.txt` - Documentation build dependencies
