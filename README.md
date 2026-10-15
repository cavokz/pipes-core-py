# About

Elastic Pipes Core defines a simple composition system in Python. The
components, named _pipes_, are executed in sequence and a _state_ is
passed through from one to the next.

It looks like how UNIX pipes allow composing independent tools on the
command line; indeed you can invoke Elastic Pipes in such way. Differently
from UNIX pipes though, each component adds new content to the state which
is otherwise passed down in the sequence as-is.
