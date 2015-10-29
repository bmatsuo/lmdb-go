# Contributing

The lmdb-go is grateful for any outside contributions.  The simplest way to
contribute is to comment on [open
issues](https://github.com/bmatsuo/lmdb-go/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc)
that are important to you.  This is a critical component in ensuring that
lmdb-go evolves in ways that are best for everyone.

## New Issues

**IMPORTANT**: Do not open a new issue if the problem is already being
addressed in another open issue.  Use Github's search features to find relevant
open issues and add relevant new input there if necessary.

If there is a problem with lmdb-go that is not being addressed in an open issue
then a new issue should be
[opened](https://github.com/bmatsuo/lmdb-go/issues/new).  All users are
encouraged to file issues when they encounter behavior that is incorrect or
inconsistent.  Proposals for new or expanded features/behavior are also welcome
and will be considered. But such proposals must adequately describe the problem
they are addressing to be considered.

Be as specific as possible when about the problem when filing an issue.  That
said, there is some baseline information that must be provided in the issue
description.

-  Description of the problem.  Describing only the problem symptoms is fine.
   But be as specific as possible so that another user could reproduce the
   error.

-  What os/arch does the problem affect (if applicable)? For example, "darwin/amd64" or "OS X".

## Code Contributions

Pull requests are welcome but there are some rules and guidelines for
contributors to follow.

1.  Open an issue first. Anything beyond simple errors, such as errors in
    spelling and grammar, must have a corresponding issue created beforehand.
    See previous sections for information about opening new issues.

2.  Comments in pull requests must be restricted to code review.  Any
    discussion about design or overall merit must take place in the corresponding
    issue.

3.  All contributions must pass tests and format/style checks in the Makefile,
    invoked with the command

```
make all
```
