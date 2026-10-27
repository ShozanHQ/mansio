# Security policy

## Supported versions

mansio has no release yet. The supported version is `main`: a fix lands there.
When there is a first release, this page will say which releases get fixes.

## Reporting a vulnerability

Report it privately, through GitHub's private vulnerability reporting:
<https://github.com/ShozanHQ/mansio/security/advisories/new>.

Do not open a public issue, pull request or discussion for a vulnerability, and
do not post one anywhere else before it is fixed.

## What to include

- What is affected: the commit or version, and the part (the HTTP API, the CLI,
  the MCP server, credentials, the Iggy backend, the compose stack or the image).
- How to reproduce it: the requests or commands, the settings that matter, and
  whether it needs a credential and which scopes it has.
- What it allows: reading, writing or acting as another principal, a tenant
  boundary crossed, a lease or an alert taken over, a denial of service.
- Anything you know about a fix.

Test only against a mansio you run yourself, with data of your own.

A flaw in Apache Iggy itself belongs with the Iggy project. Tell us as well if
the way mansio uses Iggy makes it worse.

## What to expect

- Your report is acknowledged, and you are kept informed of what we find and
  what we plan to do, until there is a fix or a decision not to make one.
- If we do not take it to be a vulnerability, you get an answer that says why.
- You have a say in the timing of disclosure.
- A fix on `main` and a published advisory that credits you, if you want to be
  credited.
