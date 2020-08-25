# teleport-prefect

The `prefect` plugin for Teleport; run your Teleport tasks on Prefect.io's workflow orchestrator.

## Prerequisites

This plugin depends on:

* `python3`
* Python libraries: `prefect`

## Installation

From your Pad directory:

```
teleport plugin install prefect
```

If you use's Teleport's secrets management, you have 2 options for setting the Teleport secret key:

1. (Preferred) Use Prefect's [Secrets](https://docs.prefect.io/orchestration/concepts/secrets.html#setting-a-secret) feature to create a secret named `"TELEPORT_SECRET_KEY"`

2. Set the TELEPORT_SECRET_KEY environment variable in your shell when using this plugin

## Usage

```
teleport plugin -- prefect [-h] [-f] [-p]

Deploy your Teleport Pad to Prefect

optional arguments:
  -h, --help     show this help message and exit
  -f, --force    delete and re-create all flows
  -p, --preview  perform a dry-run to see log output without applying any
                 updates
```