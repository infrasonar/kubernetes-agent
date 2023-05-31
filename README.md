[![CI](https://github.com/infrasonar/kubernetes-agent/workflows/CI/badge.svg)](https://github.com/infrasonar/kubernetes-agent/actions)
[![Release Version](https://img.shields.io/github/release/infrasonar/kubernetes-agent)](https://github.com/infrasonar/kubernetes-agent/releases)

# InfraSonar Kubernetes agent

## Environment variables

Environment                 | Default                       | Description
----------------------------|-------------------------------|-------------------
`TOKEN`                     | _required_                    | Token to connect to.
`ASSET_ID`                  | _required_                    | Asset Id _or_ file where the Agent asset Id is stored _(must be a volume mount)_.
`IN_CLUSTER`                | `1`                           | For when this agent is started on a pod inside the cluster, 0 _(=disabled)_ or 1 _(=enabled)_.
`API_URI`                   | https://api.infrasonar.com    | InfraSonar API.
`CHECK_INTERVAL`            | `300`                         | Interval for the kubernetes check in seconds.
`VERIFY_SSL`                | `1`                           | Verify SSL certificate, 0 _(=disabled)_ or 1 _(=enabled)_.
`LOG_LEVEL`                 | `warning`                     | Log level _(error, warning, info, debug)_.
`LOG_COLORIZED`             | `0`                           | Log colorized, 0 _(=disabled)_ or 1 _(=enabled)_.
`LOG_FMT`                   | `%y%m...`                     | Default format is `%y%m%d %H:%M:%S`.
