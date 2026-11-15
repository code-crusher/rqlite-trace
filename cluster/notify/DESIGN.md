# Raft-event notifications: DESIGN

## Overview

This package implements notifications for Raft-related events, most notably
leadership changes. When the leadership status of the node running the Service
changes, the Service POSTs a small JSON payload to a configured HTTP(S)
endpoint. This allows logic running on top of rqlite to learn that its node
has become, or ceased to be, the Leader, without polling the cluster and
incurring the associated read penalty. See rqlite issue #1549.

The store has long had the ability to inform internal components about
leadership changes: `Store.RegisterLeaderChange` registers a channel which
receives a boolean whenever a `raft.LeaderObservation` indicates the
leadership of this node has changed. The CDC service is the main existing
consumer. This package is simply the externally-visible counterpart: it
connects that same channel to a user-configured HTTP endpoint.

## Operation

The Service is enabled by setting the `notify-endpoint` flag to an HTTP(S)
URL, or to the path of a JSON configuration file. If not set, the Service is
not started and node startup proceeds exactly as before.

On startup the Service registers a leader-change channel with the Store. When
the leadership status of this node changes, this payload is POSTed to the
endpoint:

```json
{
    "node_id": "node1",
    "leader": true,
    "timestamp": "2026-11-15T10:00:00Z"
}
```

`leader` is `true` if this node is now the Leader. `timestamp` is the UTC time
at which this node observed the change. `node_id` is the ID of the node, so
endpoints receiving notifications from multiple nodes can tell them apart.

Notifications are retried until they are successfully sent, or dropped if
sending fails. Notification delivery is not durable: if the node terminates
before a notification is sent, the notification is lost. The endpoint should
treat notifications as a convenience, not a guarantee, and confirm actual
cluster state through the rqlite API when in doubt (e.g. `/status` or the
`is_leader` status key). Unlike CDC events, leadership changes are
continuously re-announced: any subsequent leadership change (including one
that restores the previous state) triggers a new notification, so a lost
notification self-heals on the next change.

Delivery is asynchronous and never blocks the store: like CDC, the Service
consumes leadership changes from a buffered channel.

## Usage

```
rqlited -notify-endpoint http://myapp:8080/leader_changes ~/node
```
