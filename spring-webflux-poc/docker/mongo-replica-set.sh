#!/usr/bin/env bash
# Starts mongod as a single-node replica set whose member is "mongodb:27017".
#
# Why not "localhost:27017": a client that discovers the replica set (replicaSet=rs0) reconnects to the
# member host it is given. From another container, "localhost" is that container itself, so the
# connection fails. "mongodb" is the compose service name, resolvable on the compose network.
# Trade-off: from the host, use directConnection=true (the name "mongodb" does not resolve there).
set -euo pipefail

mongod --replSet rs0 --bind_ip_all &
MONGOD_PID=$!

until mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1; do sleep 1; done

mongosh --quiet --eval '
  const member = "mongodb:27017";
  try {
    const cfg = rs.conf();
    if (cfg.members.length !== 1 || cfg.members[0].host !== member) {
      // Volume created by an older setup (member "localhost:27017"): repoint it.
      cfg.members = [{ _id: 0, host: member }];
      cfg.version += 1;
      rs.reconfig(cfg, { force: true });
      print("replica set reconfigured with " + member);
    }
  } catch (e) {
    if (e.codeName !== "NotYetInitialized") throw e;
    rs.initiate({ _id: "rs0", members: [{ _id: 0, host: member }] });
    print("replica set initiated with " + member);
  }
'

wait "$MONGOD_PID"
