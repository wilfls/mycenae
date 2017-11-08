#!/bin/bash

sed -ri 's/^([# ]+)?('"authenticator"':).*/\2 '"PasswordAuthenticator"'/' /etc/scylla/scylla.yaml
sed -ri 's/authorizer:.*/authorizer: "CassandraAuthorizer"/' /etc/scylla/scylla.yaml

exec /docker-entrypoint.sh "scylla" "-f" "$@"