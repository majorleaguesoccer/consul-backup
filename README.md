# Consul Backup and Restore tool.

This will use consul api to recursively backup and restore all your
key/value pairs in JSON format.

a `go build` will generate executable named `consul-backup`

Usage:
`consul-backup [-restore] [-f] <filename>`

filename can be a local path, or an S3 url of the format `s3://bucket/path`. 
Make sure to set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.
