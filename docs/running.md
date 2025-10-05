# Running a Y-Sweet server

The [quickstart](https://docs.jamsocket.com/y-sweet/quickstart) guide provides instructions for using hosted Y-Sweet on Jamsocket, but if you prefer to host it on your own you have several options.

## Running a dev server

If you have `npm`, the fastest way to run a local server is with `npx`:

```bash
npx y-sweet@latest serve
```

This will download the Y-Sweet server if you do not already have it, and run it.

By default, `y-sweet serve` does not write data to disk. You can specify a directory to persist data to, like this:

```bash
npx y-sweet@latest serve /path/to/data
```

If the directory starts with `s3://`, Y-Sweet will treat it as an S3-compatible bucket path. In this case, Y-Sweet will pick up your local AWS credentials from the environment. If you do not have AWS credentials set up, you can set them up with `aws configure`.

## Snapshots

Y-Sweet supports versioned snapshots, allowing you to create point-in-time backups of your documents and restore them when needed.

### Enabling snapshots

Snapshots can be configured when starting the server:

```bash
npx y-sweet@latest serve /path/to/data \
  --snapshot-enable \
  --snapshot-interval-seconds 3600 \
  --snapshot-max-snapshots 24
```

This configuration will:
- Create a snapshot automatically every hour (`--snapshot-interval-seconds 3600`)
- Keep the most recent 24 snapshots (`--snapshot-max-snapshots 24`)
- Automatically delete older snapshots when the limit is reached

You can also configure snapshots via environment variables:
- `Y_SWEET_SNAPSHOT_ENABLE=true`
- `Y_SWEET_SNAPSHOT_INTERVAL_SECONDS=3600`
- `Y_SWEET_SNAPSHOT_MAX_SNAPSHOTS=24`

### Manual snapshot management

In addition to automatic snapshots, you can create and manage snapshots manually via the HTTP API.

#### Create a snapshot

```bash
curl -X POST http://localhost:8080/doc/{docId}/snapshots \
  -H "Authorization: Bearer YOUR_TOKEN"
```

Response:
```json
{
  "timestamp": 1699123456
}
```

#### List snapshots

```bash
curl http://localhost:8080/doc/{docId}/snapshots \
  -H "Authorization: Bearer YOUR_TOKEN"
```

Response:
```json
{
  "snapshots": [
    {
      "timestamp": 1699123456,
      "size": 12345,
      "hash": 0
    }
  ]
}
```

#### Restore from a snapshot

```bash
curl -X POST http://localhost:8080/doc/{docId}/snapshots/1699123456 \
  -H "Authorization: Bearer YOUR_TOKEN"
```

Response:
```json
{
  "restored": 1699123456,
  "note": "All clients have been disconnected and must reconnect to sync the restored state"
}
```

**Important**: When a snapshot is restored, the server closes all WebSocket connections for that document and evicts it from memory. This forces all connected clients to reconnect, at which point they will receive the restored document state through the normal Yjs sync protocol. This behavior is necessary because Yjs CRDTs merge concurrent updates rather than replacing state - a full reload ensures clients receive the exact snapshot state without merging it with their current state.

#### Delete a snapshot

```bash
curl -X DELETE http://localhost:8080/doc/{docId}/snapshots/1699123456 \
  -H "Authorization: Bearer YOUR_TOKEN"
```

Response:
```json
{
  "deleted": 1699123456
}
```

### Storage layout

Snapshots are stored alongside your document data:

```
doc_id/
├── data.ysweet                     # Current document state
└── snapshots/
    ├── snapshot.1699123456.ysweet  # Snapshot at timestamp
    ├── snapshot.1699127056.ysweet
    └── snapshot.1699130656.ysweet
```

This layout works with both filesystem and S3-compatible storage backends.

## Deploying to Jamsocket

Run the Y-Sweet server on [Jamsocket's session backends](https://jamsocket.com/y-sweet). Check out the [quickstart](https://docs.jamsocket.com/y-sweet/quickstart) guide to get up and running in just a few minutes.

## Docker Image

The latest Docker image is available as `ghcr.io/jamsocket/y-sweet:latest`. You can find a [list of images here](https://github.com/jamsocket/y-sweet/pkgs/container/y-sweet).
