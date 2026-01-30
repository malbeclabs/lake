# Slack Bot Setup

## Create a Slack App

1. Go to [api.slack.com/apps](https://api.slack.com/apps) and click **Create New App** → **From an app manifest**.
2. Select a workspace to develop it in, then paste the contents of [`slack-app-manifest.json`](slack-app-manifest.json).
3. Replace `<your-domain>` in the request URL and redirect URL with your actual domain.
4. Click **Create**.

This sets up all scopes, event subscriptions, and bot configuration automatically. The rest of this doc explains each setting if you need to modify them.

## Configure Scopes

Go to **OAuth & Permissions** → **Bot Token Scopes** and add:

| Scope | Purpose |
|-------|---------|
| `app_mentions:read` | Respond when mentioned in channels |
| `channels:history` | Read messages in public channels |
| `channels:read` | List public channels |
| `chat:write` | Post messages and replies |
| `groups:history` | Read messages in private channels |
| `groups:read` | List private channels |
| `im:history` | Read direct messages |
| `im:read` | List DM conversations |
| `mpim:history` | Read group DM messages |
| `reactions:write` | Add emoji reactions to messages |
| `users:read` | Look up user info |

## Configure App Home

Go to **App Home** and enable **Allow users to send Slash commands and messages from the messages tab**. Without this, users will see "Sending messages to this app has been turned off" when they open a DM with the bot.

## Configure Event Subscriptions

Go to **Event Subscriptions** → toggle **Enable Events** on.

Set the **Request URL** to `https://<your-domain>/slack/events`.

Under **Subscribe to bot events**, add:
- `app_mention`
- `message.channels`
- `message.groups`
- `message.im`
- `message.mpim`

## Operating Modes

The bot supports two deployment modes:

### Single-tenant (one workspace)

Use a single bot token. Good for development or if you only need one workspace.

**Socket mode** (recommended for local dev):
- Go to **Socket Mode** in your app settings and enable it. This generates an app-level token.
- No public URL needed — the bot connects outbound to Slack.

```
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
```

**HTTP mode** (production):
- Requires a publicly reachable URL for the event request URL.
- The signing secret is in **Basic Information** → **App Credentials**.

```
SLACK_BOT_TOKEN=xoxb-...
SLACK_SIGNING_SECRET=<from Basic Information>
```

### Multi-tenant (self-serve OAuth installs)

Users install the app to their workspace from the settings page. Each workspace gets its own bot token stored in Postgres. Do **not** set `SLACK_BOT_TOKEN` in this mode.

```
SLACK_CLIENT_ID=<from Basic Information>
SLACK_CLIENT_SECRET=<from Basic Information>
SLACK_SIGNING_SECRET=<from Basic Information>
SLACK_REDIRECT_URL=https://<your-domain>/api/slack/oauth/callback
WEB_BASE_URL=http://localhost:5173
```

- `SLACK_REDIRECT_URL` must match the public URL users are redirected back to. Behind a tunnel or reverse proxy, the server sees `localhost` as the host, so this env var is needed to generate the correct OAuth callback URL.
- `WEB_BASE_URL` is where the user is redirected after the OAuth flow completes. In dev, this is the Vite dev server (`http://localhost:5173`). In production where the API serves the frontend, this can be omitted.
- `SLACK_ALLOWED_TEAM_IDS` (optional) restricts which workspaces can install the app. Set to a comma-separated list of Slack team IDs (e.g. `T01ABC,T02DEF`). If unset, any workspace can install.

**Additional setup required:**

1. **OAuth redirect URL**: Go to **OAuth & Permissions** → **Redirect URLs** and add the same URL as `SLACK_REDIRECT_URL`:
   ```
   https://<your-domain>/api/slack/oauth/callback
   ```

2. **Public distribution** (optional): To allow users outside your workspace to install the app, go to **Basic Information** → **Manage Distribution** and enable public distribution.

## Local Development

Multi-tenant and HTTP modes require a public URL for Slack to send events to. Use a Cloudflare Tunnel to get a stable subdomain.

**One-time setup:**

1. Install cloudflared:
   ```
   brew install cloudflared
   ```

2. Authenticate with Cloudflare (opens a browser):
   ```
   cloudflared tunnel login
   ```

3. Create a tunnel:
   ```
   cloudflared tunnel create dz-local
   ```

4. Route a subdomain to the tunnel (you must have the domain on Cloudflare).
   Use a single-level subdomain — nested subdomains (e.g. `dz.lo.example.com`) won't work with Cloudflare's free SSL certificate.
   ```
   cloudflared tunnel route dns dz-local yourapp.yourdomain.com
   ```

5. Configure your Slack app URLs (one-time, since the subdomain is stable):
   - **Event Subscriptions** → Request URL: `https://yourapp.yourdomain.com/slack/events`
   - **OAuth & Permissions** → Redirect URL: `https://yourapp.yourdomain.com/api/slack/oauth/callback`

**Each dev session:**

1. Start the tunnel:
   ```
   cloudflared tunnel --url http://localhost:8080 run dz-local
   ```

2. Start the API server and web dev server.
3. Sign in as a domain user, go to Settings, click "Add to Slack".
