# Production Deployment & Mechanism Changes

This document tracks critical changes made to the Evolution API to ensure production stability and resolve connectivity issues.

## 1. Baileys Integration: QR Reconnection Loop Fix
**Issue**: The instance would enter an infinite reconnection loop when trying to generate the initial QR code, as the connection close event triggered an immediate reconnect before the QR could be emitted.

**Change**: Added an `isInitialConnection` guard in `src/api/integrations/channel/whatsapp/whatsapp.baileys.service.ts`.
- **Condition**: Reconnection is blocked if `!this.instance.wuid && this.instance.qrcode?.count === 0`.
- **Effect**: Allows the first connection attempt to close gracefully, permitting the QR code to be generated and displayed without triggering a loop.

## 2. Docker Networking: Host Mode
**Issue**: Internal Docker bridge networking (`evolution-net`) was experiencing "Network unreachable" errors and DNS resolution failures when the API tried to reach WhatsApp's WebSocket servers.

**Change**: Switched the `api` service to `network_mode: host` in `docker-compose.yaml`.
- **Reason**: Provides the container with direct access to the host's network stack, bypassing iptables/routing issues prevalent in some server environments.
- **Port Management**: The API now listens directly on the host's port `8080`.

## 3. Database & Cache Connectivity
**Change**: With the API in `network_mode: host`, all internal service connections must use `localhost`.
- **DATABASE_CONNECTION_URI**: Pointed to `localhost:5432`.
- **CACHE_REDIS_URI**: Pointed to `localhost:6379`.
- **Credentials**: Corrected to match the initialized Postgres volume (`user: evolution`, `password: strongpassword123`, `db: evolution`).

## 4. File Restoration
**Note**: The following critical directories were restored from the repository history:
- `src/`: TypeScript source code.
- `prisma/`: Database schemas and migrations.
- `manager/`: Evolution Manager frontend.
- `public/`: Static assets.

## 5. Maintenance & Updates
> [!IMPORTANT]
> When pulling updates from the upstream repository (`evolution-api`), ensure the Baileys reconnection fix is preserved or merged carefully.
>
> If you need to rebuild the Docker image, ensure the environment has proper DNS configuration (e.g., `8.8.8.8`) if the build process fails to resolve `dl-cdn.alpinelinux.org`.
