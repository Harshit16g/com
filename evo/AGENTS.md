# evo API - AI Agent Guidelines

This document provides comprehensive guidelines for AI agents (Claude, GPT, Cursor, etc.) working with the evo API codebase.

## Project Overview

**evo API** is a production-ready, multi-tenant WhatsApp API platform built with Node.js, TypeScript, and Express.js. It supports multiple WhatsApp providers and extensive integrations with chatbots, CRM systems, and messaging platforms.

## Project Structure & Module Organization

### Core Directories
- **`src/`** – TypeScript source code with modular architecture
  - `api/controllers/` – HTTP route handlers (thin layer)
  - `api/services/` – Business logic (core functionality)
  - `api/routes/` – Express route definitions (RouterBroker pattern)
  - `api/integrations/` – External service integrations
    - `channel/` – WhatsApp providers (Baileys, Business API, evo)
    - `chatbot/` – AI/Bot integrations (OpenAI, Dify, Typebot, Chatwoot)
    - `event/` – Event systems (WebSocket, RabbitMQ, SQS, NATS, Pusher)
    - `storage/` – File storage (S3, MinIO)
  - `dto/` – Data Transfer Objects (simple classes, no decorators)
  - `guards/` – Authentication/authorization middleware
  - `types/` – TypeScript type definitions
  - `repository/` – Data access layer (Prisma)
- **`prisma/`** – Database schemas and migrations
  - `postgresql-schema.prisma` / `mysql-schema.prisma` – Provider-specific schemas
  - `postgresql-migrations/` / `mysql-migrations/` – Provider-specific migrations
- **`config/`** – Environment and application configuration
- **`utils/`** – Shared utilities and helper functions
- **`validate/`** – JSONSchema7 validation schemas
- **`exceptions/`** – Custom HTTP exception classes
- **`cache/`** – Redis and local cache implementations

### Build & Deployment
- **`dist/`** – Build output (do not edit directly)
- **`public/`** – Static assets and media files
- **`Docker*`**, **`docker-compose*.yaml`** – Containerization and local development stack

## Build, Test, and Development Commands

### Development Workflow
```bash
# Development server with hot reload
npm run dev:server

# Direct execution for testing
npm start

# Production build and run
npm run build
npm run start:prod
```

### Code Quality
```bash
# Linting and formatting
npm run lint        # ESLint with auto-fix
npm run lint:check  # ESLint check only

# Commit with conventional commits
npm run commit      # Interactive commit with Commitizen
```

### Database Management
```bash
# Set database provider first (CRITICAL)
export DATABASE_PROVIDER=postgresql  # or mysql

# Generate Prisma client
npm run db:generate

# Development migrations (with provider sync)
npm run db:migrate:dev      # Unix/Mac
npm run db:migrate:dev:win  # Windows

# Production deployment
npm run db:deploy      # Unix/Mac
npm run db:deploy:win  # Windows

# Database tools
npm run db:studio      # Open Prisma Studio
```

### Docker Development
```bash
# Start local services (Redis, PostgreSQL, etc.)
docker-compose up -d

# Full development stack
docker-compose -f docker-compose.dev.yaml up -d
```

## Coding Standards & Architecture Patterns

### Code Style (Enforced by ESLint + Prettier)
- **TypeScript strict mode** with full type coverage
- **2-space indentation**, single quotes, trailing commas
- **120-character line limit**
- **Import order** via `simple-import-sort`
- **File naming**: `feature.kind.ts` (e.g., `whatsapp.baileys.service.ts`)
- **Naming conventions**:
  - Classes: `PascalCase`
  - Functions/variables: `camelCase`
  - Constants: `UPPER_SNAKE_CASE`
  - Files: `kebab-case.type.ts`

### Architecture Patterns

#### Service Layer Pattern
```typescript
export class ExampleService {
  constructor(private readonly waMonitor: WAMonitoringService) {}
  
  private readonly logger = new Logger('ExampleService');
  
  public async create(instance: InstanceDto, data: ExampleDto) {
    // Business logic here
    return { example: { ...instance, data } };
  }
  
  public async find(instance: InstanceDto): Promise<ExampleDto | null> {
    try {
      const result = await this.waMonitor.waInstances[instance.instanceName].findData();
      return result || null; // Return null on not found (evo pattern)
    } catch (error) {
      this.logger.error('Error finding data:', error);
      return null; // Return null on error (evo pattern)
    }
  }
}
```

#### Controller Pattern (Thin Layer)
```typescript
export class ExampleController {
  constructor(private readonly exampleService: ExampleService) {}
  
  public async createExample(instance: InstanceDto, data: ExampleDto) {
    return this.exampleService.create(instance, data);
  }
}
```

#### RouterBroker Pattern
```typescript
export class ExampleRouter extends RouterBroker {
  constructor(...guards: any[]) {
    super();
    this.router.post(this.routerPath('create'), ...guards, async (req, res) => {
      const response = await this.dataValidate<ExampleDto>({
        request: req,
        schema: exampleSchema, // JSONSchema7
        ClassRef: ExampleDto,
        execute: (instance, data) => controller.createExample(instance, data),
      });
      res.status(201).json(response);
    });
  }
}
```

#### DTO Pattern (Simple Classes)
```typescript
// CORRECT - evo API pattern (no decorators)
export class ExampleDto {
  name: string;
  description?: string;
  enabled: boolean;
}

// INCORRECT - Don't use class-validator decorators
export class BadExampleDto {
  @IsString() // ❌ evo API doesn't use decorators
  name: string;
}
```

#### Validation Pattern (JSONSchema7)
```typescript
import { JSONSchema7 } from 'json-schema';
import { v4 } from 'uuid';

export const exampleSchema: JSONSchema7 = {
  $id: v4(),
  type: 'object',
  properties: {
    name: { type: 'string' },
    description: { type: 'string' },
    enabled: { type: 'boolean' },
  },
  required: ['name', 'enabled'],
};
```

## Multi-Tenant Architecture

### Instance Isolation
- **CRITICAL**: All operations must be scoped by `instanceName` or `instanceId`
- **Database queries**: Always include `where: { instanceId: ... }`
- **Authentication**: Validate instance ownership before operations
- **Data isolation**: Complete separation between tenant instances

### WhatsApp Instance Management
```typescript
// Access instance via WAMonitoringService
const waInstance = this.waMonitor.waInstances[instance.instanceName];
if (!waInstance) {
  throw new NotFoundException(`Instance ${instance.instanceName} not found`);
}
```

## Database Patterns

### Multi-Provider Support
- **PostgreSQL**: Uses `@db.Integer`, `@db.JsonB`, `@default(now())`
- **MySQL**: Uses `@db.Int`, `@db.Json`, `@default(now())`
- **Environment**: Set `DATABASE_PROVIDER=postgresql` or `mysql`
- **Migrations**: Provider-specific folders auto-selected

### Prisma Repository Pattern
```typescript
// Always use PrismaRepository for database operations
const result = await this.prismaRepository.instance.findUnique({
  where: { name: instanceName },
});
```

## Integration Patterns

### Channel Integration (WhatsApp Providers)
- **Baileys**: WhatsApp Web with QR code authentication
- **Business API**: Official Meta WhatsApp Business API  
- **evo API**: Custom WhatsApp integration
- **Pattern**: Extend base channel service classes

### Chatbot Integration
- **Base classes**: Extend `BaseChatbotService` and `BaseChatbotController`
- **Trigger system**: Support keyword, regex, and advanced triggers
- **Session management**: Handle conversation state per user
- **Available integrations**: evoBot, OpenAI, Dify, Typebot, Chatwoot, Flowise, N8N, EvoAI

### Event Integration
- **Internal events**: EventEmitter2 for application events
- **External events**: WebSocket, RabbitMQ, SQS, NATS, Pusher
- **Webhook delivery**: Reliable delivery with retry logic

## Testing Guidelines

### Current State
- **No formal test suite** currently implemented
- **Manual testing** is the primary approach
- **Integration testing** in development environment

### Testing Strategy
```typescript
// Place tests in test/ directory as *.test.ts
// Run: npm test (watches test/all.test.ts)

describe('ExampleService', () => {
  it('should create example', async () => {
    // Mock external dependencies
    // Test business logic
    // Assert expected behavior
  });
});
```

### Recommended Approach
- Focus on **critical business logic** in services
- **Mock external dependencies** (WhatsApp APIs, databases)
- **Integration tests** for API endpoints
- **Manual testing** for WhatsApp connection flows

## Commit & Pull Request Guidelines

### Conventional Commits (Enforced by commitlint)
```bash
# Use interactive commit tool
npm run commit

# Commit format: type(scope): subject (max 100 chars)
# Types: feat, fix, docs, style, refactor, perf, test, chore, ci, build, revert, security
```

### Examples
- `feat(api): add WhatsApp message status endpoint`
- `fix(baileys): resolve connection timeout issue`
- `docs(readme): update installation instructions`
- `refactor(service): extract common message validation logic`

### Pull Request Requirements
- **Clear description** of changes and motivation
- **Linked issues** if applicable
- **Migration impact** (specify database provider)
- **Local testing steps** with screenshots/logs
- **Breaking changes** clearly documented

## Security & Configuration

### Environment Setup
```bash
# Copy example environment file
cp .env.example .env

# NEVER commit secrets to version control
# Set DATABASE_PROVIDER before database commands
export DATABASE_PROVIDER=postgresql  # or mysql
```

### Security Best Practices
- **API key authentication** via `apikey` header
- **Input validation** with JSONSchema7
- **Rate limiting** on all endpoints
- **Webhook signature validation**
- **Instance-based access control**
- **Secure defaults** for all configurations

### Vulnerability Reporting
- See `SECURITY.md` for security vulnerability reporting process
- Contact: `contato@evo-api.com`

## Communication Standards

### Language Requirements
- **User communication**: Always respond in Portuguese (PT-BR)
- **Code/comments**: English for technical documentation
- **API responses**: English for consistency
- **Error messages**: Portuguese for user-facing errors

### Documentation Standards
- **Inline comments**: Document complex business logic
- **API documentation**: Document all public endpoints
- **Integration guides**: Document new integration patterns
- **Migration guides**: Document database schema changes

## Performance & Scalability

### Caching Strategy
- **Redis primary**: Distributed caching for production
- **Node-cache fallback**: Local caching when Redis unavailable
- **TTL strategy**: Appropriate cache expiration per data type
- **Cache invalidation**: Proper invalidation on data changes

### Connection Management
- **Database**: Prisma connection pooling
- **WhatsApp**: One connection per instance with lifecycle management
- **Redis**: Connection pooling and retry logic
- **External APIs**: Rate limiting and retry with exponential backoff

### Monitoring & Observability
- **Structured logging**: Pino logger with correlation IDs
- **Error tracking**: Comprehensive error scenarios
- **Health checks**: Instance status and connection monitoring
- **Telemetry**: Usage analytics (non-sensitive data only)

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **evolution-api** (4468 symbols, 10240 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/evolution-api/context` | Codebase overview, check index freshness |
| `gitnexus://repo/evolution-api/clusters` | All functional areas |
| `gitnexus://repo/evolution-api/processes` | All execution flows |
| `gitnexus://repo/evolution-api/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
