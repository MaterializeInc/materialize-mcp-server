# Materialize MCP Server

A Model Context Protocol (MCP) server that provides tools for managing and querying Materialize databases.

The server exposes a comprehensive set of static tools for database operations including listing objects, managing clusters, executing SQL transactions, monitoring query performance, creating PostgreSQL connections and sources, managing materialized views, and handling indexes.

## Installation

### Quick Start Checklist

✅ **Before you begin:**
1. Install Python 3.13+ and `uv`
2. Configure AWS credentials: `aws configure --profile your-profile-name`
3. (Optional) Start Materialize: `docker run -p 6875:6875 materialize/materialized:latest`

✅ **Installation steps:**
1. Clone repo and install: `cd materialize-mcp-server/developers && uv sync`
2. Configure your IDE:
   - **Claude Code**: `claude mcp add -e AWS_PROFILE=your-profile ... materialize-local ...`
   - **Cursor**: Create `.cursor/mcp.json` (see "Using with Cursor" section)
3. Restart your IDE
4. Verify: Ask your AI assistant "Check the Materialize connection status"

### Using with Claude Code

Claude Code automatically starts and manages MCP servers for you. You don't need to run the server manually - just configure it and Claude Code will handle the rest.

#### Prerequisites

Before setting up the MCP server, ensure you have:

1. **Python 3.13+** and **uv** installed
2. **AWS credentials configured** (required for documentation search):
   ```bash
   # Check if you have AWS credentials
   aws sts get-caller-identity

   # If not, configure your AWS profile
   aws configure --profile your-profile-name
   ```

   The server uses AWS S3 Vector Buckets for semantic documentation search. Without AWS credentials, the documentation search tool won't work (but all database tools will still function).

3. **Materialize instance** (optional - server works without it):
   - Local: `docker run -p 6875:6875 materialize/materialized:latest`
   - Or Materialize Cloud: Get your connection string from the console

#### Quick Setup

**Step 1: Install dependencies**

```bash
git clone https://github.com/MaterializeInc/materialize-mcp-server
cd materialize-mcp-server/developers
uv sync
```

⚠️ **Important:** You must run `uv sync` before configuring Claude Code. This pre-installs all dependencies (including large ML packages for documentation search) to ensure fast startup times (~0.5 seconds instead of 2+ seconds).

**Step 2: Configure Claude Code**

Using Claude Code CLI (recommended for fastest startup ~0.5s):

```bash
# For local Materialize with AWS credentials
claude mcp add \
  -e MCP_LOG_LEVEL=ERROR \
  -e MCP_ENABLE_JSON_LOG=false \
  -e AWS_PROFILE=your-aws-profile \
  materialize-local \
  "$(pwd)/.venv/bin/python" -m mcp_materialize_developers

# For Materialize Cloud
claude mcp add \
  -e MCP_LOG_LEVEL=ERROR \
  -e MCP_ENABLE_JSON_LOG=false \
  -e AWS_PROFILE=your-aws-profile \
  -e MZ_DSN="postgresql://user@host.materialize.cloud:6875/materialize?sslmode=require" \
  materialize-cloud \
  "$(pwd)/.venv/bin/python" -m mcp_materialize_developers
```

**Step 3: Restart Claude Code**

After configuration, fully restart Claude Code to load the MCP server.

**Step 4: Verify it works**

Ask Claude: "Check the Materialize connection status" - it should use the `check_connection` tool and report the connection state.

#### Manual Configuration

Alternatively, create a `.mcp.json` file in your project root or edit `~/.claude.json`:

**Option 1: Using venv directly (fastest - recommended)**

```json
{
  "mcpServers": {
    "materialize-local": {
      "command": "/Users/yourname/path/to/materialize-mcp-server/developers/.venv/bin/python",
      "args": ["-m", "mcp_materialize_developers"],
      "env": {
        "MCP_LOG_LEVEL": "ERROR",
        "MCP_ENABLE_JSON_LOG": "false",
        "AWS_PROFILE": "your-aws-profile",
        "MZ_DSN": "postgresql://materialize@localhost:6875/materialize"
      }
    }
  }
}
```

**Option 2: Using `uv run` (slightly slower but simpler paths)**

```json
{
  "mcpServers": {
    "materialize-local": {
      "command": "uv",
      "args": ["run", "--project", "/Users/yourname/path/to/materialize-mcp-server/developers", "mcp_materialize_developers"],
      "env": {
        "MCP_LOG_LEVEL": "ERROR",
        "MCP_ENABLE_JSON_LOG": "false",
        "AWS_PROFILE": "your-aws-profile",
        "MZ_DSN": "postgresql://materialize@localhost:6875/materialize"
      }
    }
  }
}
```

**Required environment variables:**
- `MCP_LOG_LEVEL=ERROR` - Reduces log noise for Claude Code
- `MCP_ENABLE_JSON_LOG=false` - Prevents JSON logs from interfering with MCP protocol
- `AWS_PROFILE=your-aws-profile` - AWS credentials for documentation search (use your actual profile name)

**Optional environment variables:**
- `MZ_DSN` - Materialize connection string (defaults to local instance if not specified)
- `MCP_MODE` - Server mode: `READ_WRITE` (default), `READ_ONLY`, or `DOCS_ONLY`

#### All Configuration Options

**Required for Claude Code:**
- `MCP_LOG_LEVEL=ERROR` - Reduces log noise (prevents interference with MCP protocol)
- `MCP_ENABLE_JSON_LOG=false` - Disables JSON logging format
- `AWS_PROFILE` - Your AWS profile name for documentation search (e.g., `default`, `mz-scratch-admin`)

**Materialize Connection (optional - server works without it):**
- `MZ_DSN`: Full connection string (e.g., `postgresql://materialize@localhost:6875/materialize`)
- `PGHOST`: Materialize host (default: localhost)
- `PGPORT`: Materialize port (default: 6875)
- `PGUSER`: Database user (default: materialize)
- `PGPASSWORD`: Database password (if required)
- `PGDATABASE`: Database name (default: materialize)

**Documentation Search (requires AWS credentials):**
- `AWS_PROFILE`: AWS profile name (required for documentation search)
- `AWS_REGION`: AWS region for S3 (default: us-east-1)
- `S3_BUCKET_NAME`: S3 bucket for document vectors (default: materialize-docs-vectors)

**Advanced Options:**
- `MCP_MODE`: Server mode - `READ_WRITE`, `READ_ONLY`, or `DOCS_ONLY` (default: READ_WRITE)
  - `READ_WRITE`: Full database access + documentation search
  - `READ_ONLY`: Read-only database access + documentation search
  - `DOCS_ONLY`: Documentation search only (no database connection needed)
- `MCP_TRANSPORT`: Communication transport - always use `stdio` for Claude Code (default: stdio)
- `MCP_POOL_MIN_SIZE`: Minimum database connection pool size (default: 1)
- `MCP_POOL_MAX_SIZE`: Maximum database connection pool size (default: 10)

#### How Documentation Search Works

The MCP server provides semantic search over Materialize documentation using:

1. **ML-powered embeddings** - Uses `sentence-transformers` to create vector embeddings of documentation
2. **AWS S3 Vector Buckets** - Stores embeddings in S3 for fast semantic search
3. **Automatic indexing** - On first search, automatically scrapes and indexes https://materialize.com/docs/

**First-time setup (automatic):**
- When you first use documentation search, the server will:
  - Load the ML model (~2-3 seconds one-time cost)
  - Create the S3 bucket `materialize-docs-vectors` if it doesn't exist
  - Scrape and index Materialize documentation
  - Store embeddings in S3 for future use

**Subsequent searches:**
- Fast semantic search over indexed documentation
- Returns relevant docs with URLs and excerpts
- Works even if Materialize database is unavailable

**Usage example:**
```
You: "How do I create a materialized view?"
Claude: [Uses search_documentation tool]
[Returns relevant documentation with URLs]
```

### Using with Cursor

Cursor IDE supports MCP servers through configuration files. Like Claude Code, Cursor automatically starts and manages MCP servers - you just configure them and Cursor handles the rest.

#### Prerequisites

Same as Claude Code above:

1. **Python 3.13+** and **uv** installed
2. **AWS credentials configured** (required for documentation search)
3. **Materialize instance** (optional - server works without it)

#### Quick Setup

**Step 1: Install dependencies**

```bash
git clone https://github.com/MaterializeInc/materialize-mcp-server
cd materialize-mcp-server/developers
uv sync
```

⚠️ **Important:** You must run `uv sync` before configuring Cursor. This pre-installs all dependencies to ensure fast startup times (~0.5 seconds).

**Step 2: Configure Cursor**

Cursor supports two configuration options:

**Option A: Project-specific (recommended)**

Create `.cursor/mcp.json` in your project root:

```bash
mkdir -p .cursor
```

Then create `.cursor/mcp.json` with this content:

```json
{
  "mcpServers": {
    "materialize-local": {
      "command": "/absolute/path/to/materialize-mcp-server/developers/.venv/bin/python",
      "args": ["-m", "mcp_materialize_developers"],
      "env": {
        "MCP_LOG_LEVEL": "ERROR",
        "MCP_ENABLE_JSON_LOG": "false",
        "AWS_PROFILE": "your-aws-profile",
        "MZ_DSN": "postgresql://materialize@localhost:6875/materialize"
      }
    }
  }
}
```

**Option B: Global configuration**

Create `~/.cursor/mcp.json` in your home directory for tools available across all projects (same format as above).

**Step 3: Restart Cursor**

After creating the configuration file, fully restart Cursor to load the MCP server.

**Step 4: Verify it works**

1. Open Cursor's AI chat (Cmd+L or Ctrl+L)
2. Ask: "Check the Materialize connection status"
3. Cursor should use the `check_connection` tool and report the connection state
4. You can also ask: "What MCP tools are available?" to see all the Materialize tools

#### Configuration Details

**Using venv directly (fastest - recommended)**

```json
{
  "mcpServers": {
    "materialize-local": {
      "command": "/Users/yourname/path/to/materialize-mcp-server/developers/.venv/bin/python",
      "args": ["-m", "mcp_materialize_developers"],
      "env": {
        "MCP_LOG_LEVEL": "ERROR",
        "MCP_ENABLE_JSON_LOG": "false",
        "AWS_PROFILE": "your-aws-profile",
        "MZ_DSN": "postgresql://materialize@localhost:6875/materialize"
      }
    }
  }
}
```

**Using `uv run` (slightly slower but simpler paths)**

```json
{
  "mcpServers": {
    "materialize-local": {
      "command": "uv",
      "args": ["run", "--project", "/Users/yourname/path/to/materialize-mcp-server/developers", "mcp_materialize_developers"],
      "env": {
        "MCP_LOG_LEVEL": "ERROR",
        "MCP_ENABLE_JSON_LOG": "false",
        "AWS_PROFILE": "your-aws-profile",
        "MZ_DSN": "postgresql://materialize@localhost:6875/materialize"
      }
    }
  }
}
```

**For Materialize Cloud:**

```json
{
  "mcpServers": {
    "materialize-cloud": {
      "command": "/Users/yourname/path/to/materialize-mcp-server/developers/.venv/bin/python",
      "args": ["-m", "mcp_materialize_developers"],
      "env": {
        "MCP_LOG_LEVEL": "ERROR",
        "MCP_ENABLE_JSON_LOG": "false",
        "AWS_PROFILE": "your-aws-profile",
        "MZ_DSN": "postgresql://user@host.materialize.cloud:6875/materialize?sslmode=require"
      }
    }
  }
}
```

**Required environment variables:**
- `MCP_LOG_LEVEL=ERROR` - Reduces log noise for Cursor
- `MCP_ENABLE_JSON_LOG=false` - Prevents JSON logs from interfering with MCP protocol
- `AWS_PROFILE=your-aws-profile` - AWS credentials for documentation search

**Optional environment variables:**
- `MZ_DSN` - Materialize connection string (defaults to local instance if not specified)
- `MCP_MODE` - Server mode: `READ_WRITE` (default), `READ_ONLY`, or `DOCS_ONLY`

See "All Configuration Options" section above for the complete list of configuration options.

#### Using MCP Tools in Cursor

Once configured, you can interact with Materialize through Cursor's AI chat:

**Example interactions:**

```
You: "List all clusters in my Materialize instance"
Cursor: [Uses list_clusters tool and displays results]

You: "Create a new cluster called analytics with size 100cc"
Cursor: [Uses create_cluster tool]

You: "Show me all materialized views in the public schema"
Cursor: [Uses list_materialized_views tool]

You: "Check if any views are lagging by more than 5 seconds"
Cursor: [Uses monitor_data_freshness tool]

You: "How do I create a source from PostgreSQL?"
Cursor: [Uses search_documentation tool to find relevant docs]
```

**Tool approval:**
- Cursor may ask you to approve tool usage the first time
- You can choose to always allow specific tools or approve each use

#### Troubleshooting for Cursor

**MCP Server Fails to Start in Cursor:**

1. **Check the configuration file path**:
   - Project: `.cursor/mcp.json` in your project root
   - Global: `~/.cursor/mcp.json` in your home directory
   - Verify the file is valid JSON (no trailing commas!)

2. **Verify the Python path**:
   ```bash
   # Test the path works
   /absolute/path/to/.venv/bin/python -m mcp_materialize_developers --help
   ```

3. **Check Cursor's MCP logs**:
   - Open Cursor's output panel
   - Look for "MCP" in the output channels
   - Check for error messages about server startup

4. **Test startup manually**:
   ```bash
   cd materialize-mcp-server/developers
   echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' | \
     MCP_LOG_LEVEL=ERROR MCP_ENABLE_JSON_LOG=false \
     .venv/bin/python -m mcp_materialize_developers 2>&1 | head -1
   ```
   You should see a JSON response with `"serverInfo"`.

**Tools not appearing in Cursor:**

1. **Restart Cursor completely** - Don't just reload, fully quit and reopen
2. **Check configuration syntax** - Ensure JSON is valid (use a JSON validator)
3. **Verify AWS credentials** - If only documentation search fails:
   ```bash
   aws sts get-caller-identity --profile your-profile-name
   ```

**Materialize Connection Issues:**

See the "Materialize Connection Issues" section in the Troubleshooting section above - the same steps apply for Cursor.

**Documentation Search Issues:**

See the "Documentation Search Issues" section in the Troubleshooting section above - the same steps apply for Cursor.

### Manual Installation (for development)

If you want to run the server manually for development:

```bash
git clone https://github.com/MaterializeInc/materialize-mcp-server
cd materialize-mcp-server
uv sync
uv run materialize-mcp-server
```

## Available Tools

The server provides the following tools:

### Connection & Health
- **check_connection**: Check the connection status to Materialize database. Returns connection state, health status, and error information if the connection failed. This tool works even if Materialize is unavailable and helps diagnose connection issues.
- **search_documentation**: Search Materialize documentation using semantic search. This tool works without a Materialize connection and can help answer questions about Materialize features and syntax.

### Database Object Management
- **list_objects**: List all queryable objects in the Materialize database including sources, tables, views, materialized views, and indexed views
- **list_schemas**: List schemas in Materialize, optionally filtered by database name
- **show_sources**: Show sources in Materialize, optionally filtered by schema and/or cluster

### Cluster Management
- **list_clusters**: List all clusters in the Materialize instance
- **create_cluster**: Create a new cluster in Materialize with the specified name and size

### SQL Execution
- **run_sql_transaction**: Execute one or more SQL statements within a single transaction on a specified cluster

### Data Freshness Monitoring
- **monitor_data_freshness**: Monitor data freshness with dependency-aware analysis. Shows lagging objects, their dependency chains, and critical paths that introduce delay to help identify root causes of freshness issues
- **get_object_freshness_diagnostics**: Get detailed freshness diagnostics for a specific object, showing its freshness and the complete dependency chain with freshness information for each dependency

### PostgreSQL Integration
- **create_postgres_connection**: Create a PostgreSQL connection in Materialize with specified host, database, credentials, and SSL settings
- **create_postgres_source**: Create a PostgreSQL source using an existing connection and publication

### View Management
- **create_view**: Create a view with the specified name and SQL query
- **create_materialized_view**: Create a materialized view with the specified name, cluster, and SQL query
- **list_materialized_views**: List materialized views in Materialize, optionally filtered by schema and/or cluster

### Index Management
- **list_indexes**: List indexes in Materialize, optionally filtered by schema and/or cluster
- **create_index**: Create a default index on a source, view, or materialized view in a specified cluster
- **drop_index**: Drop an index from Materialize with optional CASCADE support

## Quickstart

Run the server with default settings:

```bash
uv run materialize-mcp
```

## Configuration

| Argument | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `--mz-dsn` | `MZ_DSN` | `postgresql://materialize@localhost:6875/materialize` | Materialize DSN |
| `--transport` | `MCP_TRANSPORT` | `stdio` | Communication transport (`stdio` or `sse`) |
| `--host` | `MCP_HOST` | `0.0.0.0` | Server host |
| `--port` | `MCP_PORT` | `3001` | Server port |
| `--pool-min-size` | `MCP_POOL_MIN_SIZE` | `1` | Minimum connection pool size |
| `--pool-max-size` | `MCP_POOL_MAX_SIZE` | `10` | Maximum connection pool size |
| `--log-level` | `MCP_LOG_LEVEL` | `INFO` | Logging level |

## Example Usage

### List Database Objects

```json
{
  "name": "list_objects",
  "arguments": {}
}
```

### Create a New Cluster

```json
{
  "name": "create_cluster",
  "arguments": {
    "cluster_name": "my_cluster",
    "size": "100cc"
  }
}
```

### Execute SQL Transaction

```json
{
  "name": "run_sql_transaction",
  "arguments": {
    "cluster_name": "my_cluster",
    "sql_statements": [
      "CREATE TABLE test_table (id int, name text)",
      "INSERT INTO test_table VALUES (1, 'test')",
      "SELECT * FROM test_table"
    ]
  }
}
```

### Create PostgreSQL Connection

```json
{
  "name": "create_postgres_connection",
  "arguments": {
    "connection_name": "my_connection",
    "host": "postgres.example.com",
    "database": "my_database",
    "password_secret": "pg_password",
    "username": "postgres",
    "port": 5432,
    "ssl_mode": "require"
  }
}
```

### Create PostgreSQL Source

```json
{
  "name": "create_postgres_source",
  "arguments": {
    "source_name": "my_source",
    "cluster_name": "my_cluster",
    "connection_name": "my_connection",
    "publication_name": "my_publication",
    "for_all_tables": true
  }
}
```

### Create Materialized View

```json
{
  "name": "create_materialized_view",
  "arguments": {
    "view_name": "my_materialized_view",
    "cluster_name": "my_cluster",
    "sql_query": "SELECT category_id, COUNT(*) as count FROM products GROUP BY category_id"
  }
}
```

### Create Index

```json
{
  "name": "create_index",
  "arguments": {
    "cluster_name": "my_cluster",
    "object_name": "my_materialized_view"
  }
}
```

### Monitor Data Freshness

```json
{
  "name": "monitor_data_freshness",
  "arguments": {
    "threshold_seconds": 3.0,
    "cluster": "my_cluster"
  }
}
```

### Get Object Freshness Diagnostics

```json
{
  "name": "get_object_freshness_diagnostics",
  "arguments": {
    "object_name": "my_materialized_view",
    "schema": "public"
  }
}
```

## Using with Claude Code and Cursor

Once configured, your IDE (Claude Code or Cursor) will automatically start the Materialize MCP server when you open your project. The tools will be available for the AI assistant to use when helping you with Materialize-related tasks.

### Troubleshooting

**MCP Server Fails to Start:**
1. **Run `uv sync` first** - This is the most common issue. The server needs dependencies pre-installed.
   ```bash
   cd materialize-mcp-server/developers
   uv sync
   ```

2. **Check startup time** - The server should start in ~0.5 seconds. If it takes longer:
   ```bash
   # Test startup manually
   cd materialize-mcp-server/developers
   echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' | \
     MCP_LOG_LEVEL=ERROR MCP_ENABLE_JSON_LOG=false \
     .venv/bin/python -m mcp_materialize_developers 2>&1 | head -1
   ```
   You should see a JSON response with `"serverInfo"`.

3. **Verify environment variables** - Make sure `MCP_LOG_LEVEL=ERROR` and `MCP_ENABLE_JSON_LOG=false` are set in your configuration to prevent log messages from interfering with the MCP protocol.

**Materialize Connection Issues:**

The MCP server can run even if Materialize is not available. The `check_connection` and `search_documentation` tools will always work, while database tools will fail with clear error messages if Materialize is not reachable.

1. **Use the `check_connection` tool** - In Claude Code, ask Claude to check the connection:
   ```
   You: "Check the Materialize connection status"
   Claude will use the check_connection tool and report the connection state
   ```

2. **Check Materialize is running** - If using a local instance:
   ```bash
   psql "postgresql://materialize@localhost:6875/materialize" -c "SELECT 1;"
   ```

3. **Verify connection string** - Check that `MZ_DSN` in your config points to the correct Materialize instance.

4. **Check logs** - If connection fails, the server will log a warning on startup but continue running. Documentation search and connection checking tools will still work.

**Documentation Search Issues:**

Documentation search requires valid AWS credentials. If searches aren't working:

1. **Verify AWS credentials**:
   ```bash
   # Check if your profile is configured
   aws sts get-caller-identity --profile your-profile-name

   # Should return your AWS account info
   ```

2. **Check AWS_PROFILE is set** - Make sure `AWS_PROFILE` is set in your MCP server configuration (in `~/.claude.json`)

3. **Verify S3 access**:
   ```bash
   # Check if you can access S3
   aws s3 ls --profile your-profile-name
   ```

4. **Check S3 bucket permissions** - Your AWS credentials need:
   - `s3:CreateBucket` (for first-time setup)
   - `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` (for document storage/retrieval)

5. **First search takes longer** - The first documentation search will take 2-3 seconds to:
   - Load the ML model (sentence-transformers)
   - Create the S3 bucket if it doesn't exist
   - Index documentation if not already indexed

   Subsequent searches are fast (~100-500ms).

**Slow Performance:**
- Server startup should be ~0.5s (or ~0.8s with `uv run`)
- If slower, make sure you ran `uv sync` to pre-install dependencies
- Use the venv path directly instead of `uv run` for fastest startup

### Example Interactions

```
You: "Show me all the clusters in my Materialize instance"
Claude: I'll list all the clusters in your Materialize instance.
[Claude automatically uses the list_clusters tool]

You: "Create a new cluster called analytics with size 100cc"
Claude: I'll create a new cluster called 'analytics' with size 100cc.
[Claude automatically uses the create_cluster tool]

You: "What materialized views are lagging by more than 5 seconds?"
Claude: I'll check for materialized views with freshness lag greater than 5 seconds.
[Claude automatically uses the monitor_data_freshness tool]
```

### Common Workflows

1. **Setting up a real-time data pipeline**:
   - Create a PostgreSQL connection
   - Create a source from the connection
   - Create materialized views on the source
   - Create indexes for query performance

2. **Monitoring data freshness**:
   - Use `monitor_data_freshness` to find lagging objects
   - Use `get_object_freshness_diagnostics` to analyze specific objects
   - Identify bottlenecks in your data pipeline

3. **Multi-cluster architecture**:
   - Create separate clusters for different workloads
   - Place transformation views on compute clusters
   - Place serving views on dedicated query clusters

## Use Cases

This MCP server is particularly useful for:

- **Data Pipeline Management**: Create and manage PostgreSQL sources for real-time data ingestion
- **Analytics Workloads**: Build materialized views for fast analytical queries
- **Performance Optimization**: Monitor query performance and create indexes for better response times
- **Multi-Cluster Architectures**: Separate compute workloads across different clusters (e.g., transform vs. serve clusters)
- **Database Administration**: Manage schemas, objects, and connections programmatically

## Architecture

The server supports a multi-cluster architecture where you can:
- Use dedicated clusters for different workloads (e.g., `transform` for data processing, `serve` for query serving)
- Create materialized views in specific clusters for optimal resource allocation
- Maintain indexes on different clusters for performance isolation
- Monitor and optimize query performance across clusters

