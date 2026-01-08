# clidup 🗄️

**Professional CLI tool for database backups and restores**

`clidup` is a command-line interface designed to simplify database backup and restore operations. Built with Python, it provides a clean, professional interface for database administrators and developers.

##  Features (MVP)

-  **PostgreSQL Support**: Full backup and restore for PostgreSQL databases
-  **Compression**: Optional tar.gz compression for backups
-  **Local Storage**: Save backups to your local filesystem
-  **Configuration Management**: YAML configuration + environment variables
-  **Comprehensive Logging**: Rotating log files with detailed operation history
-  **User Safety**: Confirmation prompts for destructive operations
-  **Cross-Platform**: Works on Windows, Linux, and macOS

##  Requirements

### System Requirements

- Python 3.10 or higher
- PostgreSQL client tools (`pg_dump` and `psql`)

### Installing PostgreSQL Client Tools

**Windows:**
Download and install from [PostgreSQL Downloads](https://www.postgresql.org/download/windows/)

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install postgresql-client
```

**macOS:**
```bash
brew install postgresql
```

##  Installation

1. **Clone or download the repository:**
```bash
https://github.com/Geovani29/clidup.git
```

2. **Install in editable mode:**
```bash
pip install -e .
```

3. **Verify installation:**
```bash
clidup --help
```

> **Note for Windows Users:**
> If you get a "command not found" error, it means the Python Scripts folder is not in your system PATH.
> **Alternative run method:**
> You can always run the tool using:
> ```bash
> python -m clidup ...
> ```

##  Configuration

### 1. Create Configuration File

A `config.yaml` file should already exist in the project root. Modify it as needed:

```yaml
# PostgreSQL Database Configuration
postgres:
  host: localhost
  port: 5432
  username: postgres
  database: postgres

# Backup Settings
backup:
  directory: ./backups
```

### 2. Set Environment Variables

Create a `.env` file in the project root:

```bash
# Copy the example file
copy .env.example .env
```

Edit `.env` and set your PostgreSQL password:

```
POSTGRES_PASSWORD=your_actual_password
```


##  Usage

### First Time Setup

Before using clidup, initialize the configuration:

```bash
clidup init
```

This will interactively ask for:
- PostgreSQL host, port, username, password
- Backup directory location

And create:
- `config.yaml` - Database configuration
- `.env` - Password (never commit this!)
- `backups/` - Directory for backups

### Backup a Database

**Basic backup:**
```bash
clidup backup --db postgres --db-name myapp_db
```

**With compression:**
```bash
clidup backup --db postgres --db-name myapp_db --compress
```

**Output:**
```
✅ Backup completed successfully!
📁 Backup file: C:\...\backups\postgres_myapp_db_full_2026-01-07_23-15.sql.tar.gz
📝 Logs: C:\...\backups\clidup.log
```

### Restore a Database

**Basic restore:**
```bash
clidup restore --db postgres --file backups\postgres_myapp_db_full_2026-01-07_23-15.sql
```

**Restore compressed backup:**
```bash
clidup restore --db postgres --file backups\postgres_myapp_db_full_2026-01-07_23-15.sql.tar.gz
```

**Skip confirmation (use with caution):**
```bash
clidup restore --db postgres --file backups\backup.sql --yes
```

**Specify different database name:**
```bash
clidup restore --db postgres --file backups\backup.sql --db-name target_database
```

### View Help

```bash
# General help
clidup --help

# Backup command help
clidup backup --help

# Restore command help
clidup restore --help

# Version information
clidup --version
```

##  Backup File Naming

Backups are automatically named with the following format:

```
<db_type>_<db_name>_full_<YYYY-MM-DD>_<HH-MM>.sql
```

**Examples:**
- `postgres_myapp_db_full_2026-01-07_23-15.sql`
- `postgres_production_full_2026-01-08_02-30.sql.tar.gz` (compressed)

##  Security Best Practices

-  **Passwords via environment variables only** - Never hardcode passwords
-  **`.env` file in `.gitignore`** - Prevent accidental commits
-  **No passwords in logs** - Automatic sanitization
-  **Confirmation prompts** - Prevent accidental data loss

##  Troubleshooting

### "pg_dump not found"

**Solution:** Install PostgreSQL client tools (see Requirements section)

### "POSTGRES_PASSWORD environment variable not set"

**Solution:** Create a `.env` file with your password:
```
POSTGRES_PASSWORD=your_password
```

### "config.yaml not found"

**Solution:** Make sure you're running `clidup` from the project directory, or specify the config path:
```bash
clidup backup --db postgres --db-name mydb --config /path/to/config.yaml
```

### Backup fails with "connection refused"

**Solution:** Check that PostgreSQL is running and the connection details in `config.yaml` are correct.

##  Current Limitations

This is the MVP (Minimum Viable Product) version. The following features are **not yet implemented**:

-  Cloud storage (AWS S3, Azure Blob, Google Cloud Storage)
-  Multiple database types (MySQL, MongoDB, etc.)
-  Incremental or differential backups
-  Scheduled/automated backups
-  Email notifications
-  Backup encryption
-  Automated tests

These features are planned for future releases.

##  Roadmap

**Phase 2 (Future):**
- Support for MySQL and MongoDB
- Cloud storage integration (S3, Azure, GCS)
- Incremental backups
- Automated scheduling
- Web dashboard
- Backup encryption
- Comprehensive test suite

##  License

MIT License - feel free to use this project for personal or commercial purposes.

##  Contributing

This is currently an MVP project. Contributions, issues, and feature requests are welcome!

##  Support

For issues or questions, please create an issue in the repository.

---

**Built with ❤️ using Python and Typer**
