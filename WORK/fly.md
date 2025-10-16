Here’s a clean **Shell Script template** for your use case — it checks if a database exists, and if it does, it triggers **Flyway** to run a specific SQL migration (like `V0001__create_schema_ingestion.sql`).

---

### ✅ **Script: `run_flyway_ingestion.sh`**

```bash
#!/bin/bash

# === Configuration ===
DB_NAME="ingestion"
DB_USER="your_db_user"
DB_HOST="localhost"
DB_PORT="5432"
FLYWAY_PATH="/path/to/flyway"              # Example: /opt/flyway/flyway
FLYWAY_CONF="/path/to/flyway.conf"         # Example: /opt/flyway/conf/flyway.conf
FLYWAY_SQL_DIR="/path/to/sql"              # Example: /opt/flyway/sql
MIGRATION_FILE="V0001__create_schema_ingestion.sql"

# === Function to check DB existence ===
check_db_exists() {
    echo "🔍 Checking if database '$DB_NAME' exists..."
    DB_EXISTS=$(psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -tAc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME';")

    if [ "$DB_EXISTS" = "1" ]; then
        echo "✅ Database '$DB_NAME' exists."
        return 0
    else
        echo "❌ Database '$DB_NAME' does not exist."
        return 1
    fi
}

# === Main Logic ===
if check_db_exists; then
    echo "🚀 Running Flyway migration..."
    $FLYWAY_PATH -configFiles="$FLYWAY_CONF" migrate \
        -locations="filesystem:$FLYWAY_SQL_DIR" \
        -target="1" \
        -schemas="$DB_NAME"

    if [ $? -eq 0 ]; then
        echo "✅ Flyway migration completed successfully for '$DB_NAME'."
    else
        echo "❌ Flyway migration failed. Check Flyway logs."
        exit 1
    fi
else
    echo "⚠️ Skipping Flyway run. Database '$DB_NAME' not found."
    exit 1
fi
```

---

### 🧩 **How to Configure Flyway**

Your `flyway.conf` (example):

```bash
flyway.url=jdbc:postgresql://localhost:5432/ingestion
flyway.user=your_db_user
flyway.password=your_password
flyway.schemas=ingestion
flyway.locations=filesystem:/opt/flyway/sql
```

And ensure your migration file is:

```
/opt/flyway/sql/V0001__create_schema_ingestion.sql
```

---

### 🧠 **How It Works**

1. The script uses `psql` to check if the PostgreSQL database `ingestion` exists.
2. If found, it triggers Flyway’s `migrate` command to apply the SQL migration.
3. If the DB doesn’t exist, it prints a warning and exits.

---

### ⚙️ **How to Run**

```bash
chmod +x run_flyway_ingestion.sh
./run_flyway_ingestion.sh
```

---

### 🪶 Optional Enhancements

You can extend it to:

* Accept DB name or Flyway path as arguments (`$1`, `$2`, etc.)
* Log outputs to `/var/log/flyway_ingestion.log`
* Add retries or wait logic (if DB creation takes time)
* Run multiple Flyway scripts in sequence

---

Would you like me to modify this script for **MySQL** instead of PostgreSQL (since syntax slightly differs)?
