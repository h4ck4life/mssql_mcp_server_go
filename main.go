package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// Constants for timeout handling
const DEFAULT_QUERY_TIMEOUT = 120 // seconds

// Database connection configuration
type DbConfig struct {
	Driver       string
	Server       string
	User         string
	Password     string
	Database     string
	QueryTimeout int
}

func getDbConfig() (*DbConfig, error) {
	config := &DbConfig{
		Driver:       getEnvOrDefault("MSSQL_DRIVER", "sqlserver"),
		Server:       getEnvOrDefault("MSSQL_HOST", "localhost"),
		User:         getEnvOrDefault("MSSQL_USER", ""),
		Password:     getEnvOrDefault("MSSQL_PASSWORD", ""),
		Database:     getEnvOrDefault("MSSQL_DATABASE", ""),
		QueryTimeout: getEnvIntOrDefault("MSSQL_QUERY_TIMEOUT", DEFAULT_QUERY_TIMEOUT),
	}

	if config.User == "" || config.Password == "" || config.Database == "" {
		return nil, errors.New("missing required database configuration (MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE)")
	}

	return config, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvIntOrDefault(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		var result int
		_, err := fmt.Sscanf(value, "%d", &result)
		if err == nil {
			return result
		}
	}
	return defaultValue
}

func isWriteOperation(query string) bool {
	normalizedQuery := strings.TrimSpace(strings.ToUpper(query))

	// List of SQL commands that modify data or structure
	writeOperations := []string{
		"CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE",
		"TRUNCATE", "MERGE", "UPSERT", "GRANT", "REVOKE", "EXEC", "EXECUTE",
	}

	for _, operation := range writeOperations {
		if strings.HasPrefix(normalizedQuery, operation) || strings.Contains(normalizedQuery, " "+operation+" ") {
			return true
		}
	}

	return false
}

func getConnection(config *DbConfig) (*sql.DB, error) {
	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s;encrypt=true;trustservercertificate=true",
		config.Server, config.User, config.Password, config.Database)

	// Create connection
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}

	// Set connection properties
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetConnMaxIdleTime(time.Minute * 1)

	// Set query timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.QueryTimeout)*time.Second)
	defer cancel()

	// Test connection
	err = db.PingContext(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func executeQuery(query string, fetchResults bool) (map[string]interface{}, error) {
	config, err := getDbConfig()
	if err != nil {
		return nil, err
	}

	db, err := getConnection(config)
	if err != nil {
		return nil, fmt.Errorf("database connection error: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.QueryTimeout)*time.Second)
	defer cancel()

	if fetchResults {
		// Execute query and fetch results
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		result := make([]map[string]interface{}, 0)

		for rows.Next() {
			// Create a slice of interface{} to hold the values
			values := make([]interface{}, len(columns))
			scanArgs := make([]interface{}, len(columns))

			for i := range values {
				scanArgs[i] = &values[i]
			}

			// Scan the result into the values slice
			if err := rows.Scan(scanArgs...); err != nil {
				return nil, err
			}

			// Create a map for this row's data
			rowData := make(map[string]interface{})
			for i, colName := range columns {
				val := values[i]

				// Convert to appropriate Go type
				if val == nil {
					rowData[colName] = nil
				} else {
					// Handle different types
					switch v := val.(type) {
					case []byte:
						rowData[colName] = string(v)
					default:
						rowData[colName] = v
					}
				}
			}

			result = append(result, rowData)
		}

		if err = rows.Err(); err != nil {
			return nil, err
		}

		return map[string]interface{}{
			"columns": columns,
			"rows":    result,
		}, nil
	} else {
		// Execute non-select query
		res, err := db.ExecContext(ctx, query)
		if err != nil {
			return nil, err
		}

		rowCount, _ := res.RowsAffected()
		return map[string]interface{}{
			"rowCount": rowCount,
		}, nil
	}
}

func formatResults(data map[string]interface{}) (string, error) {
	columns, hasColumns := data["columns"].([]string)
	if !hasColumns {
		rowCount, hasRowCount := data["rowCount"].(int64)
		if hasRowCount {
			return fmt.Sprintf("Query executed successfully. Rows affected: %d", rowCount), nil
		}
		return "", errors.New("unknown result format")
	}

	rows, hasRows := data["rows"].([]map[string]interface{})
	if !hasRows {
		return "No results found", nil
	}

	if len(rows) == 0 {
		return "No results found", nil
	}

	// Format the results in a tabular format
	var result strings.Builder
	result.WriteString(strings.Join(columns, ","))
	result.WriteString("\n")

	for _, row := range rows {
		values := make([]string, len(columns))
		for i, col := range columns {
			val := row[col]
			if val == nil {
				values[i] = ""
			} else {
				values[i] = fmt.Sprintf("%v", val)
			}
		}
		result.WriteString(strings.Join(values, ","))
		result.WriteString("\n")
	}

	return result.String(), nil
}

func main() {
	// Create MCP server
	s := server.NewMCPServer(
		"MSSQL MCP Server", // Server name
		"1.0.0",            // Version
		server.WithLogging(),
		server.WithRecovery(),
	)

	// Add execute_sql tool
	sqlTool := mcp.NewTool("execute_sql",
		mcp.WithDescription("Execute a read-only SQL query on the MSSQL server. Write operations (CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, etc.) are not permitted."),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("The SQL query to execute (read-only operations only)"),
		),
	)

	// Add tool handler
	s.AddTool(sqlTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		query, ok := request.Params.Arguments["query"].(string)
		if !ok || query == "" {
			return mcp.NewToolResultError("Query is required"), nil
		}

		log.Printf("Executing SQL query: %s", query)

		// Check if the query is a write operation
		if isWriteOperation(query) {
			errorMessage := "Write operations (CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, etc.) are not permitted for security reasons."
			log.Printf("Attempted write operation denied: %s", truncateString(query, 100))
			return mcp.NewToolResultError(errorMessage), nil
		}

		// Special handling for "SHOW TABLES" query
		if regexp.MustCompile(`(?i)^\s*SHOW\s+TABLES\s*$`).MatchString(query) {
			config, err := getDbConfig()
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("Configuration error: %v", err)), nil
			}

			showTablesQuery := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"
			data, err := executeQuery(showTablesQuery, true)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("Error executing query: %v", err)), nil
			}

			rows := data["rows"].([]map[string]interface{})
			var result strings.Builder
			result.WriteString(fmt.Sprintf("Tables_in_%s\n", config.Database))
			for _, row := range rows {
				tableName := row["TABLE_NAME"]
				result.WriteString(fmt.Sprintf("%v\n", tableName))
			}
			return mcp.NewToolResultText(result.String()), nil
		}

		// For all other queries
		try := func() (*mcp.CallToolResult, error) {
			data, err := executeQuery(query, true)
			if err != nil {
				log.Printf("Error executing SQL '%s': %v", query, err)
				return mcp.NewToolResultError(fmt.Sprintf("Error executing query: %v", err)), nil
			}

			formattedResult, err := formatResults(data)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("Error formatting results: %v", err)), nil
			}

			return mcp.NewToolResultText(formattedResult), nil
		}

		// Execute with recovery
		result, err := try()
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Unexpected error: %v", err)), nil
		}
		return result, nil
	})

	// Initialize and log configuration
	config, err := getDbConfig()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}
	log.Printf("Database config: %s/%s as %s", config.Server, config.Database, config.User)

	// Start the server
	log.Printf("Starting MSSQL MCP server...")
	if err := server.ServeStdio(s); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
