package postgresconn

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// MaintenanceSessionOptions are the server-side bounds BoundMaintenanceSession
// applies to one dedicated connection. They exist because a client-side context
// deadline dies with the process: when the pod running a long statement is
// killed, only limits the server enforces itself still stop that statement.
type MaintenanceSessionOptions struct {
	// StatementTimeout is the server-side statement_timeout for the session.
	// Zero disables it, as DisableStatementTimeout does.
	StatementTimeout time.Duration
	// ClientCheckInterval sets client_connection_check_interval, so a backend
	// running a long statement notices that its client went away and aborts it.
	// Without it the statement runs to completion first. Zero leaves the
	// session's value unchanged.
	ClientCheckInterval time.Duration
	// TempFileLimit sets temp_file_limit (a Postgres size such as "20GB"), so one
	// statement cannot fill the server's disk with temp files. Empty leaves the
	// session's value unchanged. The parameter needs superuser or
	// GRANT SET ON PARAMETER temp_file_limit; without that it is skipped.
	TempFileLimit string
}

// MaintenanceSessionResult reports what BoundMaintenanceSession could not apply.
type MaintenanceSessionResult struct {
	// TempFileLimitDenied is true when TempFileLimit was set but the role may not
	// change temp_file_limit. The other bounds were still applied.
	TempFileLimitDenied bool
}

// tempFileLimitPattern accepts the size forms Postgres takes for temp_file_limit:
// -1 (no limit), or a whole number with an optional kB/MB/GB/TB unit.
var tempFileLimitPattern = regexp.MustCompile(`^(-1|\d+\s*(kB|MB|GB|TB)?)$`)

// ValidTempFileLimit reports whether s is a temp_file_limit value Postgres
// accepts, so a bad config value is caught at startup instead of on every pass.
func ValidTempFileLimit(s string) bool {
	return tempFileLimitPattern.MatchString(strings.TrimSpace(s))
}

// BoundMaintenanceSession applies opts to conn for long maintenance work (for
// example materialized-view refreshes) and returns a function that restores the
// previous values. Call the restore before the connection goes back to the pool,
// through RestoreStatementTimeoutOrDiscard, exactly as with DisableStatementTimeout.
// On an error part way through, the returned restore is non-nil and undoes what
// was already applied; callers must still run it.
func BoundMaintenanceSession(ctx context.Context, conn sqlExecQueryer, opts MaintenanceSessionOptions) (func(context.Context) error, MaintenanceSessionResult, error) {
	var result MaintenanceSessionResult
	var prevStatement, prevCheck, prevTemp string
	if err := conn.QueryRowContext(ctx,
		"SELECT current_setting('statement_timeout'), current_setting('client_connection_check_interval'), current_setting('temp_file_limit')",
	).Scan(&prevStatement, &prevCheck, &prevTemp); err != nil {
		return nil, result, fmt.Errorf("failed to read maintenance session settings: %w", err)
	}

	statementMillis := int64(0)
	if opts.StatementTimeout > 0 {
		statementMillis = durationMillis(opts.StatementTimeout)
	}
	if _, err := conn.ExecContext(ctx, "SELECT set_config('statement_timeout', $1, false)", fmt.Sprintf("%d", statementMillis)); err != nil {
		return nil, result, fmt.Errorf("failed to set statement_timeout: %w", err)
	}
	// restores lists the undo steps for the settings changed so far, so a failure
	// part way through still hands back a restore for what was applied.
	restores := [][2]string{{"statement_timeout", prevStatement}}
	restore := func(ctx context.Context) error {
		for _, r := range restores {
			if _, err := conn.ExecContext(ctx, "SELECT set_config($1, $2, false)", r[0], r[1]); err != nil {
				return fmt.Errorf("failed to restore %s: %w", r[0], err)
			}
		}
		return nil
	}

	if opts.ClientCheckInterval > 0 {
		if _, err := conn.ExecContext(ctx, "SELECT set_config('client_connection_check_interval', $1, false)", fmt.Sprintf("%d", durationMillis(opts.ClientCheckInterval))); err != nil {
			return restore, result, fmt.Errorf("failed to set client_connection_check_interval: %w", err)
		}
		restores = append(restores, [2]string{"client_connection_check_interval", prevCheck})
	}

	if limit := strings.TrimSpace(opts.TempFileLimit); limit != "" {
		_, err := conn.ExecContext(ctx, "SELECT set_config('temp_file_limit', $1, false)", limit)
		var pgErr *pgconn.PgError
		switch {
		case err == nil:
			restores = append(restores, [2]string{"temp_file_limit", prevTemp})
		case errors.As(err, &pgErr) && pgErr.Code == "42501":
			result.TempFileLimitDenied = true
		default:
			return restore, result, fmt.Errorf("failed to set temp_file_limit: %w", err)
		}
	}
	return restore, result, nil
}
