package logstore

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// refreshBackendCount counts sessions running a REFRESH MATERIALIZED VIEW.
func refreshBackendCount(t *testing.T, db *gorm.DB) int {
	t.Helper()
	var n int
	require.NoError(t, db.Raw(`SELECT count(*) FROM pg_stat_activity
		WHERE state = 'active' AND ltrim(query) ILIKE 'REFRESH MATERIALIZED VIEW%'`).Scan(&n).Error)
	return n
}

// backendAlive reports whether the backend with pid still exists.
func backendAlive(t *testing.T, db *gorm.DB, pid int) bool {
	t.Helper()
	var n int
	require.NoError(t, db.Raw("SELECT count(*) FROM pg_stat_activity WHERE pid = ?", pid).Scan(&n).Error)
	return n > 0
}

// blockMatViewRefresh holds an ACCESS EXCLUSIVE lock on logs in an
// open transaction, so any REFRESH of it waits in Postgres until the returned
// release runs. That stands in for a pass that runs for hours.
func blockMatViewRefresh(t *testing.T, db *gorm.DB) (release func()) {
	t.Helper()
	tx := db.Begin()
	require.NoError(t, tx.Error)
	require.NoError(t, tx.Exec("LOCK TABLE logs IN ACCESS EXCLUSIVE MODE").Error)
	released := false
	release = func() {
		if !released {
			released = true
			tx.Rollback()
		}
	}
	t.Cleanup(release)
	return release
}

// deadClientHelperEnv makes the test binary act as the refresher process that
// TestMatViewSessionDeadClientReleasesRefreshLock kills. Its value says whether
// the helper applies the matview session bounds ("bounded") or not ("plain").
const deadClientHelperEnv = "BIFROST_MATVIEW_DEAD_CLIENT_HELPER"

// TestMatViewDeadClientHelper is not a test on its own: run as a child process,
// it takes the refresh lock and runs a long statement until it is killed.
func TestMatViewDeadClientHelper(t *testing.T) {
	mode := os.Getenv(deadClientHelperEnv)
	if mode == "" {
		t.Skip("helper process only")
	}
	db := pauseTestDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	ctx := context.Background()
	conn, err := sqlDB.Conn(ctx)
	require.NoError(t, err)
	if mode == "bounded" {
		_, err = matViewSessionLimits{}.apply(ctx, conn, 0)
		require.NoError(t, err)
	}
	var acquired bool
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", matviewRefreshAdvisoryLockKey).Scan(&acquired))
	require.True(t, acquired)
	_, _ = conn.ExecContext(ctx, "SELECT pg_sleep(300) /* matview dead client helper */")
}

// startDeadClientHelper runs TestMatViewDeadClientHelper in a child process and
// returns it with the backend pid it is running on, once its statement runs.
func startDeadClientHelper(t *testing.T, observer *gorm.DB, mode string) (*exec.Cmd, int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestMatViewDeadClientHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), deadClientHelperEnv+"="+mode)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	var pid int
	require.Eventually(t, func() bool {
		return observer.Raw(`SELECT COALESCE(max(pid), 0) FROM pg_stat_activity
			WHERE state = 'active' AND query LIKE '%matview dead client helper%' AND pid <> pg_backend_pid()`).Scan(&pid).Error == nil && pid != 0
	}, 20*time.Second, 100*time.Millisecond, "helper statement never started")
	return cmd, pid
}

// TestMatViewSessionDeadClientReleasesRefreshLock reproduces a pod killed mid
// refresh: the process holding the refresh lock gets SIGKILL, so no cancel
// request is ever sent. The client check on the bounded session must make the
// server end the statement and the session, which frees the refresh lock. The
// unbounded control shows the gap this closes: that backend keeps running.
func TestMatViewSessionDeadClientReleasesRefreshLock(t *testing.T) {
	if os.Getenv(deadClientHelperEnv) != "" {
		t.Skip("running as helper")
	}
	observer := pauseTestDB(t)

	t.Run("unbounded session outlives its killed client", func(t *testing.T) {
		cmd, pid := startDeadClientHelper(t, observer, "plain")
		require.NoError(t, cmd.Process.Kill())
		_ = cmd.Wait()
		time.Sleep(3 * time.Second)
		require.True(t, backendAlive(t, observer, pid), "without the client check the statement keeps running")
		require.NoError(t, observer.Exec("SELECT pg_terminate_backend(?)", pid).Error)
		require.Eventually(t, func() bool { return !backendAlive(t, observer, pid) }, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("bounded session ends with its killed client", func(t *testing.T) {
		cmd, pid := startDeadClientHelper(t, observer, "bounded")
		require.NoError(t, cmd.Process.Kill())
		_ = cmd.Wait()
		require.Eventually(t, func() bool { return !backendAlive(t, observer, pid) },
			matViewClientCheckInterval+10*time.Second, 200*time.Millisecond,
			"the server must end a statement whose client is gone")
		other := awaitTestAdvisoryLock(t, observer, matviewRefreshAdvisoryLockKey, 5*time.Second)
		releaseTestAdvisoryLock(t, other, matviewRefreshAdvisoryLockKey)
	})
}

// TestMatViewServerStatementTimeout pins the per-pass server bound: the time
// left on ctx plus the grace, and none without a deadline.
func TestMatViewServerStatementTimeout(t *testing.T) {
	require.Zero(t, matViewServerStatementTimeout(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	got := matViewServerStatementTimeout(ctx)
	require.Greater(t, got, time.Minute+matViewServerTimeoutGrace-5*time.Second)
	require.LessOrEqual(t, got, time.Minute+matViewServerTimeoutGrace)

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	require.Equal(t, matViewServerTimeoutGrace, matViewServerStatementTimeout(expired))
}

// TestCloseCancelsRunningMatViewRefresh pins graceful shutdown: Close must
// cancel a REFRESH that is still running on the server and free the refresh
// lock, instead of leaving it running after the process exits.
func TestCloseCancelsRunningMatViewRefresh(t *testing.T) {
	_, db := setupPerfTestDB(t)
	observer := pauseTestDB(t)
	resetTestMatViewRefreshGate()
	blockMatViewRefresh(t, observer)

	storeDB := pauseTestDB(t)
	store := &RDBLogStore{db: storeDB, logger: testLogger{}}
	store.maintenanceCtx, store.maintenanceCancel = context.WithCancel(context.Background())
	passErr := make(chan error, 1)
	require.True(t, store.goMaintenance(func() {
		ctx, cancel := context.WithTimeout(store.maintenanceContext(), time.Hour)
		defer cancel()
		passErr <- refreshMatViewsLimited(ctx, storeDB, 0, store.matViewLimits)
	}))
	require.Eventually(t, func() bool { return refreshBackendCount(t, observer) == 1 },
		10*time.Second, 50*time.Millisecond, "the refresh must be running before Close")

	started := time.Now()
	require.NoError(t, store.Close(context.Background()))
	require.Less(t, time.Since(started), matViewShutdownWait, "Close must not wait out the refresh")

	err := <-passErr
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(store.maintenanceCtx.Err(), context.Canceled))
	require.Zero(t, refreshBackendCount(t, observer), "no REFRESH may keep running after Close")
	other := awaitTestAdvisoryLock(t, db, matviewRefreshAdvisoryLockKey, 5*time.Second)
	releaseTestAdvisoryLock(t, other, matviewRefreshAdvisoryLockKey)

	require.False(t, store.goMaintenance(func() {}), "no maintenance may start after Close")
}

// TestCloseWithoutMaintenance pins that Close still works on a store built
// without newPostgresLogStore (no maintenance context).
func TestCloseWithoutMaintenance(t *testing.T) {
	db := pauseTestDB(t)
	store := &RDBLogStore{db: db, logger: testLogger{}}
	require.NoError(t, store.Close(context.Background()))
}

// TestMatViewTickStateTimeoutStreak pins the repeated-timeout warning: it fires
// once when the streak reaches the threshold and re-arms after a success.
func TestMatViewTickStateTimeoutStreak(t *testing.T) {
	var s matViewTickState
	require.False(t, s.recordOutcome(true))
	require.True(t, s.recordOutcome(true), "second timeout in a row warns")
	require.False(t, s.recordOutcome(true), "warn once per streak")
	require.False(t, s.recordOutcome(false))
	require.Zero(t, s.consecutiveTimeouts)
	require.False(t, s.recordOutcome(true))
	require.True(t, s.recordOutcome(true), "a new streak warns again")
}

// TestResolveMatViewTempFileLimit pins config handling: valid values pass,
// invalid ones are dropped so they cannot fail every pass.
func TestResolveMatViewTempFileLimit(t *testing.T) {
	require.Equal(t, "", resolveMatViewTempFileLimit("", testLogger{}))
	require.Equal(t, "20GB", resolveMatViewTempFileLimit(" 20GB ", testLogger{}))
	require.Equal(t, "", resolveMatViewTempFileLimit("20 gigs", testLogger{}))
}
