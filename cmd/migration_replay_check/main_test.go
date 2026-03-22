// main_test.go verifies the small migration-replay helper functions that discover and read repository migration files.
package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMustReadMigrationFileReturnsExactContents verifies that the helper returns the committed SQL contents without modification.
func TestMustReadMigrationFileReturnsExactContents(t *testing.T) {
	tempDir := t.TempDir()                                                          // Allocate one isolated temporary directory so the helper can read a synthetic migration file safely.
	migrationPath := filepath.Join(tempDir, "001_test.sql")                         // Build one synthetic migration path inside the isolated temporary directory.
	expectedSQL := "BEGIN;\nSELECT 1;\nCOMMIT;\n"                                   // Define one stable SQL payload so the helper can be compared against exact file contents.
	if err := os.WriteFile(migrationPath, []byte(expectedSQL), 0o644); err != nil { // Write the synthetic migration file that the helper should read back verbatim.
		t.Fatalf("failed to write migration file: %v", err) // Surface the setup failure because the helper cannot be exercised without a real file.
	}

	if got := mustReadMigrationFile(migrationPath); got != expectedSQL { // Read the synthetic migration file through the production helper so file-content handling is exercised directly.
		t.Fatalf("expected SQL %q, got %q", expectedSQL, got) // Surface the unexpected contents so replay-helper regressions are obvious.
	}
}

// TestMustListMigrationFilesSortsLexically verifies that the helper returns migration files in lexical order from a repository-shaped directory tree.
func TestMustListMigrationFilesSortsLexically(t *testing.T) {
	tempRepo := t.TempDir()                                                                   // Allocate one isolated temporary repository root so the helper can discover synthetic migration files safely.
	migrationsDir := filepath.Join(tempRepo, "internal", "storage", "postgres", "migrations") // Build the repository-shaped migrations directory expected by the production helper.
	if err := os.MkdirAll(migrationsDir, 0o755); err != nil {                                 // Create the repository-shaped migrations directory before writing any synthetic files into it.
		t.Fatalf("failed to create migration directory: %v", err) // Surface the setup failure because the helper cannot discover files without the expected directory tree.
	}
	for _, name := range []string{"005_b.sql", "001_a.sql", "010_c.sql"} { // Seed the synthetic repository directory with out-of-order migration filenames so lexical sorting can be asserted.
		if err := os.WriteFile(filepath.Join(migrationsDir, name), []byte("-- test\n"), 0o644); err != nil { // Write each synthetic migration file so the production helper can discover it from disk.
			t.Fatalf("failed to write migration file %s: %v", name, err) // Surface the setup failure because the helper cannot discover missing files.
		}
	}

	files := mustListMigrationFiles(tempRepo) // Discover the synthetic migration files through the production helper so lexical sorting is exercised directly.
	expected := []string{                     // Define the lexical order expected from the helper.
		filepath.Join(migrationsDir, "001_a.sql"), // Expect the lexically smallest migration first.
		filepath.Join(migrationsDir, "005_b.sql"), // Expect the middle migration second.
		filepath.Join(migrationsDir, "010_c.sql"), // Expect the lexically largest migration last.
	}
	if len(files) != len(expected) { // Fail the test when the helper returns an unexpected number of migration files.
		t.Fatalf("expected %d migration files, got %d", len(expected), len(files)) // Surface the unexpected discovery count so repository-scan regressions are obvious.
	}
	for index := range expected { // Compare each returned migration path in order so lexical sorting regressions are obvious.
		if files[index] != expected[index] { // Fail the test when any returned migration path is out of order.
			t.Fatalf("expected migration %q at index %d, got %q", expected[index], index, files[index]) // Surface the unexpected ordering so helper regressions are easy to diagnose.
		}
	}
}
