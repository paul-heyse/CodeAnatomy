//! WS6.5: Join/Union Stability Harness.
//!
//! Targeted performance and correctness tests for complex plan combination patterns.
//! The actual tests live in `tests/join_union_stability.rs`.
//!
//! This module provides the infrastructure for stability testing:
//! - Fixture traits for test case definition
//! - Result types for test execution
//! - Metrics collection and validation

/// Marker trait for stability test fixtures.
///
/// Implementations should define test cases that stress specific join/union patterns:
/// - Multi-way joins with different join types
/// - Union-over-join patterns
/// - Join-over-union patterns
/// - Nested subquery patterns
/// - Partition skew scenarios
///
/// # Example
///
/// ```
/// use codeanatomy_engine::stability::harness::StabilityFixture;
///
/// struct MultiWayJoinFixture;
///
/// impl StabilityFixture for MultiWayJoinFixture {
///     fn name(&self) -> &str {
///         "multi_way_join"
///     }
///
///     fn description(&self) -> &str {
///         "Tests correctness and performance of 5-way star join"
///     }
/// }
/// ```
pub trait StabilityFixture {
    /// Name of the test fixture (used for reporting).
    fn name(&self) -> &str;

    /// Human-readable description of what the test validates.
    fn description(&self) -> &str;

    /// Expected result characteristics for pass/fail determination.
    ///
    /// Returns (expected_rows_min, expected_rows_max, max_elapsed_ms).
    /// Default implementation returns (0, u64::MAX, u64::MAX) (permissive).
    fn expected_characteristics(&self) -> (u64, u64, u64) {
        (0, u64::MAX, u64::MAX)
    }

    /// Whether this fixture requires exact row count validation.
    ///
    /// If true, the test will fail if the actual row count doesn't match
    /// the expected range exactly. If false, row count is advisory only.
    fn strict_row_count(&self) -> bool {
        false
    }
}

/// Result of a stability test run.
///
/// Captures execution metrics and pass/fail status for a single test case.
#[derive(Debug, Clone)]
pub struct StabilityResult {
    /// Name of the test that was run.
    pub test_name: String,

    /// Whether the test passed all validation criteria.
    pub passed: bool,

    /// Total elapsed time in milliseconds.
    pub elapsed_ms: u64,

    /// Number of rows produced by the query.
    pub rows_produced: u64,

    /// Number of partitions used during execution.
    pub partition_count: usize,

    /// Additional notes, warnings, or failure reasons.
    pub notes: Vec<String>,
}

impl StabilityResult {
    /// Create a new StabilityResult.
    pub fn new(test_name: String, passed: bool, elapsed_ms: u64) -> Self {
        Self {
            test_name,
            passed,
            elapsed_ms,
            rows_produced: 0,
            partition_count: 0,
            notes: Vec::new(),
        }
    }

    /// Add a note to the result.
    pub fn add_note(&mut self, note: impl Into<String>) {
        self.notes.push(note.into());
    }

    /// Check if the result satisfies the expected characteristics.
    ///
    /// Returns true if:
    /// - Row count is within expected range (if strict)
    /// - Elapsed time is under the threshold
    pub fn satisfies_characteristics(
        &self,
        expected_rows_min: u64,
        expected_rows_max: u64,
        max_elapsed_ms: u64,
        strict_row_count: bool,
    ) -> bool {
        let row_count_ok = if strict_row_count {
            self.rows_produced >= expected_rows_min && self.rows_produced <= expected_rows_max
        } else {
            true // Non-strict mode doesn't validate row count
        };

        let elapsed_ok = self.elapsed_ms <= max_elapsed_ms;

        row_count_ok && elapsed_ok
    }

    /// Format the result as a human-readable summary.
    pub fn summary(&self) -> String {
        let status = if self.passed { "PASS" } else { "FAIL" };
        let mut summary = format!(
            "[{}] {} - {}ms, {} rows, {} partitions",
            status, self.test_name, self.elapsed_ms, self.rows_produced, self.partition_count
        );

        if !self.notes.is_empty() {
            summary.push_str("\n  Notes:");
            for note in &self.notes {
                summary.push_str(&format!("\n    - {}", note));
            }
        }

        summary
    }
}

/// Builder for StabilityResult.
#[derive(Debug, Clone)]
pub struct StabilityResultBuilder {
    test_name: String,
    passed: bool,
    elapsed_ms: u64,
    rows_produced: u64,
    partition_count: usize,
    notes: Vec<String>,
}

impl StabilityResultBuilder {
    /// Create a new builder with the test name.
    pub fn new(test_name: impl Into<String>) -> Self {
        Self {
            test_name: test_name.into(),
            passed: true, // Default to passed, failures set this to false
            elapsed_ms: 0,
            rows_produced: 0,
            partition_count: 0,
            notes: Vec::new(),
        }
    }

    /// Set whether the test passed.
    pub fn passed(mut self, passed: bool) -> Self {
        self.passed = passed;
        self
    }

    /// Set the elapsed time in milliseconds.
    pub fn elapsed_ms(mut self, elapsed_ms: u64) -> Self {
        self.elapsed_ms = elapsed_ms;
        self
    }

    /// Set the number of rows produced.
    pub fn rows_produced(mut self, rows_produced: u64) -> Self {
        self.rows_produced = rows_produced;
        self
    }

    /// Set the partition count.
    pub fn partition_count(mut self, partition_count: usize) -> Self {
        self.partition_count = partition_count;
        self
    }

    /// Add a note.
    pub fn note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }

    /// Build the final StabilityResult.
    pub fn build(self) -> StabilityResult {
        StabilityResult {
            test_name: self.test_name,
            passed: self.passed,
            elapsed_ms: self.elapsed_ms,
            rows_produced: self.rows_produced,
            partition_count: self.partition_count,
            notes: self.notes,
        }
    }
}

/// Collection of stability test results.
#[derive(Debug, Clone)]
pub struct StabilityReport {
    /// Individual test results.
    pub results: Vec<StabilityResult>,

    /// Overall pass/fail status (true if all tests passed).
    pub all_passed: bool,

    /// Total elapsed time across all tests.
    pub total_elapsed_ms: u64,
}

impl StabilityReport {
    /// Create a new empty report.
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
            all_passed: true,
            total_elapsed_ms: 0,
        }
    }

    /// Add a test result to the report.
    pub fn add_result(&mut self, result: StabilityResult) {
        if !result.passed {
            self.all_passed = false;
        }
        self.total_elapsed_ms += result.elapsed_ms;
        self.results.push(result);
    }

    /// Get the number of tests that passed.
    pub fn passed_count(&self) -> usize {
        self.results.iter().filter(|r| r.passed).count()
    }

    /// Get the number of tests that failed.
    pub fn failed_count(&self) -> usize {
        self.results.iter().filter(|r| !r.passed).count()
    }

    /// Format the report as a human-readable summary.
    pub fn summary(&self) -> String {
        let total = self.results.len();
        let passed = self.passed_count();
        let failed = self.failed_count();
        let status = if self.all_passed { "PASS" } else { "FAIL" };

        let mut summary = format!(
            "Stability Report [{}]\n  Total: {} tests, {} passed, {} failed, {}ms total\n",
            status, total, passed, failed, self.total_elapsed_ms
        );

        for result in &self.results {
            summary.push_str(&format!("\n{}", result.summary()));
        }

        summary
    }
}

impl Default for StabilityReport {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stability_fixture_defaults() {
        struct MinimalFixture;

        impl StabilityFixture for MinimalFixture {
            fn name(&self) -> &str {
                "minimal"
            }

            fn description(&self) -> &str {
                "Minimal fixture"
            }
        }

        let fixture = MinimalFixture;
        assert_eq!(fixture.name(), "minimal");
        assert_eq!(fixture.expected_characteristics(), (0, u64::MAX, u64::MAX));
        assert!(!fixture.strict_row_count());
    }

    #[test]
    fn test_stability_result_builder() {
        let result = StabilityResultBuilder::new("test")
            .passed(true)
            .elapsed_ms(500)
            .rows_produced(150)
            .partition_count(4)
            .note("Test note")
            .build();

        assert_eq!(result.test_name, "test");
        assert!(result.passed);
        assert_eq!(result.elapsed_ms, 500);
        assert_eq!(result.rows_produced, 150);
        assert_eq!(result.partition_count, 4);
        assert_eq!(result.notes.len(), 1);
    }

    #[test]
    fn test_stability_result_satisfies_characteristics() {
        let result = StabilityResultBuilder::new("test")
            .elapsed_ms(500)
            .rows_produced(150)
            .build();

        // Within bounds (strict)
        assert!(result.satisfies_characteristics(100, 200, 1000, true));

        // Outside row count bounds (strict)
        assert!(!result.satisfies_characteristics(100, 120, 1000, true));

        // Outside row count bounds (non-strict)
        assert!(result.satisfies_characteristics(100, 120, 1000, false));

        // Elapsed time exceeds threshold
        assert!(!result.satisfies_characteristics(100, 200, 400, true));
    }

    #[test]
    fn test_stability_result_summary() {
        let result = StabilityResultBuilder::new("test")
            .passed(true)
            .elapsed_ms(500)
            .rows_produced(150)
            .partition_count(4)
            .note("Note 1")
            .note("Note 2")
            .build();

        let summary = result.summary();
        assert!(summary.contains("[PASS]"));
        assert!(summary.contains("test"));
        assert!(summary.contains("500ms"));
        assert!(summary.contains("150 rows"));
        assert!(summary.contains("4 partitions"));
        assert!(summary.contains("Note 1"));
        assert!(summary.contains("Note 2"));
    }

    #[test]
    fn test_stability_report() {
        let mut report = StabilityReport::new();

        let result1 = StabilityResultBuilder::new("test1")
            .passed(true)
            .elapsed_ms(100)
            .build();

        let result2 = StabilityResultBuilder::new("test2")
            .passed(false)
            .elapsed_ms(200)
            .note("Failed validation")
            .build();

        report.add_result(result1);
        report.add_result(result2);

        assert_eq!(report.results.len(), 2);
        assert!(!report.all_passed);
        assert_eq!(report.total_elapsed_ms, 300);
        assert_eq!(report.passed_count(), 1);
        assert_eq!(report.failed_count(), 1);

        let summary = report.summary();
        assert!(summary.contains("[FAIL]"));
        assert!(summary.contains("Total: 2 tests"));
        assert!(summary.contains("1 passed, 1 failed"));
    }

    #[test]
    fn test_stability_report_all_passed() {
        let mut report = StabilityReport::new();

        let result1 = StabilityResultBuilder::new("test1")
            .passed(true)
            .elapsed_ms(100)
            .build();

        let result2 = StabilityResultBuilder::new("test2")
            .passed(true)
            .elapsed_ms(200)
            .build();

        report.add_result(result1);
        report.add_result(result2);

        assert!(report.all_passed);
        assert_eq!(report.passed_count(), 2);
        assert_eq!(report.failed_count(), 0);

        let summary = report.summary();
        assert!(summary.contains("[PASS]"));
    }
}
