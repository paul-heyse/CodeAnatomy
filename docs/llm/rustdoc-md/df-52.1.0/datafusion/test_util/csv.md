**datafusion > test_util > csv**

# Module: test_util::csv

## Contents

**Structs**

- [`TestCsvFile`](#testcsvfile) - a CSV file that has been created for testing.

---

## datafusion::test_util::csv::TestCsvFile

*Struct*

a CSV file that has been created for testing.

**Methods:**

- `fn try_new<impl IntoIterator<Item = RecordBatch>>(path: PathBuf, batches: impl Trait) -> Result<Self>` - Creates a new csv file at the specified location
- `fn schema(self: &Self) -> SchemaRef` - The schema of this csv file
- `fn path(self: &Self) -> &std::path::Path` - The path to the csv file



