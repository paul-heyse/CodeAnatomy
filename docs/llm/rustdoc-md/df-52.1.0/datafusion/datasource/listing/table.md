**datafusion > datasource > listing > table**

# Module: datasource::listing::table

## Contents

**Traits**

- [`ListingTableConfigExt`](#listingtableconfigext) - Extension trait for [`ListingTableConfig`] that supports inferring schemas

---

## datafusion::datasource::listing::table::ListingTableConfigExt

*Trait*

Extension trait for [`ListingTableConfig`] that supports inferring schemas

This trait exists because the following inference methods only
work for [`SessionState`] implementations of [`Session`].
See [`ListingTableConfig`] for the remaining inference methods.

**Methods:**

- `infer_options`: Infer `ListingOptions` based on `table_path` and file suffix.
- `infer`: Convenience method to call both [`Self::infer_options`] and [`ListingTableConfig::infer_schema`]



