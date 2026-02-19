# df_plugin_api Changelog

## Unreleased

- Documented ABI compatibility policy for `DfPluginExportsV1` and capability bits.
- Clarified planner-extension capability expectations:
  - Capability bits must only be advertised when a matching executable surface exists.
  - Unknown capability bits are treated as unsupported by hosts.
- Added `DfPluginMod::relation_planner_names` root-module export hook for planner-extension parity.

## 1.2

- ABI minor advanced to 2 (`DF_PLUGIN_ABI_MINOR=2`) for relation-planner export support.

## 1.1

- Current ABI baseline (`DF_PLUGIN_ABI_MAJOR=1`, `DF_PLUGIN_ABI_MINOR=1`).
