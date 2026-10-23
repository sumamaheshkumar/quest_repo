-- Prerequisite: an account-level group named rearc_analysts exists.
-- These grants are additive; review inherited permissions separately.

GRANT USE CATALOG ON CATALOG workspace TO `rearc_analysts`;
GRANT USE SCHEMA ON SCHEMA workspace.rearc TO `rearc_analysts`;

GRANT SELECT ON TABLE workspace.rearc.gold_population_statistics
TO `rearc_analysts`;

GRANT SELECT ON TABLE workspace.rearc.gold_best_year
TO `rearc_analysts`;

GRANT SELECT ON TABLE workspace.rearc.gold_target_series_population
TO `rearc_analysts`;
