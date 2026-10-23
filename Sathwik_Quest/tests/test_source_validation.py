"""Failure-case tests for population source validation."""

import copy
import json
import unittest
from pathlib import Path

from ingestion.download import validate_population


BASELINE = Path(
    ".local/snapshots/"
    "20261010T224411Z_70a1d510014d4a1bab5107f7131d1c53/"
    "population/population.json"
)


class PopulationValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.baseline = json.loads(
            BASELINE.read_text(encoding="utf-8")
        )

    def setUp(self):
        self.payload = copy.deepcopy(self.baseline)

    def test_valid_baseline_is_accepted(self):
        count = validate_population(self.payload)
        self.assertEqual(count, len(self.payload["data"]))

    def test_duplicate_year_is_rejected(self):
        self.payload["data"].append(
            copy.deepcopy(self.payload["data"][0])
        )
        self.payload["page"]["total"] = len(self.payload["data"])

        with self.assertRaisesRegex(ValueError, "Duplicate population key"):
            validate_population(self.payload)

    def test_missing_required_year_is_rejected(self):
        self.payload["data"] = [
            row for row in self.payload["data"]
            if row["Year"] != 2013
        ]
        self.payload["page"]["total"] = len(self.payload["data"])

        with self.assertRaisesRegex(
            ValueError, "Assessment population years missing"
        ):
            validate_population(self.payload)

    def test_fractional_population_is_rejected(self):
        self.payload["data"][0]["Population"] = 316128839.5

        with self.assertRaisesRegex(ValueError, "Invalid population value"):
            validate_population(self.payload)

    def test_negative_population_is_rejected(self):
        self.payload["data"][0]["Population"] = -1

        with self.assertRaisesRegex(ValueError, "Invalid population value"):
            validate_population(self.payload)

    def test_incomplete_response_is_rejected(self):
        self.payload["page"]["total"] = len(self.payload["data"]) + 1

        with self.assertRaisesRegex(ValueError, "response is incomplete"):
            validate_population(self.payload)

    def test_wrong_nation_is_rejected(self):
        self.payload["data"][0]["Nation ID"] = "unexpected"

        with self.assertRaisesRegex(ValueError, "Unexpected nation"):
            validate_population(self.payload)

    def test_missing_population_field_is_rejected(self):
        del self.payload["data"][0]["Population"]

        with self.assertRaisesRegex(ValueError, "missing required fields"):
            validate_population(self.payload)


if __name__ == "__main__":
    unittest.main(verbosity=2)