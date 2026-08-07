import sys
import unittest
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parents[2] / "scripts" / "pipelines"
sys.path.insert(0, str(PIPELINE_DIR))

from registry import (  # noqa: E402
    load_data_products,
    load_experiments,
    load_factors,
    load_idea_lineage,
    load_legacy_compute_inventory,
    load_quality_gates,
    required_gates,
    validate_registries,
)


class RegistryTest(unittest.TestCase):
    def test_registries_are_valid(self) -> None:
        self.assertEqual(validate_registries(), [])

    def test_idea_lineage_covers_every_current_id(self) -> None:
        lineage = load_idea_lineage()
        self.assertEqual({row["id"] for row in lineage["factor_lineage"]}, set(load_factors()))
        self.assertEqual(
            {item_id for row in lineage["research_lineage"] for item_id in row["ids"]},
            set(load_experiments()),
        )
        self.assertEqual(lineage["unmapped_legacy_ideas"], [])

    def test_research_entries_are_falsifiable(self) -> None:
        for row in load_experiments().values():
            self.assertTrue(row["research_question"])
            self.assertTrue(row["research_outputs"])
            self.assertTrue(row["decision_rule"])

    def test_legacy_inventory_classifies_every_current_id_once(self) -> None:
        inventory = load_legacy_compute_inventory()

        def collect(groups: dict) -> list[str]:
            return [
                item_id
                for rows in groups.values()
                for row in rows
                for item_id in row.get("ids", [row["id"]] if "id" in row else [])
            ]

        factor_ids = collect(inventory["factors"])
        research_ids = collect(inventory["research"])
        self.assertEqual(len(factor_ids), len(set(factor_ids)))
        self.assertEqual(set(factor_ids), set(load_factors()))
        self.assertEqual(len(research_ids), len(set(research_ids)))
        self.assertEqual(set(research_ids), set(load_experiments()))

    def test_engineering_items_are_not_research_experiments(self) -> None:
        experiments = load_experiments()
        aliases = {alias for row in experiments.values() for alias in row.get("legacy_aliases", [])}
        self.assertNotIn("PB01", aliases)
        self.assertNotIn("BATCH01", aliases)
        self.assertNotIn("RG01", aliases)
        self.assertIn("P001", load_data_products())
        self.assertIn("Q003", load_quality_gates())

    def test_dependencies_resolve_and_inherit_quality_gates(self) -> None:
        factors = load_factors()
        products = load_data_products()
        for experiment in load_experiments().values():
            self.assertTrue(set(experiment["factor_dependencies"]) <= set(factors))
            self.assertTrue(set(experiment["data_dependencies"]) <= set(products))
        gates = required_gates(["F008"], ["P001"])
        self.assertIn("Q003", gates)
        self.assertIn("Q006", gates)


if __name__ == "__main__":
    unittest.main()
