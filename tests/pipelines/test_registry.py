import sys
import unittest
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parents[2] / "scripts" / "pipelines"
sys.path.insert(0, str(PIPELINE_DIR))

from registry import load_experiments, load_factors, validate_registries  # noqa: E402


class RegistryTest(unittest.TestCase):
    def test_registries_are_valid(self) -> None:
        self.assertEqual(validate_registries(), [])

    def test_prebook_impact_classes_are_explicit(self) -> None:
        factors = load_factors()
        self.assertEqual(factors["F001"]["affected_by_prebook_fix"], "required")
        self.assertEqual(factors["F002"]["affected_by_prebook_fix"], "conditional")
        self.assertEqual(factors["F003"]["affected_by_prebook_fix"], "no")

    def test_experiment_dependencies_resolve(self) -> None:
        experiments = load_experiments()
        factors = load_factors()
        for experiment in experiments.values():
            self.assertTrue(set(experiment["factor_dependencies"]) <= set(factors))
            self.assertTrue(set(experiment["experiment_dependencies"]) <= set(experiments))


if __name__ == "__main__":
    unittest.main()
