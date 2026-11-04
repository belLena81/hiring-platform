"""Behaviour of the spec verdict gate. Run: python3 -I -m unittest discover -s scripts/tests -p 'test_*.py'"""

import importlib.util
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "check-spec-verdicts.py"
spec = importlib.util.spec_from_file_location("check_spec_verdicts", SCRIPT)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

COMPLETE = (
    "- Code Reviewer verdict (2026-10-16, S1): PASS; findings fixed.\n"
    "- Security Engineer verdict: not applicable - tests only, no data access or authorization path.\n"
    "- Final independent QA verdict: PASS\n"
)


class VerdictLines(unittest.TestCase):
    def test_pass_with_scope_and_documented_not_applicable_are_accepted(self):
        self.assertEqual([], gate.problems_in(COMPLETE))

    def test_pending_blocked_fail_and_conditional_are_rejected(self):
        for value in ("pending", "BLOCKED", "FAIL: H1 open", "CONDITIONAL PASS", "PASS-with-FAIL", "PASSED"):
            text = COMPLETE.replace("Final independent QA verdict: PASS", f"Final independent QA verdict: {value}")
            self.assertEqual(1, len(gate.problems_in(text)), value)

    def test_not_applicable_needs_a_reason_of_at_least_ten_characters(self):
        short = COMPLETE.replace("tests only, no data access or authorization path.", "n/a")
        self.assertEqual(1, len(gate.problems_in(short)))
        bare = COMPLETE.replace("not applicable - tests only, no data access or authorization path.", "not applicable")
        self.assertEqual(1, len(gate.problems_in(bare)))

    def test_a_scope_with_colons_in_parentheses_is_read_to_its_verdict(self):
        scoped = COMPLETE.replace("(2026-10-16, S1)", "(2026-10-16, S1: harness scripts, tests: all)")
        self.assertEqual([], gate.problems_in(scoped))
        failing = scoped.replace("all): PASS; findings fixed.", "all): FAIL; findings open.")
        self.assertEqual(1, len(gate.problems_in(failing)))

    def test_an_unedited_placeholder_reason_is_rejected(self):
        placeholder = COMPLETE.replace("tests only, no data access or authorization path.", "<tier reason>")
        self.assertEqual(1, len(gate.problems_in(placeholder)))

    def test_each_missing_role_line_is_reported_by_name(self):
        problems = gate.problems_in("- Code Reviewer verdict: PASS\n")
        self.assertEqual(2, len(problems))
        self.assertTrue(any("Security Engineer" in p for p in problems))
        self.assertTrue(any("Final independent QA" in p for p in problems))

    def test_only_done_specs_are_gated(self):
        self.assertTrue(gate.is_done("- Status: done (local), 2026-10-14."))
        self.assertTrue(gate.is_done("# Title\n\n- Status: **done**"))
        for status in ("- Status: draft", "- Status: in review", "- Status: not done yet", "no status line"):
            self.assertFalse(gate.is_done(status), status)

    def test_only_the_first_status_line_decides(self):
        self.assertFalse(gate.is_done("- Status: in review\n\n- Status: done (slice 1)"))
        self.assertTrue(gate.is_done("- Status: done (local)\n\n- Status: accepted by the user"))

    def test_conditional_pass_is_rejected(self):
        text = COMPLETE.replace("Final independent QA verdict: PASS", "Final independent QA verdict: PASS, conditional on a fix")
        self.assertEqual(1, len(gate.problems_in(text)))


if __name__ == "__main__":
    unittest.main()
