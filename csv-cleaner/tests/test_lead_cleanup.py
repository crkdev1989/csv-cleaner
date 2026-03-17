from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from cleaner.engine import run_cleaner
from cleaner.lead_cleanup import (
    classify_email_quality,
    classify_page_type,
    classify_phone_quality,
    is_email_clearly_junk,
    normalize_email_for_validation,
    normalize_root_domain,
)
from cleaner.pipeline import load_module
from cleaner.report import CleaningReport


class LeadCleanupTests(unittest.TestCase):
    def test_lead_cleanup_helpers_cover_real_world_classifications(self) -> None:
        self.assertEqual(
            "justicecounts.com",
            normalize_root_domain(
                "https://justicecounts.com/our-attorneys/alex-riddle/?ref=directory"
            ),
        )
        self.assertEqual("brentadams.com", normalize_root_domain("http://www.brentadams.com"))
        self.assertEqual("baitylaw.com", normalize_root_domain("http://Baitylaw.com"))
        self.assertEqual("", normalize_root_domain("not a url"))

        self.assertEqual(
            "social_profile",
            classify_page_type("https://www.linkedin.com/in/example-lawyer/"),
        )
        self.assertEqual(
            "attorney_profile",
            classify_page_type("https://justicecounts.com/attorney/alex-riddle/"),
        )
        self.assertEqual(
            "team_page",
            classify_page_type("https://justicecounts.com/our-team/"),
        )
        self.assertEqual("firm_homepage", classify_page_type("https://justicecounts.com/"))
        self.assertEqual(
            "directory_page",
            classify_page_type(
                "https://thenationaltriallawyers.org/member-directory/?_regions=north-carolina"
            ),
        )

        self.assertEqual(
            "generic_firm_email",
            classify_email_quality(
                "info@justicecounts.com",
                normalized_domain="justicecounts.com",
            )["email_quality"],
        )
        self.assertEqual(
            "valid_firm_email",
            classify_email_quality(
                "alex@justicecounts.com",
                normalized_domain="justicecounts.com",
            )["email_quality"],
        )
        self.assertEqual(
            "vendor_platform",
            classify_email_quality("support@webador.com")["email_quality"],
        )
        self.assertEqual(
            "free_provider",
            classify_email_quality("intake@gmail.com")["email_quality"],
        )
        self.assertEqual(
            "junk_asset",
            classify_email_quality("chosen-sprite@2x.png")["email_quality"],
        )
        # Sentry / wix system addresses treated as junk
        self.assertTrue(
            classify_email_quality("x@sentry.wixpress.com")["email_is_junk"],
        )
        self.assertEqual(
            "vendor_platform",
            classify_email_quality("user@sentry.io")["email_quality"],
        )

        self.assertEqual(
            "placeholder",
            classify_phone_quality("(333) 333-3333")["phone_quality"],
        )
        self.assertEqual(
            "valid",
            classify_phone_quality("(919) 851-1234")["phone_quality"],
        )

    def test_milestone4_email_validation_preserves_generic_role_addresses(self) -> None:
        """Generic role addresses (intake@, info@, contact@, office@, help@, admin@) must never be rejected."""
        for addr in (
            "intake@firm.com",
            "info@firm.com",
            "contact@firm.com",
            "office@firm.com",
            "help@smallfirm.co",
            "admin@company.org",
            "referrals@lawfirm.com",
            "support@business.net",
        ):
            normalized = normalize_email_for_validation(addr)
            self.assertIn("@", normalized, msg=addr)
            is_junk, reason = is_email_clearly_junk(normalized)
            self.assertFalse(is_junk, msg=f"expected to preserve {addr!r} (reason={reason})")
            self.assertEqual(reason, "")

    def test_milestone4_email_validation_rejects_placeholders_and_junk(self) -> None:
        """Clearly bad emails should be rejected (placeholder domains, noreply, malformed, asset-like)."""
        # Placeholder / example domains
        for addr in ("test@test.com", "example@example.com", "foo@bar.com", "user@example.com", "johndoe@email.com"):
            normalized = normalize_email_for_validation(addr)
            is_junk, reason = is_email_clearly_junk(normalized)
            self.assertTrue(is_junk, msg=f"expected to reject {addr!r}")
            self.assertEqual(reason, "placeholder_junk")
        # noreply-style
        for addr in ("noreply@anywhere.com", "no-reply@company.com", "donotreply@firm.com"):
            normalized = normalize_email_for_validation(addr)
            is_junk, reason = is_email_clearly_junk(normalized)
            self.assertTrue(is_junk, msg=f"expected to reject {addr!r}")
            self.assertEqual(reason, "placeholder_junk")
        # Asset-like
        is_junk, _ = is_email_clearly_junk(normalize_email_for_validation("x@2x.png"))
        self.assertTrue(is_junk)
        # Malformed: no @, empty, or bad structure
        for bad in ("no-at-sign", "", "local@", "@domain.com", "a b@c.com", "a@b"):
            n = normalize_email_for_validation(bad)
            val = n if n else bad
            is_junk, reason = is_email_clearly_junk(val)
            self.assertTrue(is_junk, msg=f"expected malformed for {bad!r}")
            self.assertEqual(reason, "malformed")

    def test_milestone4_normalize_email_safe(self) -> None:
        """Normalization trims, lowercases, strips angle brackets and trailing punctuation."""
        self.assertEqual(normalize_email_for_validation("  Info@Firm.COM  "), "info@firm.com")
        self.assertEqual(normalize_email_for_validation("<info@firm.com>"), "info@firm.com")
        self.assertEqual(normalize_email_for_validation("contact@firm.com;"), "contact@firm.com")
        self.assertEqual(normalize_email_for_validation("intake@firm.com."), "intake@firm.com")

    def test_milestone4_blank_junk_contacts_preserves_generic_emails_and_metrics(self) -> None:
        """blank_junk_contacts keeps generic role emails, blanks junk, and records email metrics."""
        run = load_module("leads.blank_junk_contacts")
        report = CleaningReport()
        df = pd.DataFrame({
            "email": [
                "intake@firm.com",
                "info@lawfirm.org",
                "test@test.com",
                "noreply@company.com",
                "contact@smallfirm.co",
                "  Office@Firm.COM  ",
            ],
        })
        config = {"module_id": "leads.blank_junk_contacts", "options": {"email_column": "email"}}
        out = run(df, config, report)
        self.assertIn("leads.blank_junk_contacts", report.module_stats)
        stats = report.module_stats["leads.blank_junk_contacts"]
        self.assertEqual(stats["emails_inspected"], 6)
        self.assertEqual(stats["emails_kept"], 4)
        self.assertEqual(stats["email_blanked"], 2)
        # Generic role addresses kept (and normalized)
        self.assertEqual(out.at[0, "email"], "intake@firm.com")
        self.assertEqual(out.at[1, "email"], "info@lawfirm.org")
        self.assertEqual(out.at[4, "email"], "contact@smallfirm.co")
        self.assertEqual(out.at[5, "email"], "office@firm.com")
        # Junk blanked
        self.assertTrue(pd.isna(out.at[2, "email"]))
        self.assertTrue(pd.isna(out.at[3, "email"]))
        self.assertIn("emails_rejected_malformed", stats)
        self.assertIn("emails_rejected_placeholder_junk", stats)
        self.assertIn("emails_normalized", stats)

    def test_lead_cleanup_modules_keep_best_record_per_normalized_domain(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            input_path = temp_root / "scraped_leads.csv"
            output_path = temp_root / "deduped.csv"
            report_path = temp_root / "report.json"

            rows = [
                {
                    "firm_name": "Justice Counts",
                    "website": "https://justicecounts.com/attorneys/alex-riddle/",
                    "email": "alex@justicecounts.com",
                    "phone": "(919) 555-5555",
                    "contact_name": "Alex Riddle",
                },
                {
                    "firm_name": "Justice Counts",
                    "website": "https://justicecounts.com/",
                    "email": "info@justicecounts.com",
                    "phone": "(919) 851-1234",
                    "contact_name": "",
                },
                {
                    "firm_name": "Justice Counts",
                    "website": "https://www.linkedin.com/in/alex-riddle/",
                    "email": "support@webador.com",
                    "phone": "(333) 333-3333",
                    "contact_name": "Alex Riddle",
                },
                {
                    "firm_name": "Baity Law",
                    "website": "http://Baitylaw.com",
                    "email": "miller-law-group-logo-white-lettering-transparent@2x.png",
                    "phone": "4444444444",
                    "contact_name": "",
                },
                {
                    "firm_name": "Baity Law",
                    "website": "https://baitylaw.com/contact",
                    "email": "office@baitylaw.com",
                    "phone": "704-555-2001",
                    "contact_name": "",
                },
                {
                    "firm_name": "",
                    "website": "",
                    "email": "intake@gmail.com",
                    "phone": "",
                    "contact_name": "",
                },
            ]

            with input_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["firm_name", "website", "email", "phone", "contact_name"],
                )
                writer.writeheader()
                writer.writerows(rows)

            report = run_cleaner(
                config_dict={
                    "input": {"path": str(input_path), "format": "csv"},
                    "output": {"path": str(output_path), "format": "csv"},
                    "report": {"path": str(report_path)},
                    "modules": [
                        "text.trim_whitespace",
                        {
                            "id": "text.normalize_empty_strings",
                            "options": {"empty_values": ["n/a", "N/A", "-", "null", "NULL"]},
                        },
                        {
                            "id": "leads.enrich_leads",
                            "options": {"include_score_reasons": True},
                        },
                        {
                            "id": "leads.keep_best_per_domain",
                            "options": {"keep_empty_domain_rows": True},
                        },
                    ],
                }
            )

            with output_path.open("r", encoding="utf-8", newline="") as handle:
                output_rows = list(csv.DictReader(handle))

        self.assertEqual(6, report.rows_loaded)
        self.assertEqual(4, report.rows_output)
        self.assertEqual(2, report.module_stats["leads.keep_best_per_domain"]["rows_removed"])

        justice_row = next(
            row for row in output_rows if row["normalized_domain"] == "justicecounts.com"
        )
        self.assertEqual("https://justicecounts.com/", justice_row["website"])
        self.assertEqual("firm_homepage", justice_row["page_type"])
        self.assertEqual("generic_firm_email", justice_row["email_quality"])
        self.assertEqual("valid", justice_row["phone_quality"])

        baity_row = next(row for row in output_rows if row["normalized_domain"] == "baitylaw.com")
        self.assertEqual("https://baitylaw.com/contact", baity_row["website"])
        self.assertEqual("generic_firm_email", baity_row["email_quality"])
        self.assertEqual("valid", baity_row["phone_quality"])

        linkedin_row = next(row for row in output_rows if row["normalized_domain"] == "linkedin.com")
        self.assertEqual("social_profile", linkedin_row["page_type"])
        self.assertEqual("vendor_platform", linkedin_row["email_quality"])
        self.assertEqual("placeholder", linkedin_row["phone_quality"])

        no_domain_row = next(row for row in output_rows if row["normalized_domain"] == "")
        self.assertEqual("free_provider", no_domain_row["email_quality"])
        self.assertEqual("-10", no_domain_row["lead_score"])


if __name__ == "__main__":
    unittest.main()
