from kodiak.refresh_pull_requests import _is_pr_potentially_actionable


class TestIsPrPotentiallyActionable:
    def test_draft_pr_is_skipped(self) -> None:
        pr = {
            "number": 1,
            "isDraft": True,
            "labels": {"nodes": [{"name": "automerge"}]},
            "baseRef": {"name": "main"},
        }
        assert _is_pr_potentially_actionable(pr) is False

    def test_pr_without_automerge_label_is_skipped(self) -> None:
        pr = {
            "number": 2,
            "isDraft": False,
            "labels": {"nodes": [{"name": "bug"}, {"name": "enhancement"}]},
            "baseRef": {"name": "main"},
        }
        assert _is_pr_potentially_actionable(pr) is False

    def test_pr_with_no_labels_is_skipped(self) -> None:
        pr = {
            "number": 3,
            "isDraft": False,
            "labels": {"nodes": []},
            "baseRef": {"name": "main"},
        }
        assert _is_pr_potentially_actionable(pr) is False

    def test_pr_with_automerge_label_is_actionable(self) -> None:
        pr = {
            "number": 4,
            "isDraft": False,
            "labels": {"nodes": [{"name": "automerge"}]},
            "baseRef": {"name": "main"},
        }
        assert _is_pr_potentially_actionable(pr) is True

    def test_pr_with_dependencies_label_is_actionable(self) -> None:
        pr = {
            "number": 5,
            "isDraft": False,
            "labels": {"nodes": [{"name": "dependencies"}]},
            "baseRef": {"name": "main"},
        }
        assert _is_pr_potentially_actionable(pr) is True

    def test_pr_with_version_bump_label_is_actionable(self) -> None:
        pr = {
            "number": 6,
            "isDraft": False,
            "labels": {"nodes": [{"name": "version-bump"}]},
            "baseRef": {"name": "main"},
        }
        assert _is_pr_potentially_actionable(pr) is True

    def test_pr_with_mixed_labels_including_automerge(self) -> None:
        pr = {
            "number": 7,
            "isDraft": False,
            "labels": {"nodes": [{"name": "bug"}, {"name": "automerge"}]},
            "baseRef": {"name": "main"},
        }
        assert _is_pr_potentially_actionable(pr) is True

    def test_pr_with_missing_labels_field_is_actionable(self) -> None:
        """When labels data is missing, err on the side of caution."""
        pr = {
            "number": 8,
            "isDraft": False,
            "baseRef": {"name": "main"},
        }
        assert _is_pr_potentially_actionable(pr) is True

    def test_pr_with_custom_labels(self) -> None:
        pr = {
            "number": 9,
            "isDraft": False,
            "labels": {"nodes": [{"name": "ship-it"}]},
            "baseRef": {"name": "main"},
        }
        assert (
            _is_pr_potentially_actionable(pr, automerge_labels=frozenset({"ship-it"}))
            is True
        )
        assert _is_pr_potentially_actionable(pr) is False

    def test_non_draft_without_is_draft_field_is_not_skipped(self) -> None:
        """Missing isDraft should default to not-draft."""
        pr = {
            "number": 10,
            "labels": {"nodes": [{"name": "automerge"}]},
            "baseRef": {"name": "main"},
        }
        assert _is_pr_potentially_actionable(pr) is True
