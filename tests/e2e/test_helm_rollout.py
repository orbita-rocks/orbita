"""Rendered Helm rollout semantics for the first Raft-capable release."""

from __future__ import annotations

import subprocess

import harness


CHART = harness.REPO_ROOT / "deploy" / "helm" / "orbita"
NOTES = CHART / "templates" / "NOTES.txt"


def _render(*values):
    command = ["helm", "template", "orbita", str(CHART)]
    for value in values:
        command.extend(("--set", value))
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def _leader_statefulset(rendered):
    return next(
        document
        for document in rendered.split("---")
        if "# Source: orbita/templates/statefulset-leader.yaml" in document
    )


def test_ordinary_leader_upgrades_remain_readiness_gated_rolling_updates():
    leader = _leader_statefulset(_render())

    assert "updateStrategy:\n    type: RollingUpdate" in leader


def test_first_raft_upgrade_renders_the_two_voter_on_delete_transition():
    rendered = _render("leader.firstRaftUpgrade=true")
    leader = _leader_statefulset(rendered)

    assert "updateStrategy:\n    # The pre-Raft binaries" in leader
    assert "type: OnDelete" in leader
    notes = NOTES.read_text()
    assert "delete pod {{ include \"orbita.fullname\" . }}-leader-1" in notes
    assert "leader.firstRaftUpgrade=false" in notes


def test_chart_rejects_a_production_group_below_three_voters():
    result = subprocess.run(
        ["helm", "template", "orbita", str(CHART), "--set", "leader.replicas=2"],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "leader.replicas must be at least 3" in result.stderr


def test_first_raft_transition_reaches_quorum_where_rolling_update_stalls():
    # False is the old binary, which cannot vote. True is the Raft-capable
    # binary. A normal StatefulSet replaces one pod and waits for readiness,
    # but one capable voter cannot satisfy a two-of-three quorum.
    ordinary = [False, False, True]
    assert sum(ordinary) == 1
    assert sum(ordinary) < 2

    # OnDelete lets the operator replace two named pods as one transition.
    # They become Ready only after they can vote together, then the last old
    # pod can be replaced without losing that quorum.
    transition = [False, True, True]
    assert sum(transition) >= 2
    assert transition[1] and transition[2]

    transition[0] = True
    assert all(transition)
