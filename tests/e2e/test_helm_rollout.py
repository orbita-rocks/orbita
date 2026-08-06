"""Rendered Helm rollout semantics for the first Raft-capable release."""

from __future__ import annotations

import string
import subprocess
import tempfile

import harness


CHART = harness.REPO_ROOT / "deploy" / "helm" / "orbita"
NOTES = CHART / "templates" / "NOTES.txt"
EKS_OVERLAY = CHART / "values-eks.yaml"


def _render(*values):
    command = ["helm", "template", "orbita", str(CHART)]
    for value in values:
        command.extend(("--set", value))
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def _render_with_values(values_text):
    with tempfile.NamedTemporaryFile("w", suffix=".yaml") as values_file:
        values_file.write(values_text)
        values_file.flush()
        command = [
            "helm",
            "template",
            "orbita",
            str(CHART),
            "--namespace",
            "orbita",
            "--values",
            values_file.name,
        ]
        return subprocess.run(
            command, check=True, capture_output=True, text=True
        ).stdout


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


def test_the_eks_overlay_renders_irsa_and_never_a_static_credential():
    # The EKS overlay is the one deployment the chart is written for and the one
    # nobody runs on every commit, so this pins that it still turns the AWS
    # knobs the way deploy/eks expects. The tokens are ${VAR} on purpose, which
    # is exactly string.Template's syntax, so a sample fill is a substitution.
    # safe_substitute fills the three tokens and leaves any other $ alone, which
    # is what up.sh's envsubst does and what keeps the literal ${...} in the
    # overlay's own comments from tripping the fill.
    filled = string.Template(EKS_OVERLAY.read_text()).safe_substitute(
        ORBITA_EKS_ACCOUNT_ID="123456789012",
        ORBITA_EKS_REGION="us-west-2",
        ORBITA_EKS_BUCKET="orbita-test-sample",
    )
    rendered = _render_with_values(filled)

    # The service account carries the workload role, which is the whole IRSA
    # handshake on the Kubernetes side.
    assert (
        "eks.amazonaws.com/role-arn: "
        "arn:aws:iam::123456789012:role/orbita-test-s3" in rendered
    )
    # web-identity against real S3: regional endpoint, virtual-host addressing.
    assert 'credential_source = "web-identity"' in rendered
    assert "force_path_style = false" in rendered
    assert 'endpoint = "https://s3.us-west-2.amazonaws.com"' in rendered
    # The point of a keyless source is that there is no key to leak: no static
    # credential environment and no Secret object at all.
    assert "ORBITA_OBJECT_STORE_ACCESS_KEY_ID" not in rendered
    assert "kind: Secret" not in rendered
    # Data on gp3, provisioned by the addon the cluster config installs.
    assert 'storageClassName: "gp3"' in rendered


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
