import subprocess

import pytest


def run_iptables(args: list[str]) -> None:
    """Run a single iptables command via sudo."""
    cmd = ["sudo", "iptables"] + args
    subprocess.run(cmd, check=True)


def pytest_sessionstart(session: pytest.Session):
    # Allow loopback
    run_iptables(["-A", "OUTPUT", "-o", "lo", "-j", " ACCEPT"])
    # Allow traffic to proxy port on localhost
    run_iptables(
        [
            "-A",
            "OUTPUT",
            "-p",
            "tcp",
            "--dport",
            "8080",
            "-j",
            "ACCEPT",
        ]
    )
    # Allow established connections
    run_iptables(
        [
            "-A",
            "OUTPUT",
            "-m",
            "conntrack",
            "--ctstate",
            "ESTABLISHED,RELATED",
            "-j",
            "ACCEPT",
        ]
    )
    # Drop everything else
    run_iptables(["-A", "OUTPUT", "-j", "DROP"])


def pytest_sessionfinish(session: pytest.Session):
    # Flush rules in OUTPUT
    run_iptables(["-F", "OUTPUT"])
    # Set default policy to ACCEPT
    run_iptables(["-P", "OUTPUT", "ACCEPT"])
