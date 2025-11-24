import subprocess

import pytest


def run_iptables(args: list[str]) -> None:
    """Run a single iptables command via sudo."""
    cmd = ["sudo", "iptables"] + args
    subprocess.run(cmd, check=True, capture_output=True)


# this test is intended to be run on github actions
def pytest_sessionstart(session: pytest.Session):
    # Allow loopback
    run_iptables(["-A", "OUTPUT", "-o", "lo", "-j", "ACCEPT"])
    # Allow traffic to proxy port on localhost
    run_iptables(["-A", "OUTPUT", "-p", "tcp", "--dport", "8080", "-j", "ACCEPT"])
    # block direct HTTP
    run_iptables(["-A", "OUTPUT", "-p", "tcp", "--dport", "80", "-j", "REJECT"])
    # block direct HTTPS
    run_iptables(["-A", "OUTPUT", "-p", "tcp", "--dport", "443", "-j", "REJECT"])


def pytest_sessionfinish(session: pytest.Session):
    # Remove in reverse order of insertion
    # remove block direct HTTPS
    run_iptables(["-D", "OUTPUT", "-p", "tcp", "--dport", "443", "-j", "REJECT"])
    # remove block direct HTTP
    run_iptables(["-D", "OUTPUT", "-p", "tcp", "--dport", "80", "-j", "REJECT"])
    # remove allow traffic to proxy port
    run_iptables(["-D", "OUTPUT", "-p", "tcp", "--dport", "8080", "-j", "ACCEPT"])
    # remove allow loopback
    run_iptables(["-D", "OUTPUT", "-o", "lo", "-j", "ACCEPT"])
