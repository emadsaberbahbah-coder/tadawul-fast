"""SMTP acknowledgements distinguish complete, partial and unknown delivery.

Exercise the actual send boundary with in-memory SMTP fakes; no email is sent.
"""
import smtplib

import pytest

from scripts import send_digest as digest


PRIVATE_RESPONSE = b"private SMTP response api_token=must-not-appear"


def configure(monkeypatch, port, *, refusal=None, error=None):
    observations = {"connects": 0, "sends": 0, "tls": 0, "logins": 0}

    class FakeSMTP:
        def __init__(self, *_args, **_kwargs):
            observations["connects"] += 1

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def starttls(self, **_kwargs):
            observations["tls"] += 1

        def login(self, *_args):
            observations["logins"] += 1

        def sendmail(self, sender, recipients, message):
            observations["sends"] += 1
            assert sender == "sender@example.invalid"
            assert recipients == ["accepted@example.invalid", "refused@example.invalid"]
            assert "synthetic subject" in message
            if error is not None:
                raise error
            return refusal

    for key, value in {
        "SMTP_HOST": "smtp.example.invalid", "SMTP_PORT": str(port),
        "SMTP_USER": "fixture-user", "SMTP_PASSWORD": "fixture-password",
        "SMTP_USE_TLS": "1", "DIGEST_FROM": "sender@example.invalid",
        "DIGEST_TO": "accepted@example.invalid,refused@example.invalid",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(digest.smtplib, "SMTP", FakeSMTP)
    monkeypatch.setattr(digest.smtplib, "SMTP_SSL", FakeSMTP)
    return observations


def send(*, dry_run=False):
    return digest._send_email("synthetic subject", "fixture", "<p>fixture</p>", dry_run)


@pytest.mark.parametrize("port", [465, 587])
def test_partial_refusal_is_nonzero_count_only_and_never_replayed(monkeypatch, capsys, port):
    observed = configure(monkeypatch, port,
                         refusal={"refused@example.invalid": (550, PRIVATE_RESPONSE)})

    assert send() == 2

    assert observed["connects"] == observed["sends"] == 1
    assert observed["tls"] == (port == 587)
    output = capsys.readouterr().out
    assert "requested=2 accepted=1 refused=1" in output
    assert "verify accepted recipients before retrying" in output
    assert "email sent to" not in output
    assert "@example.invalid" not in output and "api_token=" not in output


@pytest.mark.parametrize("port", [465, 587])
def test_empty_refusal_receipt_confirms_all_recipients(monkeypatch, capsys, port):
    observed = configure(monkeypatch, port, refusal={})

    assert send() == 0

    assert observed["sends"] == 1
    assert "email sent to 2 recipient(s)" in capsys.readouterr().out


@pytest.mark.parametrize("receipt", [None, False, [], {"unknown@example.invalid": (550, PRIVATE_RESPONSE)}])
def test_malformed_delivery_receipt_stays_unconfirmed(monkeypatch, capsys, receipt):
    observed = configure(monkeypatch, 587, refusal=receipt)

    assert send() == 2

    assert observed["sends"] == 1
    output = capsys.readouterr().out
    assert "delivery unconfirmed" in output
    assert "email sent to" not in output and "api_token=" not in output
    assert "@example.invalid" not in output


@pytest.mark.parametrize("error", [
    smtplib.SMTPRecipientsRefused({"refused@example.invalid": (550, PRIVATE_RESPONSE)}),
    smtplib.SMTPAuthenticationError(535, PRIVATE_RESPONSE),
    smtplib.SMTPServerDisconnected(PRIVATE_RESPONSE.decode()),
])
def test_smtp_failures_remain_nonzero_without_private_payloads(monkeypatch, capsys, error):
    observed = configure(monkeypatch, 587, error=error)

    assert send() == 2

    assert observed["sends"] == 1
    output = capsys.readouterr().out
    assert type(error).__name__ in output
    assert "delivery unconfirmed" in output
    assert "api_token=" not in output and "@example.invalid" not in output


def test_preview_never_connects_even_when_delivery_would_be_partial(monkeypatch, capsys):
    observed = configure(monkeypatch, 587,
                         refusal={"refused@example.invalid": (550, PRIVATE_RESPONSE)})

    assert send(dry_run=True) == 0

    assert observed["connects"] == observed["sends"] == 0
    assert "DRY RUN" in capsys.readouterr().out
