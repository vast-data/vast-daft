from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from urllib import error, request

from note_definitions import build_validation_notes


def _api_request(*, method: str, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {body}") from exc


def _note_title(note: dict[str, Any]) -> str:
    return str(note.get("name", "Unnamed note"))


def _normalize_note_path(path: str) -> str:
    return path.lstrip("/")


def _existing_notes(*, endpoint: str) -> list[dict[str, Any]]:
    resp = _api_request(method="GET", url=f"{endpoint}/api/notebook")
    if resp.get("status") != "OK":
        raise RuntimeError(f"Failed to list notes: {resp}")
    body = resp.get("body")
    if not isinstance(body, list):
        raise RuntimeError(f"Unexpected notebook list response: {resp}")
    return body


def _delete_note(*, endpoint: str, note_id: str) -> None:
    resp = _api_request(method="DELETE", url=f"{endpoint}/api/notebook/{note_id}")
    if resp.get("status") != "OK":
        raise RuntimeError(f"Failed to delete note {note_id}: {resp}")


def _create_note(*, endpoint: str, note: dict[str, Any]) -> str:
    resp = _api_request(method="POST", url=f"{endpoint}/api/notebook", payload=note)
    if resp.get("status") != "OK":
        raise RuntimeError(f"Failed to create {_note_title(note)}: {resp}")
    note_id = resp.get("body")
    if not isinstance(note_id, str):
        raise RuntimeError(f"Missing note id for {_note_title(note)}: {resp}")
    return note_id


def _get_note(*, endpoint: str, note_id: str) -> dict[str, Any]:
    resp = _api_request(method="GET", url=f"{endpoint}/api/notebook/{note_id}")
    if resp.get("status") != "OK":
        raise RuntimeError(f"Failed to get note {note_id}: {resp}")
    body = resp.get("body")
    if not isinstance(body, dict):
        raise RuntimeError(f"Unexpected note response for {note_id}: {resp}")
    return body


def _run_paragraph(*, endpoint: str, note_id: str, paragraph_id: str) -> None:
    resp = _api_request(method="POST", url=f"{endpoint}/api/notebook/run/{note_id}/{paragraph_id}")
    if resp.get("status") != "OK":
        raise RuntimeError(f"Paragraph {paragraph_id} failed: {resp}")


def _result_summary(paragraph: dict[str, Any]) -> str:
    results = paragraph.get("results")
    if not isinstance(results, dict):
        return ""
    messages = results.get("msg")
    if not isinstance(messages, list) or not messages:
        return ""
    first_message = messages[0]
    if not isinstance(first_message, dict):
        return ""
    data = str(first_message.get("data", "")).strip()
    return data.replace("\n", " ")[:200]


def _run_note(*, endpoint: str, note: dict[str, Any]) -> None:
    note_name = _note_title(note)
    for existing in _existing_notes(endpoint=endpoint):
        existing_path = existing.get("path")
        if (
            isinstance(existing_path, str)
            and _normalize_note_path(existing_path) == note_name
            and isinstance(existing.get("id"), str)
        ):
            _delete_note(endpoint=endpoint, note_id=existing["id"])

    note_id = _create_note(endpoint=endpoint, note=note)
    print(f"Created {note_name} ({note_id})")

    note_body = _get_note(endpoint=endpoint, note_id=note_id)
    paragraphs = note_body.get("paragraphs")
    if not isinstance(paragraphs, list):
        raise RuntimeError(f"Note {note_name} has no paragraphs")

    for paragraph in paragraphs:
        paragraph_id = paragraph.get("id")
        if not isinstance(paragraph_id, str):
            raise RuntimeError(f"Paragraph missing id in {note_name}")

        title = str(paragraph.get("title") or paragraph_id)
        print(f"Running {note_name}: {title}")
        _run_paragraph(endpoint=endpoint, note_id=note_id, paragraph_id=paragraph_id)

        updated_note = _get_note(endpoint=endpoint, note_id=note_id)
        updated_paragraphs = updated_note.get("paragraphs")
        if not isinstance(updated_paragraphs, list):
            raise RuntimeError(f"Updated note {note_name} has no paragraphs")

        matching = next((item for item in updated_paragraphs if item.get("id") == paragraph_id), None)
        if not isinstance(matching, dict):
            raise RuntimeError(f"Could not find paragraph {paragraph_id} in {note_name}")

        result_code = matching.get("results", {}).get("code") if isinstance(matching.get("results"), dict) else None
        if result_code not in {None, "SUCCESS"}:
            raise RuntimeError(f"Paragraph {title} failed with result code {result_code}: {_result_summary(matching)}")

        summary = _result_summary(matching)
        if summary:
            print(f"  {summary}")

    print(f"Finished {note_name}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Import and execute Zeppelin validation notes")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8080", help="Zeppelin base URL")
    args = parser.parse_args()

    endpoint = args.endpoint.rstrip("/")
    try:
        _existing_notes(endpoint=endpoint)
        for note in build_validation_notes():
            _run_note(endpoint=endpoint, note=note)
    except Exception as exc:
        print(f"Zeppelin validation failed: {exc}", file=sys.stderr)
        return 1

    print("All Zeppelin validation notes passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
