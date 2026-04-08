from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import socket
from urllib.parse import quote_plus
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        str(col)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("__", "_")
        for col in df.columns
    ]
    return df


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def timestamp_str() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def save_output(df: pd.DataFrame, output_dir: Path, prefix: str = "stock_minimo_resultado") -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{prefix}_{timestamp_str()}.csv"
    output_path = output_dir / filename
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def send_webhook_get(url: str, timeout_seconds: int = 15) -> dict[str, object]:
    req = Request(url=url, method="GET", headers={"User-Agent": "stock-min-service/1.0"})
    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read(4096)
            return {
                "ok": 200 <= int(resp.status) < 300,
                "status_code": int(resp.status),
                "body": body.decode("utf-8", errors="replace"),
            }
    except HTTPError as exc:
        body = exc.read(4096) if getattr(exc, "fp", None) is not None else b""
        return {
            "ok": False,
            "status_code": int(getattr(exc, "code", 0) or 0),
            "body": body.decode("utf-8", errors="replace"),
        }
    except URLError as exc:
        return {
            "ok": False,
            "status_code": 0,
            "body": str(exc),
        }


def docker_sock_available(sock_path: str = "/var/run/docker.sock") -> bool:
    return Path(sock_path).exists()


def docker_request(
    method: str,
    path: str,
    query: dict[str, str] | None = None,
    body: dict | None = None,
    sock_path: str = "/var/run/docker.sock",
) -> tuple[int, bytes]:
    if not docker_sock_available(sock_path):
        raise FileNotFoundError(sock_path)

    query_str = ""
    if query:
        parts = []
        for k, v in query.items():
            parts.append(f"{quote_plus(str(k))}={quote_plus(str(v))}")
        query_str = "?" + "&".join(parts)

    req_path = f"{path}{query_str}"
    payload = b""
    headers = {
        "Host": "localhost",
        "User-Agent": "stock-min-service/1.0",
        "Accept": "application/json",
        "Connection": "close",
    }
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = str(len(payload))

    header_lines = "".join([f"{k}: {v}\r\n" for k, v in headers.items()])
    raw = (f"{method} {req_path} HTTP/1.1\r\n" + header_lines + "\r\n").encode("utf-8") + payload

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(sock_path)
        s.sendall(raw)
        chunks = []
        while True:
            data = s.recv(65536)
            if not data:
                break
            chunks.append(data)

    resp = b"".join(chunks)
    header_end = resp.find(b"\r\n\r\n")
    head = resp[:header_end].decode("iso-8859-1", errors="replace") if header_end != -1 else ""
    status_line = head.splitlines()[0] if head else ""
    status_code = int(status_line.split(" ")[1]) if status_line.startswith("HTTP/") else 0
    body_bytes = resp[header_end + 4 :] if header_end != -1 else b""

    if "transfer-encoding: chunked" in head.lower():
        out = bytearray()
        i = 0
        while i < len(body_bytes):
            j = body_bytes.find(b"\r\n", i)
            if j == -1:
                break
            size_line = body_bytes[i:j].decode("ascii", errors="replace").strip()
            try:
                size = int(size_line, 16)
            except ValueError:
                break
            i = j + 2
            if size == 0:
                break
            out += body_bytes[i : i + size]
            i += size + 2
        body_bytes = bytes(out)
    return status_code, body_bytes


def docker_find_container_by_name(name: str) -> dict | None:
    filters = json.dumps({"name": [name]})
    status, body = docker_request("GET", "/containers/json", query={"all": "1", "filters": filters})
    if status != 200:
        return None
    arr = json.loads(body.decode("utf-8", errors="replace") or "[]")
    if not arr:
        return None
    return arr[0]


def docker_container_logs(container_id: str, tail: int = 120) -> str:
    status, body = docker_request(
        "GET",
        f"/containers/{container_id}/logs",
        query={"stdout": "1", "stderr": "1", "tail": str(int(tail))},
    )
    if status != 200:
        return ""
    return body.decode("utf-8", errors="replace")


def docker_container_start(container_id: str) -> bool:
    status, _ = docker_request("POST", f"/containers/{container_id}/start")
    return status in (204, 304)


def docker_container_stop(container_id: str, timeout_seconds: int = 10) -> bool:
    status, _ = docker_request("POST", f"/containers/{container_id}/stop", query={"t": str(int(timeout_seconds))})
    return status in (204, 304)


def docker_container_remove(container_id: str, force: bool = True) -> bool:
    status, _ = docker_request(
        "DELETE",
        f"/containers/{container_id}",
        query={"force": "1" if force else "0"},
    )
    return status in (204,)


def docker_image_pull(image: str) -> bool:
    if ":" in image:
        from_image, tag = image.split(":", 1)
    else:
        from_image, tag = image, "latest"
    status, _ = docker_request("POST", "/images/create", query={"fromImage": from_image, "tag": tag})
    return status in (200, 201)


def docker_container_create_tunnel(
    name: str,
    image: str,
    target_container: str,
    url: str,
) -> str | None:
    body = {
        "Image": image,
        "Cmd": ["tunnel", "--no-autoupdate", "--protocol", "http2", "--url", url],
        "HostConfig": {
            "NetworkMode": f"container:{target_container}",
            "RestartPolicy": {"Name": "unless-stopped"},
        },
    }
    status, resp_body = docker_request("POST", "/containers/create", query={"name": name}, body=body)
    if status not in (201,):
        return None
    data = json.loads(resp_body.decode("utf-8", errors="replace") or "{}")
    return data.get("Id")
