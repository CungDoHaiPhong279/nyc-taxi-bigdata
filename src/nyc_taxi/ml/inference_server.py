"""Internal-only bounded stdlib HTTP transport for a single frozen Spark process."""
from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import signal
import threading

from nyc_taxi.ml.inference import FrozenPredictor, InferenceError, InputError, identity

MAX_BODY_BYTES = 4096


def handler_for(predictor):
    def selected_identity():
        method = getattr(predictor, "identity", None)
        return method() if callable(method) else identity()

    class Handler(BaseHTTPRequestHandler):
        server_version = "FareDemo/1"

        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def log_message(self, format, *args):
            # No user inputs, paths, request bodies or config/connection secrets.
            pass

        def reply(self, status, payload):
            body = json.dumps(payload, allow_nan=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            if self.path in ("/health", "/ready"):
                self.reply(200 if self.path == "/health" or predictor.ready else 503, predictor.readiness())
            elif self.path == "/zones":
                if not predictor.ready:
                    self.reply(503, {"error": "model_not_ready", **selected_identity()})
                else:
                    method = getattr(predictor, "zone_response", None)
                    self.reply(200, method() if callable(method) else {"zones": predictor.zones, **selected_identity()})
            else:
                self.reply(404, {"error": "not_found"})

        def do_POST(self):
            if self.path != "/predict":
                self.reply(404, {"error": "not_found"})
                return
            try:
                if self.headers.get("Content-Type", "").split(";")[0] != "application/json":
                    raise InputError("Cần Content-Type application/json.")
                length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= length <= MAX_BODY_BYTES or self.headers.get("Transfer-Encoding"):
                    raise InputError("JSON request rỗng hoặc vượt giới hạn.")
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise InputError("JSON request chưa đầy đủ.")
                def invalid_constant(value):
                    raise InputError("JSON không được chứa NaN/Infinity.")
                payload = json.loads(raw, parse_constant=invalid_constant)
                result = predictor.predict(payload)
                self.reply(200, result)
            except (InputError, ValueError, UnicodeError):
                message = ("Inputs không hợp lệ theo contract V2 và thời gian America/New_York."
                           if selected_identity().get("model_version") == "v2.2" else
                           "Inputs không hợp lệ; dùng distance hữu hạn [0,200], local datetime năm 2025 và zone IDs 1–263.")
                self.reply(400, {"error": "invalid_inputs", "message": message, **selected_identity()})
            except InferenceError as error:
                self.reply(error.status, {"error": error.code, **getattr(error, "public_payload", {}), **selected_identity()})
            except Exception:
                self.reply(503, {"error": "service_unavailable", **selected_identity()})

    return Handler


def make_predictor(model_version, model_dir, contract_dir, zones):
    """Explicit one-model factory. Failure never falls back to another version."""
    if model_version == "v1":
        return FrozenPredictor(model_dir, contract_dir, zones)
    if model_version == "v2.2":
        from nyc_taxi.ml.pretrip_inference import FrozenPretripPredictor
        return FrozenPretripPredictor(model_dir, contract_dir, zones)
    raise ValueError("Unsupported explicitly selected model version")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--contract-dir", type=Path, required=True)
    parser.add_argument("--zones", type=Path, required=True)
    parser.add_argument("--model-version", choices=("v1", "v2.2"), default="v1")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)
    predictor = make_predictor(args.model_version, args.model_dir, args.contract_dir, args.zones)
    server = ThreadingHTTPServer((args.host, args.port), handler_for(predictor))
    server.daemon_threads = True
    loader = threading.Thread(target=predictor.load, name="frozen-model-loader", daemon=True)
    loader.start()

    def stop(signum, frame):
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
        predictor.close()


if __name__ == "__main__":
    main()
