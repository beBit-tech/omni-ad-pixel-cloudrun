import atexit
import base64
import json
import logging
import os
import uuid
from datetime import UTC, datetime

from flask import Flask, make_response, redirect, request

from buffer_writer import BufferWriter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", str(uuid.uuid4()))

GCS_BUCKET = os.environ.get("GCS_BUCKET")
GCS_PROJECT = os.environ.get("GCS_PROJECT")
COOKIE_NAME = "mapping_id"
COOKIE_MAX_AGE = 365 * 24 * 60 * 60

if not GCS_BUCKET:
    logger.warning("GCS_BUCKET not set, data will not be persisted")
if not GCS_PROJECT:
    logger.warning("GCS_PROJECT not set")

buffer_writer = BufferWriter(
    gcs_bucket=GCS_BUCKET,
    gcs_project=GCS_PROJECT,
    buffer_size=500000,
    buffer_time=10,
)

buffer_writer.start()


@atexit.register
def cleanup():
    logger.info("Shutting down buffer writer...")
    buffer_writer.stop()


PIXEL_GIF = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")
ALLOWED_REDIRECT_HOSTS = ["onead.onevision.com.tw", "localhost"]


def make_pixel_response():
    response = make_response(PIXEL_GIF, 200)
    response.headers["Content-Type"] = "image/gif"
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


def validate_redirect_url(url):
    from urllib.parse import unquote, urlparse

    decoded = unquote(url)
    parsed = urlparse(decoded)

    if parsed.scheme not in ["http", "https"]:
        print("Invalid scheme:", parsed.scheme)
        return None
    if not parsed.hostname:
        print("No hostname found")
        return None
    if parsed.hostname not in ALLOWED_REDIRECT_HOSTS:
        print("Hostname not allowed:", parsed.hostname)
        return None

    return decoded


@app.route("/track", methods=["GET"])
def track_pixel():
    try:
        cid = request.args.get("cid")
        to = request.args.get("to")

        if not cid:
            return make_pixel_response()

        redirect_url = validate_redirect_url(to) if to else None
        print("redirect_url:", redirect_url)
        mapping_id = request.cookies.get(COOKIE_NAME)
        is_created = False

        if not mapping_id:
            mapping_id = str(uuid.uuid4())
            is_created = True

        buffer_writer.add(
            {
                "cid": cid,
                "mapping_id": mapping_id,
                "timestamp": datetime.now(UTC).isoformat(),
                "ip_address": request.headers.get("X-Forwarded-For", request.remote_addr),
                "user_agent": request.headers.get("User-Agent", ""),
                "referer": request.headers.get("Referer", ""),
                "date": datetime.now(UTC).strftime("%Y-%m-%d"),
                "origin": request.headers.get("Origin", ""),
                "headers": json.dumps(dict(request.headers)),
            }
        )

        if redirect_url:
            from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

            parsed = urlparse(redirect_url)
            query_params = parse_qs(parsed.query)
            query_params["id"] = [mapping_id]
            new_query = urlencode(query_params, doseq=True)
            redirect_url = urlunparse(
                (
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    parsed.params,
                    new_query,
                    parsed.fragment,
                )
            )
            response = make_response(redirect(redirect_url, code=302))
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        else:
            response = make_pixel_response()

        if is_created:
            cookie_kwargs = {
                "max_age": COOKIE_MAX_AGE,
                "httponly": True,
                "secure": True,
                "samesite": "None",
            }

            response.set_cookie(COOKIE_NAME, mapping_id, **cookie_kwargs)

        return response

    except Exception as e:
        logger.error(f"Error processing pixel request: {e}", exc_info=True)
        return make_pixel_response()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8070))
    logger.info(f"Starting Flask app on port {port}")
    debug_mode = os.environ.get("DEBUG", "false").lower() == "true"
    logger.info(f"Debug mode is {'on' if debug_mode else 'off'}")
    app.run(host="0.0.0.0", port=port, debug=debug_mode)
