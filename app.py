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
    buffer_time=5 * 60,
)

buffer_writer.start()


PIXEL_GIF = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")

ALLOWED_REDIRECT_HOSTS = {
    "onead.onevision.com.tw": "OneAD",
    "localhost": "test",
}


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

    if parsed.scheme not in ["http", "https"] or not parsed.hostname:
        return None, None

    if parsed.hostname in ALLOWED_REDIRECT_HOSTS:
        return decoded, ALLOWED_REDIRECT_HOSTS[parsed.hostname]

    logger.warning("Hostname not allowed: %s", parsed.hostname)
    return None, None


def get_or_create_mapping_id():
    mapping_id = request.cookies.get(COOKIE_NAME)
    if mapping_id:
        return mapping_id, False
    return str(uuid.uuid4()), True


def add_mapping_id_to_url(url, mapping_id):
    from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    query_params["id"] = [mapping_id]
    new_query = urlencode(query_params, doseq=True)
    return urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )


def make_redirect_response(url):
    response = make_response(redirect(url, code=302))
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


@app.route("/", methods=["GET"])
def health_check():
    return {"status": "ok", "service": "omni-ad-pixel"}, 200


@app.route("/track", methods=["GET"])
def track_pixel():
    try:
        cid = request.args.get("cid")
        to = request.args.get("to")
        partner = request.args.get("partner")

        if not cid:
            return make_pixel_response()

        valid_redirect_url, domain_partner = validate_redirect_url(to) if to else (None, None)
        final_partner = partner or domain_partner or "unknown"

        mapping_id, is_new = get_or_create_mapping_id()

        buffer_writer.add(
            {
                "partner": final_partner,
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

        if valid_redirect_url:
            url_with_mapping_id = add_mapping_id_to_url(valid_redirect_url, mapping_id)
            response = make_redirect_response(url_with_mapping_id)
        else:
            response = make_pixel_response()

        if is_new:
            response.set_cookie(
                COOKIE_NAME,
                mapping_id,
                max_age=COOKIE_MAX_AGE,
                httponly=True,
                secure=True,
                samesite="None",
            )

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
