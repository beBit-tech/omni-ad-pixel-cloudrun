import atexit
import logging
import os
import time
import uuid
from datetime import UTC, datetime
from threading import Lock, Thread

from flask import Flask, jsonify, make_response, redirect, request, url_for

from buffer_writer import BufferWriter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', str(uuid.uuid4()))

GCS_BUCKET = os.environ.get('GCS_BUCKET')
GCS_PROJECT = os.environ.get('GCS_PROJECT')
COOKIE_DOMAIN = os.environ.get('COOKIE_DOMAIN')
COOKIE_NAME = 'mapping_id'
COOKIE_MAX_AGE = 365 * 24 * 60 * 60

if not GCS_BUCKET:
    logger.warning("GCS_BUCKET not set, data will not be persisted")
if not GCS_PROJECT:
    logger.warning("GCS_PROJECT not set")

buffer_writer = BufferWriter(
    gcs_bucket=GCS_BUCKET,
    gcs_project=GCS_PROJECT,
    buffer_size=10000,
    buffer_time=10,
)

buffer_writer.start()

@atexit.register
def cleanup():
    logger.info("Shutting down buffer writer...")
    buffer_writer.stop()

@app.route('/track', methods=['GET'])
def track_pixel():
    try:
        cid = request.args.get('cid')
        partner = request.args.get('partner')
        mapping_id = request.cookies.get(COOKIE_NAME)
        origin = request.headers.get('Origin')

        missing_params = [p for p in ['cid', 'partner'] if request.args.get(p) is None]
        if missing_params:
            return make_response(jsonify({
                'error': 'Missing required parameters',
                'required': ['cid', 'partner'],
                'missing': missing_params
            }), 400)


        is_created = False
        if not mapping_id:
            mapping_id = str(uuid.uuid4())
            is_created = True

 
        data = {
            'cid': cid,
            'partner': partner,
            'mapping_id': mapping_id,
            'timestamp': datetime.now(UTC).isoformat(),
            'ip_address': request.headers.get('X-Forwarded-For', request.remote_addr),
            'user_agent': request.headers.get('User-Agent', ''),
            'referer': request.headers.get('Referer', ''),
            'date': datetime.now(UTC).strftime('%Y-%m-%d'),
            'origin': origin,
        }
        buffer_writer.add(data)

        response = make_response(jsonify({'mapping_id': mapping_id, 'is_created': is_created}), 200)
        if origin is not None:
            response.headers['Access-Control-Allow-Origin'] = origin
            response.headers['Access-Control-Allow-Credentials'] = 'true'


        if is_created:
            response.set_cookie(
                COOKIE_NAME,
                mapping_id,
                max_age=COOKIE_MAX_AGE,
                domain=COOKIE_DOMAIN,
                httponly=True,
                secure=True,
                samesite='None'
            )

        return response

    except Exception as e:
        logger.error(f"Error processing pixel request: {e}", exc_info=True)
        return make_response(jsonify({'error': 'Internal server error'}), 500)



@app.route('/', methods=['GET'])
def index():
    return jsonify({
        'service': 'Pixel Tracker API',
        'version': '1.0.0',
        'endpoints': {
            'pixel': '/pixel?partner=xxx&cid=yyy',
        }
    }), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8070))
    logger.info(f"Starting Flask app on port {port}")
    debug_mode = os.environ.get('DEBUG', 'false').lower() == 'true'
    logger.info(f"Debug mode is {'on' if debug_mode else 'off'}")
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
