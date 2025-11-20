import atexit
import logging
import os
import time
import uuid
from datetime import datetime
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
COOKIE_NAME = 'm_id'
COOKIE_MAX_AGE = 365 * 24 * 60 * 60

if not GCS_BUCKET:
    logger.warning("GCS_BUCKET not set, data will not be persisted")
if not GCS_PROJECT:
    logger.warning("GCS_PROJECT not set")

buffer_writer = BufferWriter(
    gcs_bucket=GCS_BUCKET,
    gcs_project=GCS_PROJECT,
    buffer_size=10000,
    buffer_time=60
)

buffer_writer.start()

@atexit.register
def cleanup():
    logger.info("Shutting down buffer writer...")
    buffer_writer.stop()


@app.route('/pixel', methods=['GET'])
def pixel():
    try:
        cid = request.args.get('cid')
        partner_id = request.args.get('partner_id','')
        mapping_id = request.cookies.get(COOKIE_NAME)

        if not mapping_id:
            mapping_id = str(uuid.uuid4())

        data = {
        'cid': cid,
        'partner_id': partner_id,
        'mapping_id': mapping_id,
        'timestamp': datetime.utcnow().isoformat(),
        'ip_address': request.headers.get('X-Forwarded-For', request.remote_addr),
        'user_agent': request.headers.get('User-Agent', ''),
        'referer': request.headers.get('Referer', ''),
        'query_params': dict(request.args),
        'date': datetime.utcnow().strftime('%Y-%m-%d'),
    }
        buffer_writer.add(data)

        if cid is  None or cid is None:
            gif_data = b'GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;'
            response = make_response(gif_data)
            response.headers['Content-Type'] = 'image/gif'
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
            return make_response(response)

        redirect_url = url_for('pixel_gif', cid=cid, mid=mapping_id)
        response = make_response(redirect(redirect_url, code=302))

        if COOKIE_NAME not in request.cookies:
            cookie_kwargs = {
                'max_age': COOKIE_MAX_AGE,
                'domain': COOKIE_DOMAIN,
                'httponly': True,
                'samesite': 'None',
                'secure': True
            }

            response.set_cookie(COOKIE_NAME, mapping_id, **cookie_kwargs)
            logger.info(f"Set new cookie for CID: {cid}")

        return response

    except Exception as e:
        logger.error(f"Error processing pixel request: {e}", exc_info=True)
        return redirect('/pixel.gif', code=302)


@app.route('/pixel.gif', methods=['GET'])
def pixel_gif():
    third_party_success = COOKIE_NAME in request.cookies

    if third_party_success:
        cid = request.args.get('cid')
        mapping_id = request.cookies.get(COOKIE_NAME)
        partner_id = request.args.get('partner_id','')

        data = {
            'cid': cid,
            'partner_id': partner_id,
            'mapping_id': mapping_id,
            'timestamp': datetime.utcnow().isoformat(),
            'ip_address': request.headers.get('X-Forwarded-For', request.remote_addr),
            'user_agent': request.headers.get('User-Agent', ''),
            'referer': request.headers.get('Referer', ''),
            'query_params': dict(request.args),
            'date': datetime.utcnow().strftime('%Y-%m-%d'),
            'third_party_success': True
        }
        buffer_writer.add(data)

    gif_data = b'GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;'
    response = make_response(gif_data)
    response.headers['Content-Type'] = 'image/gif'
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'

    return response

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        'service': 'Pixel Tracker API',
        'version': '1.0.0',
        'endpoints': {
            'pixel': '/pixel?partner_id=xxx&cid=yyy',
        }
    }), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8070))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
