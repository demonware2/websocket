const zlib = require('zlib');
const DEFAULT_MAX_BYTES = 65536;

function unpackFrame(msg, maxBytes = DEFAULT_MAX_BYTES) {
    if (!msg || msg.z !== 1) {
        return msg;
    }

    const inflated = zlib.inflateRawSync(Buffer.from(String(msg.d || ''), 'base64'), { maxOutputLength: maxBytes });
    const { z, d, ...rest } = msg;

    return { ...rest, payload: JSON.parse(inflated.toString('utf8')) };
}

module.exports = { unpackFrame };