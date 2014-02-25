module.exports = {
    WORKER: 'MDPW02',
    CLIENT: 'MDPC02',

    W_READY:      0x01,
    W_REQUEST:    0x02,
    W_PARTIAL:    0x03,
    W_FINAL:      0x04,
    W_HEARTBEAT:  0x05,
    W_DISCONNECT: 0x06,

    C_REQUEST: 0x01,
    C_PARTIAL: 0x02,
    C_FINAL:   0x03,
    C_HEARTBEAT:   0x04
};
