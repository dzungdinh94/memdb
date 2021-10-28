const exports = {};

export const collMethods = ['find', 'findOne', 'findById',
                    'findReadOnly', 'findOneReadOnly', 'findByIdReadOnly',
                    'insert', 'update', 'remove', 'lock', 'count'];

export const connMethods = ['commit', 'rollback', 'eval', 'info', 'resetCounter', 'flushBackend', '$unload', '$findReadOnly'];

export const version = require('../package').version;
export const minClientVersion = '0.4';
export default exports